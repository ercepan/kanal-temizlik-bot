#!/usr/bin/env python3
"""Otomatik yetkilendirme — bot kalıcı yönetici, hesap geçici işçi.

SORUN
-----
• BOT eski mesajları silemez (Telegram 48 saat kuralı) ama kanalda kalıcı
  yönetici durabilir.
• KULLANICI HESABI her yaştaki mesajı silebilir ama kanal sahibinin onu her
  seferinde elle yönetici yapması gerekir.

ÇÖZÜM
-----
İkisini birleştiriyoruz. Bot kanalda kalıcı yönetici durur ve iş geldiğinde:

    1. Tek kullanımlık davet linki üretir      (bot: createChatInviteLink)
    2. Hesap o linkle kanala kendi girer       (hesap: ImportChatInvite)
    3. Bot hesabı yönetici yapar               (bot: promoteChatMember)
    4. Hesap mesajları siler                   ← 48 saat sınırı YOK
    5. Bot hesabın yetkisini geri alır         (bot: promoteChatMember, hepsi False)
    6. Hesap kanaldan çıkar                    (hesap: LeaveChannel)

Kanal sahibi yalnızca bir kez botu yönetici yapar; sonrası tamamen otomatik.

BOTUN KANALDA AÇIK OLMASI GEREKEN YETKİLERİ
-------------------------------------------
    ✅ Yeni yöneticiler ekle  (can_promote_members) — yükseltmeyi bu sağlar
    ✅ Davet linki oluştur    (can_invite_users)    — hesabın girmesi için
    ✅ Mesajları sil          (can_delete_messages) — Telegram, botun kendinde
                              olmayan bir yetkiyi başkasına vermesine izin vermez

TELEGRAM KURALLARI (tasarımı bunlar belirledi)
----------------------------------------------
• Bot bir kullanıcıyı kanala EKLEYEMEZ; bu yüzden davet linki kullanılıyor.
• Bot yalnızca KENDİ yükselttiği yöneticinin yetkisini geri alabilir —
  burada yükselten de indiren de aynı bot olduğu için sorun çıkmaz.
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Optional

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

log = logging.getLogger("kanal-temizlik.yetki")

# Yükseltmenin MTProto tarafında görünmesi için beklenecek süre
YETKI_YERLESME = 3.0

# Davet linki: t.me/+HASH  ya da  t.me/joinchat/HASH
_DAVET_RE = re.compile(r"(?:t\.me/(?:joinchat/|\+))([A-Za-z0-9_-]+)")


async def sohbet_tipi(bot: Bot, chat_id: int) -> Optional[str]:
    """'channel' (yayın kanalı) | 'supergroup' | 'group'. Erişilemezse None.

    Bu ayrım SESSİZLİK için kritik:
      • Yayın kanalında üye katılması/ayrılması sohbette GÖRÜNMEZ.
      • Süper grup/grupta "X gruba katıldı" ve "X gruptan ayrıldı" servis
        mesajları sohbete DÜŞER — ve bunlar bizim silemediğimiz mesajlar.
    Bu yüzden otomatik katıl/çık dansı yalnızca yayın kanallarında yapılır.
    """
    try:
        sohbet = await bot.get_chat(chat_id)
        return sohbet.type
    except (TelegramBadRequest, TelegramForbiddenError):
        return None


async def bot_yetkileri(bot: Bot, chat_id: int) -> Optional[dict]:
    """Botun bu kanaldaki hakları. Kanalda değilse/erişemiyorsa None."""
    try:
        ben = await bot.get_chat_member(chat_id, bot.id)
    except (TelegramBadRequest, TelegramForbiddenError):
        return None
    if getattr(ben, "status", "") not in ("administrator", "creator"):
        return None
    return {
        "promote": bool(getattr(ben, "can_promote_members", False)),
        "invite": bool(getattr(ben, "can_invite_users", False)),
        "delete": bool(getattr(ben, "can_delete_messages", False)),
    }


def eksik_yetki_mesaji(haklar: dict) -> str:
    eksik = []
    if not haklar.get("promote"):
        eksik.append("• <b>Yeni yöneticiler ekle</b>")
    if not haklar.get("invite"):
        eksik.append("• <b>Davet linki oluştur</b>")
    if not haklar.get("delete"):
        eksik.append("• <b>Mesajları sil</b>")
    if not eksik:
        return ""
    return (
        "⚙️ Otomatik mod için botun şu yetkileri eksik:\n"
        + "\n".join(eksik)
        + "\n\nKanal ayarları → Yöneticiler → botu seç → bu kutuları işaretle. "
        "Bir kez yapman yeterli, sonrası otomatik."
    )


async def kullanilabilir_mi(bot: Bot, chat_id: int) -> tuple[bool, str]:
    """Otomatik yetkilendirme bu kanalda çalışır mı?"""
    haklar = await bot_yetkileri(bot, chat_id)
    if haklar is None:
        return False, (
            "❌ Bot bu kanalda yönetici değil.\n"
            "Botu kanala <b>yönetici</b> olarak ekle — gerisi otomatik olur."
        )
    eksik = eksik_yetki_mesaji(haklar)
    return (False, eksik) if eksik else (True, "")


async def _davet_linki(bot: Bot, chat_id: int) -> Optional[str]:
    """Tek kullanımlık, kısa ömürlü davet linki üretir."""
    try:
        link = await bot.create_chat_invite_link(
            chat_id,
            name="temizlik",
            member_limit=1,          # yalnızca bizim hesap girebilsin
            creates_join_request=False,
        )
        return link.invite_link
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        log.warning("Davet linki üretilemedi (chat=%s): %s", chat_id, e)
        return None


async def _linkle_katil(client, link: str) -> bool:
    """Hesap davet linkiyle kanala girer. Zaten üyeyse de başarılı sayılır."""
    m = _DAVET_RE.search(link)
    if not m:
        log.warning("Davet linki çözülemedi: %s", link)
        return False
    try:
        from telethon.tl.functions.messages import ImportChatInviteRequest

        await client(ImportChatInviteRequest(m.group(1)))
        return True
    except Exception as e:
        if "ALREADY_PARTICIPANT" in str(e).upper():
            return True
        log.warning("Davetle katılınamadı: %s", e)
        return False


async def yetki_ver(bot: Bot, client, chat_id: int, hesap_id: int) -> tuple[bool, str]:
    """Hesabı kanala sokup 'mesajları sil' yetkisi verir."""
    uygun, hata = await kullanilabilir_mi(bot, chat_id)
    if not uygun:
        return False, hata

    # 1) Hesap zaten üye mi? Değilse davet linkiyle girsin.
    try:
        uye = await bot.get_chat_member(chat_id, hesap_id)
        icerde = getattr(uye, "status", "") not in ("left", "kicked")
    except (TelegramBadRequest, TelegramForbiddenError):
        icerde = False

    if not icerde:
        # SESSİZLİK KORUMASI: süper grup/grupta kanala girmek "X gruba katıldı"
        # servis mesajı bastırır. Bu mesajı biz silemeyiz (bize ait değil, üstelik
        # ana mesajın altında kalır). O yüzden orada otomatik katılma YAPMIYORUZ.
        tip = await sohbet_tipi(bot, chat_id)
        if tip != "channel":
            return False, (
                "🔇 Burası bir <b>grup</b> (yayın kanalı değil). Gruba katılmam "
                '"gruba katıldı" yazısı bastırır ve o yazıyı silemem.\n\n'
                "Sessiz kalması için hesabı gruba <b>bir kez elle</b> ekleyin; "
                "sonraki temizliklerde yetkiyi otomatik alıp bırakırım, "
                "hiçbir iz kalmaz."
            )
        link = await _davet_linki(bot, chat_id)
        if link is None:
            return False, "❌ Davet linki üretemedim (botun davet yetkisi yok olabilir)."
        if not await _linkle_katil(client, link):
            return False, (
                "❌ Hesap kanala giremedi. Hesabın Telegram'da çok fazla kanala "
                "üye olması bunu engelliyor olabilir (sınır 500)."
            )
        try:
            await bot.revoke_chat_invite_link(chat_id, link)  # link açıkta kalmasın
        except (TelegramBadRequest, TelegramForbiddenError):
            log.debug("Davet linki iptal edilemedi", exc_info=True)
        log.info("Hesap kanala katıldı: %s", chat_id)

    # 2) Yükselt — yalnızca silme yetkisi açılıyor, başka hiçbir hak verilmiyor.
    try:
        await bot.promote_chat_member(
            chat_id=chat_id,
            user_id=hesap_id,
            can_delete_messages=True,
            can_manage_chat=False,
            can_change_info=False,
            can_post_messages=False,
            can_edit_messages=False,
            can_invite_users=False,
            can_restrict_members=False,
            can_pin_messages=False,
            can_promote_members=False,
            can_manage_video_chats=False,
            is_anonymous=False,
        )
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        log.warning("Yükseltme başarısız (chat=%s): %s", chat_id, e)
        return False, _hata_turkce(str(e))

    log.info("Hesaba silme yetkisi verildi: chat=%s (katıldık=%s)", chat_id, not icerde)
    await asyncio.sleep(YETKI_YERLESME)  # yetki MTProto tarafında görünsün
    # Dönüş metni "KATILDIK" ise iş bitince kanaldan çıkılır; zaten üyeysek
    # çıkmayız (çıkış da grupta iz bırakabilir, üstelik üyeliği bozmaya gerek yok).
    return True, ("KATILDIK" if not icerde else "")


async def yetki_al(bot: Bot, chat_id: int, hesap_id: int) -> bool:
    """Verilen yetkiyi geri alır (tüm haklar False = yöneticilikten düşürme)."""
    try:
        await bot.promote_chat_member(
            chat_id=chat_id,
            user_id=hesap_id,
            can_manage_chat=False,
            can_change_info=False,
            can_post_messages=False,
            can_edit_messages=False,
            can_delete_messages=False,
            can_invite_users=False,
            can_restrict_members=False,
            can_pin_messages=False,
            can_promote_members=False,
            can_manage_video_chats=False,
            is_anonymous=False,
        )
        log.info("Yetki geri alındı: chat=%s", chat_id)
        return True
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        log.warning("Yetki geri alınamadı (chat=%s): %s", chat_id, e)
        return False


async def kanaldan_cik(client, chat_id: int) -> bool:
    """Hesap kanaldan ayrılır — kalıcı üyelik bırakmıyoruz.

    YALNIZCA yayın kanallarında ve yalnızca bu iş için katıldıysak çağrılır
    (bkz. yetki_ver'in "KATILDIK" dönüşü); grupta ayrılmak iz bırakır.
    """
    try:
        from telethon.tl.functions.channels import LeaveChannelRequest
        from telethon.tl.types import PeerChannel

        ic = abs(chat_id) - 1_000_000_000_000 if chat_id < -1_000_000_000_000 else abs(chat_id)
        await client(LeaveChannelRequest(PeerChannel(int(ic))))
        log.info("Kanaldan çıkıldı: %s", chat_id)
        return True
    except Exception:
        log.warning("Kanaldan çıkılamadı: %s", chat_id, exc_info=True)
        return False


def _hata_turkce(metin: str) -> str:
    ust = metin.upper()
    if "USER_ADMIN_INVALID" in ust or "CHAT_ADMIN_REQUIRED" in ust:
        return (
            "❌ Bot bu hesabı yükseltemedi. Botun <b>Yeni yöneticiler ekle</b> "
            "yetkisi açık olmalı."
        )
    if "RIGHT_FORBIDDEN" in ust or "not enough rights" in metin.lower():
        return (
            "❌ Bot kendinde olmayan bir yetkiyi veremez — botun da "
            "<b>Mesajları sil</b> yetkisi açık olmalı."
        )
    if "PARTICIPANT_ID_INVALID" in ust or "user not found" in metin.lower():
        return "❌ Hesap kanalda görünmüyor; davet adımı başarısız olmuş olabilir."
    if "FLOOD" in ust:
        return "⏳ Telegram hız sınırı. Biraz bekleyip tekrar dene."
    return f"❌ Telegram reddetti: <code>{metin[:150]}</code>"
