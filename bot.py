#!/usr/bin/env python3
"""
Kanal Temizlik Botu
-------------------
Belirlediğin "ana mesaja" kadar kanaldaki mesajları siler; ana mesajın kendisi
silinmez, bot orada durur.

Telegram toplu silmede tek seferde 100 mesaja izin verir ama garanti olsun diye
90'lık gruplar kullanılır (BATCH_SIZE). Flood limitine takılırsa bekleyip devam
eder; toplu silme patlarsa o grubu tek tek silerek toparlar.
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Iterator, Optional, Union

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    KeyboardButtonRequestUsers,
    Message,
    MessageOriginChannel,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

try:
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    pass

BATCH_SIZE = 90        # Telegram limiti 100; garanti olsun diye 90
BATCH_DELAY = 0.4      # gruplar arası bekleme (saniye) — flood koruması
DATA_FILE = Path(__file__).with_name("son_isler.json")
SEEN_FILE = Path(__file__).with_name("son_gorulen.json")

# Son mesaj ID'sini sessizce bulma ayarları:
MTPROTO_WINDOW = 100        # tek MTProto çağrısında kontrol edilen ardışık ID sayısı
EMPTY_TOLERANCE = 20000     # art arda bu kadar boş ID görünce "son mesaj bu" der
                            # (önceki temizliklerden kalan bu boyuta kadar silinmiş
                            # ID boşlukları aşılır; 100'lük pencerelerle taranır)
SCAN_CALL_CAP = 3000        # tarama başına en fazla MTProto çağrısı (~300k ID)
REACTION_TOLERANCE = 300    # reaksiyon yedeğinde boşluk toleransı (tek tek kontrol, dar tutulur)
REACTION_CALL_CAP = 500     # reaksiyon yedeğinde en fazla çağrı

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
log = logging.getLogger("kanal-temizlik")

dp = Dispatcher()

pending: dict[int, dict] = {}      # user_id -> onay bekleyen temizlik işi
pending_kick: dict[int, dict] = {}  # user_id -> kişi seçimi bekleyen atma işi
active_chats: set[int] = set()     # şu an temizlik yürüyen kanallar
_jobs_lock = asyncio.Lock()        # son_isler.json'a eşzamanlı yazma koruması
_seen_lock = asyncio.Lock()        # son_gorulen.json yazma koruması

KICK_REQUEST_ID = 1001  # kişi seçme butonunun kimliği
BOOT_TS = 0.0           # bot açılış zamanı; birikmiş eski özel mesajları elemek için


def _load_seen() -> dict[int, int]:
    try:
        raw = json.loads(SEEN_FILE.read_text(encoding="utf-8"))
        return {int(k): int(v) for k, v in raw.items()}
    except (OSError, json.JSONDecodeError, ValueError):
        return {}


# kanal -> bilinen son mesaj ID'si (kanal postlarından takip edilir)
last_seen: dict[int, int] = _load_seen()


async def remember_seen(chat_id: int, message_id: int) -> None:
    if message_id <= last_seen.get(chat_id, 0):
        return
    last_seen[chat_id] = message_id
    async with _seen_lock:
        try:
            SEEN_FILE.write_text(
                json.dumps({str(k): v for k, v in last_seen.items()}), encoding="utf-8"
            )
        except OSError:
            pass

START_TEXT = (
    "👋 Merhaba! Ben <b>kanal temizlik botuyum</b>.\n\n"
    "🎯 Bana bir <b>ana mesaj</b> gösterirsin; kanaldaki mesajları o mesaja kadar "
    "silerim. Ana mesaja dokunmam, orada dururum.\n\n"
    "<b>Kurulum (tek seferlik):</b>\n"
    "1️⃣ Beni kanalına <b>yönetici</b> olarak ekle\n"
    "2️⃣ <b>Mesajları sil</b> yetkisini ver\n\n"
    "<b>Kullanım:</b>\n"
    "1️⃣ Kanalda ana mesaja bas → <i>Bağlantıyı Kopyala</i> → linki bana gönder\n"
    "     (ya da ana mesajı bana doğrudan <b>ilet/forward</b>)\n"
    "2️⃣ Çıkan butondan silme yönünü seç, gerisi bende 🧹\n\n"
    "<b>Komutlar:</b>\n"
    "/tekrar — son işi aynı ana mesajla yeniden başlat\n"
    "/kanaldanat — kanaldan kişi at (örn: <code>/kanaldanat @kullanici</code>; "
    "isim çözülemezse listeden seçtirir)\n\n"
    "ℹ️ Mesajları 90'lık gruplar halinde, ana mesaja kadar <b>hepsini</b> silerim; "
    "flood limitine takılırsam bekler, kaldığım yerden devam ederim.\n"
    "⚠️ Silinen mesajlar geri getirilemez!"
)


# ---------------------------------------------------------------- saf mantık

_LINK_RE = re.compile(
    r"(?<![\w.-])(?:https?://)?(?:t(?:elegram)?\.(?:me|dog))/(\S+)",
    re.IGNORECASE,
)
_USERNAME_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]{2,31}")


def parse_message_link(text: str) -> Optional[tuple[Union[int, str], int]]:
    """t.me mesaj linkinden (kanal, mesaj_id) çıkarır; bulamazsa None döner."""
    m = _LINK_RE.search(text)
    if not m:
        return None
    parts = m.group(1).split("?")[0].split("#")[0].strip("/").split("/")
    if parts and parts[0] == "s":  # t.me/s/kanal/123 (web önizleme linki)
        parts = parts[1:]
    if len(parts) < 2 or not parts[-1].isdigit():
        return None
    if parts[0] == "c":  # özel kanal: t.me/c/<iç_id>/[konu/]<mesaj_id>
        if len(parts) < 3 or not parts[1].isdigit():
            return None
        return int(f"-100{parts[1]}"), int(parts[-1])
    if not _USERNAME_RE.fullmatch(parts[0]):
        return None
    return f"@{parts[0]}", int(parts[-1])


def normalize_username(raw: str) -> str:
    """'@isim', 't.me/isim' gibi girdilerden çıplak kullanıcı adını çıkarır."""
    u = raw.strip()
    u = re.sub(r"^(?:https?://)?(?:t(?:elegram)?\.(?:me|dog))/", "", u, flags=re.IGNORECASE)
    return u.lstrip("@").strip("/").split("?")[0]


def iter_batches(start: int, stop: int, size: int = BATCH_SIZE) -> Iterator[list[int]]:
    """start'tan aşağıya stop+1'e kadar (stop HARİÇ) ID'leri size'lık gruplar verir."""
    mid = start
    while mid > stop:
        low = max(stop + 1, mid - size + 1)
        yield list(range(mid, low - 1, -1))
        mid = low - 1


# ------------------------------------------------------------- kalıcı kayıt

def load_jobs() -> dict:
    try:
        return json.loads(DATA_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


async def save_last_job(user_id: int, job: dict) -> None:
    async with _jobs_lock:
        jobs = load_jobs()
        jobs[str(user_id)] = job
        try:
            DATA_FILE.write_text(
                json.dumps(jobs, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except OSError:
            log.warning("son_isler.json yazılamadı")


# ------------------------------------------------------------------ yardımcı

def confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🧹 Sonrakileri sil (ana mesaja kadar)", callback_data="clean:after")],
            [InlineKeyboardButton(text="🗑 Öncekileri sil (ana mesajdan eskiler)", callback_data="clean:before")],
            [InlineKeyboardButton(text="❌ Vazgeç", callback_data="clean:cancel")],
        ]
    )


# --------------------------------------------------------------- silme işi

async def delete_batch(bot: Bot, chat_id: int, batch: list[int]) -> bool:
    """Bir 90'lık grubu siler; işlendiyse True döner.

    Flood limitine takılınca TEK DOĞRU HAMLE beklemektir: Telegram'ın söylediği
    süre kadar bekleyip AYNI grubu yeniden deneriz. (Tek tek silmeye düşmek floodu
    90 katına çıkarır — eski sürümdeki 'sadece ilk 90 siliniyor' hatasının kökü buydu.)
    Tek tek silme yalnızca 'grupta silinemeyen mesaj var' (BadRequest) durumunda kullanılır.
    """
    if not batch:
        return True
    tries = 0
    while True:
        try:
            await bot.delete_messages(chat_id=chat_id, message_ids=batch)
            return True
        except TelegramRetryAfter as e:
            tries += 1
            if tries >= 8:
                log.warning("Grup %s..%s: flood 8 kez üst üste, grup atlanıyor", batch[0], batch[-1])
                return False
            wait = min(float(e.retry_after) + 1.0, 120.0)
            log.info("Flood limiti: %.0f sn bekleniyor (chat=%s)", wait, chat_id)
            await asyncio.sleep(wait)
        except TelegramBadRequest:
            break  # grupta silinemeyen mesaj olabilir -> tek tek ele
        except TelegramForbiddenError:
            raise
        except Exception:  # ağ kopması vb. geçici hatalar temizliği ÖLDÜRMEZ
            tries += 1
            if tries >= 8:
                log.exception("Grup %s..%s: kalıcı ağ hatası, grup atlanıyor", batch[0], batch[-1])
                return False
            log.warning("Geçici hata, 3 sn sonra aynı grup yeniden denenecek")
            await asyncio.sleep(3)

    for mid in batch:
        for _ in range(4):
            try:
                await bot.delete_message(chat_id, mid)
                break
            except TelegramRetryAfter as e:
                await asyncio.sleep(min(float(e.retry_after) + 1.0, 120.0))
            except TelegramBadRequest:
                break  # zaten yok / silinemez -> atla
            except TelegramForbiddenError:
                raise
            except Exception:
                await asyncio.sleep(2)
        await asyncio.sleep(0.05)
    return True


async def sweep(bot: Bot, chat_id: int, start: int, stop: int) -> tuple[int, int]:
    """start'tan stop'a (stop HARİÇ) tüm ID aralığını 90'ar 90'ar temizler.

    Hiçbir grup hatası taramayı durdurmaz; (taranan_mesaj, atlanan_grup) döner.
    """
    total = max(0, start - stop)
    done = 0
    failed = 0
    for batch in iter_batches(start, stop):
        try:
            ok = await delete_batch(bot, chat_id, batch)
        except TelegramForbiddenError:
            raise  # kanaldan atılmışız — devam etmenin anlamı yok
        except Exception:
            log.exception("Grup beklenmedik şekilde patladı, atlanıp devam ediliyor")
            ok = False
        if ok:
            done += len(batch)
        else:
            failed += 1
            if failed >= 20:
                log.warning("20 grup üst üste başarısız; tarama durduruluyor (chat=%s)", chat_id)
                break
        if done < total:
            await asyncio.sleep(BATCH_DELAY)
    return done, failed


# ------------------------------------------- son mesaj ID'sini SESSİZCE bulma
#
# Kanala HİÇBİR mesaj atılmaz. Sıra:
#   1) MTProto (telethon) ile toplu varlık kontrolü — tek çağrıda 100 ID, boşluklara
#      dayanıklı (spot taraması), tamamen görünmez.
#   2) MTProto yoksa: boş reaksiyon listesiyle tek tek varlık kontrolü (o da görünmez).
# Eski "kanala 🧹 at, hemen sil" son-çare yolu TAMAMEN KALDIRILDI.

async def message_exists(bot: Bot, chat_id: int, message_id: int) -> bool:
    """Mesaj var mı? Boş reaksiyon listesiyle kontrol — kanalda hiçbir iz bırakmaz.

    Kararsız kalırsa True der: fazladan ID taramak zararsızdır (deleteMessages
    olmayanları kendiliğinden atlar) ama az taramak mesaj bırakır.
    """
    for _ in range(6):
        try:
            await bot.set_message_reaction(chat_id=chat_id, message_id=message_id, reaction=[])
            return True
        except TelegramRetryAfter as e:
            await asyncio.sleep(min(float(e.retry_after) + 0.5, 60.0))
        except TelegramBadRequest as e:
            s = str(e).lower()
            return not ("not found" in s or "message_id_invalid" in s)
        except TelegramForbiddenError:
            raise
        except Exception:
            await asyncio.sleep(2)
    return True


async def _mt_entity(client, chat_id: int, username: Optional[str]):
    """Telethon için kanal entity'si: önce @kullanıcıadı, sonra ID tabanlı yollar."""
    from telethon.tl.functions.channels import GetChannelsRequest
    from telethon.tl.types import InputChannel, PeerChannel

    if username:
        try:
            return await client.get_entity(f"@{username.lstrip('@')}")
        except Exception:
            pass
    s = str(chat_id)
    internal = int(s[4:]) if s.startswith("-100") else chat_id
    try:
        return await client.get_input_entity(PeerChannel(internal))
    except Exception:
        pass
    try:
        res = await client(GetChannelsRequest([InputChannel(internal, 0)]))
        if res.chats:
            return res.chats[0]
    except Exception:
        pass
    return None


async def _mt_existing_ids(client, entity, ids: list[int]) -> Optional[set[int]]:
    """Verilen ID'lerden kanalda var olanları döndürür; HATA durumunda None.

    (Hata ile 'hiçbiri yok'u karıştırmamak kritik: None dönerse çağıran MTProto
    yolunu bırakır, yanlış-küçük sonuç üretmez.)
    """
    import telethon.errors as terr

    for _ in range(4):
        try:
            msgs = await client.get_messages(entity, ids=ids)
            return {m.id for m in msgs if m is not None}
        except terr.FloodWaitError as e:
            await asyncio.sleep(min(e.seconds + 1, 60))
        except Exception:
            return None
    return None


async def _mt_find_latest(client, entity, floor: int) -> Optional[int]:
    """floor'dan yukarı kanaldaki en yüksek mesaj ID'sini bulur; hata -> None.

    100'lük pencerelerle YOĞUN tarama yapar: son bulunan mesajın üstünde
    EMPTY_TOLERANCE kadar ardışık boş ID görmeden durmaz. Böylece önceki
    temizliklerden kalan silinmiş-ID boşlukları mesaj kaçırmadan aşılır.
    """
    latest = floor
    nxt = floor + 1
    empty_run = 0
    for _ in range(SCAN_CALL_CAP):
        window = list(range(nxt, nxt + MTPROTO_WINDOW))
        found = await _mt_existing_ids(client, entity, window)
        if found is None:
            return None
        if found:
            latest = max(latest, max(found))
            empty_run = window[-1] - latest  # pencerenin son mesajdan sonraki boş kuyruğu
        else:
            empty_run += MTPROTO_WINDOW
        if empty_run >= EMPTY_TOLERANCE:
            return latest
        nxt = window[-1] + 1
    return latest


async def _reaction_find_latest(bot: Bot, chat_id: int, floor: int) -> int:
    """MTProto yoksa yedek: reaksiyon kontrolüyle aynı yoğun tarama (tek tek,
    o yüzden boşluk toleransı dar tutulur)."""
    latest = floor
    nxt = floor + 1
    calls = 0
    while calls < REACTION_CALL_CAP and (nxt - latest) <= REACTION_TOLERANCE:
        calls += 1
        if await message_exists(bot, chat_id, nxt):
            latest = nxt
        nxt += 1
    return latest


async def find_latest_id(bot: Bot, chat_id: int, anchor_id: int, username: Optional[str] = None) -> int:
    """Kanaldaki son mesaj ID'sini kanala hiçbir şey atmadan bulur."""
    floor = max(last_seen.get(chat_id, 0), anchor_id)
    client = await _get_mtproto()
    if client is not None:
        entity = await _mt_entity(client, chat_id, username)
        if entity is not None:
            latest = await _mt_find_latest(client, entity, floor)
            if latest is not None:
                await remember_seen(chat_id, latest)
                return latest
        log.info("MTProto taraması olmadı, reaksiyon yedeğine geçiliyor (chat=%s)", chat_id)
    latest = await _reaction_find_latest(bot, chat_id, floor)
    await remember_seen(chat_id, latest)
    return latest


async def clean_after(
    bot: Bot, chat_id: int, anchor_id: int, username: Optional[str] = None
) -> tuple[int, int]:
    """Ana mesajdan SONRAKİ (daha yeni) her şeyi siler; (taranan, atlanan_grup) döner."""
    latest = await find_latest_id(bot, chat_id, anchor_id, username)
    if latest <= anchor_id:
        return 0, 0
    return await sweep(bot, chat_id, start=latest, stop=anchor_id)


async def clean_before(bot: Bot, chat_id: int, anchor_id: int) -> tuple[int, int]:
    """Ana mesajdan ÖNCEKİ (daha eski) her şeyi siler; (taranan, atlanan_grup) döner."""
    if anchor_id <= 1:
        return 0, 0
    return await sweep(bot, chat_id, start=anchor_id - 1, stop=0)


# --------------------------------------------------- @kullanıcıadı çözümleme

# Bot API @kullanıcıadı -> kişi çözmeye izin vermez; API_ID/API_HASH tanımlıysa
# aynı bot token'ıyla MTProto üzerinden çözeriz. Tanımlı değilse None döner ve
# akış kişi seçme butonuna düşer.
_mtproto = None            # açık istemci ya da None
_mtproto_lock = asyncio.Lock()
_mtproto_retry_ts = 0.0    # başarısız denemeden sonra bu zamana kadar yeniden denenmez
MTPROTO_COOLDOWN = 300     # saniye — geçici bir hata MTProto'yu kalıcı kapatmasın


async def _get_mtproto():
    global _mtproto, _mtproto_retry_ts
    api_id = os.getenv("API_ID", "").strip()
    api_hash = os.getenv("API_HASH", "").strip()
    token = os.getenv("BOT_TOKEN", "").strip()
    if not (api_id.isdigit() and api_hash and token):
        return None
    async with _mtproto_lock:
        if _mtproto is not None:
            return _mtproto
        if time.time() < _mtproto_retry_ts:
            return None
        client = None
        try:
            from telethon import TelegramClient
            from telethon.sessions import MemorySession

            client = TelegramClient(MemorySession(), int(api_id), api_hash)
            await asyncio.wait_for(client.start(bot_token=token), timeout=25)
            _mtproto = client
            log.info("MTProto çözümleyici hazır")
            return _mtproto
        except Exception:
            log.exception(
                "MTProto istemcisi açılamadı — %s sn sonra yeniden denenecek", MTPROTO_COOLDOWN
            )
            _mtproto_retry_ts = time.time() + MTPROTO_COOLDOWN
            if client is not None:
                try:
                    await client.disconnect()
                except Exception:
                    pass
            return None


async def resolve_user(username: str) -> Optional[tuple[int, str]]:
    """@kullanıcıadı -> (user_id, görünen ad); çözemezse None."""
    client = await _get_mtproto()
    if client is None:
        return None
    try:
        from telethon.tl.types import User

        entity = await client.get_entity(f"@{username}")
    except Exception:
        return None
    if not isinstance(entity, User):
        return None
    name = " ".join(filter(None, [entity.first_name, entity.last_name])) or (
        f"@{entity.username}" if entity.username else str(entity.id)
    )
    return entity.id, name


# ------------------------------------------------------------- kanaldan atma

async def kick_perm_error(bot: Bot, chat_id: int, user_id: int) -> Optional[str]:
    """Atma işlemi için yetkileri kontrol eder; sorun varsa Türkçe hata döner."""
    try:
        admins = await bot.get_chat_administrators(chat_id)
    except (TelegramBadRequest, TelegramForbiddenError):
        return "❌ Kanala erişemiyorum — hâlâ yönetici miyim?"
    me = next((a for a in admins if a.user.id == bot.id), None)
    if me is None or not getattr(me, "can_restrict_members", False):
        return (
            "❌ Bende <b>Kullanıcıları yasakla</b> yetkisi yok.\n"
            "Kanal → Yöneticiler → bot → <i>Kullanıcıları yasakla</i> iznini aç, tekrar dene."
        )
    if next((a for a in admins if a.user.id == user_id), None) is None:
        return "⛔ Bu kanalda yönetici görünmüyorsun; bu işlemi yapamazsın."
    return None


async def do_kick(bot: Bot, message: Message, chat_id: int, title: str, target_id: int, target_name: str) -> None:
    try:
        await bot.ban_chat_member(chat_id, target_id)
    except TelegramBadRequest as e:
        s = str(e).lower()
        if "administrator" in s or "admin" in s or "restrict self" in s:
            msg = "❌ Bu kişi kanalda yönetici — botlar yöneticileri atamaz. Önce yöneticilikten düşürmen lazım."
        elif "not found" in s or "participant" in s:
            msg = "❌ Bu kullanıcıyı kanalda bulamadım."
        else:
            msg = f"❌ Atamadım: <code>{html.escape(str(e))}</code>"
        await message.answer(msg, reply_markup=ReplyKeyboardRemove())
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="↩️ Geri al (yasağı kaldır)", callback_data=f"unban:{chat_id}:{target_id}")
    ]])
    await message.answer(
        f"✅ <b>{html.escape(target_name)}</b> kanaldan atıldı ve yasaklandı.\n"
        f"📋 {html.escape(title)}",
        reply_markup=kb,
    )
    log.info("Kullanıcı atıldı: chat=%s user=%s", chat_id, target_id)


@dp.message(Command("kanaldanat"))
async def cmd_kick(message: Message, bot: Bot) -> None:
    job = load_jobs().get(str(message.from_user.id))
    if not job:
        await message.answer(
            "Önce hangi kanaldan atacağımı bilmem lazım: kanaldan herhangi bir mesajın "
            "<b>linkini</b> gönder (bir kez yeter), sonra tekrar /kanaldanat yaz."
        )
        return
    chat_id, title = job["chat_id"], job.get("title", "kanal")

    err = await kick_perm_error(bot, chat_id, message.from_user.id)
    if err:
        await message.answer(err)
        return

    parts = (message.text or "").split()
    target = parts[1] if len(parts) > 1 else ""
    note = ""
    if target:
        username = normalize_username(target)
        if username.isdigit():  # doğrudan sayısal ID verildi
            await do_kick(bot, message, chat_id, title, int(username), username)
            return
        resolved_id: Optional[int] = None
        resolved_name = ""
        try:
            c = await bot.get_chat(f"@{username}")
            if c.type == "private":
                resolved_id = c.id
                resolved_name = " ".join(filter(None, [c.first_name, c.last_name])) or f"@{username}"
        except (TelegramBadRequest, TelegramForbiddenError):
            pass
        if resolved_id is None:
            r = await resolve_user(username)
            if r is not None:
                resolved_id, resolved_name = r
        if resolved_id is not None:
            await do_kick(bot, message, chat_id, title, resolved_id, resolved_name)
            return
        note = f"@{html.escape(username)} adını kendim çözemedim (Telegram botlara her adı vermiyor). "

    pending_kick[message.from_user.id] = {"chat_id": chat_id, "title": title}
    kb = ReplyKeyboardMarkup(
        keyboard=[[
            KeyboardButton(
                text="👤 Atılacak kişiyi seç",
                request_users=KeyboardButtonRequestUsers(
                    request_id=KICK_REQUEST_ID,
                    user_is_bot=False,
                    max_quantity=1,
                    request_name=True,
                    request_username=True,
                ),
            )
        ]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await message.answer(
        f"{note}Aşağıdaki butona bas, açılan listeden kişiyi seç (isimle arayabilirsin) — "
        f"<b>{html.escape(title)}</b> kanalından atayım.",
        reply_markup=kb,
    )


@dp.message(F.users_shared)
async def on_user_picked(message: Message, bot: Bot) -> None:
    job = pending_kick.pop(message.from_user.id, None)
    if job is None or message.users_shared.request_id != KICK_REQUEST_ID:
        await message.answer("Bekleyen bir atma işlemi yok. /kanaldanat ile başlat.", reply_markup=ReplyKeyboardRemove())
        return
    err = await kick_perm_error(bot, job["chat_id"], message.from_user.id)
    if err:
        await message.answer(err, reply_markup=ReplyKeyboardRemove())
        return
    su = message.users_shared.users[0]
    name = " ".join(filter(None, [su.first_name, su.last_name])) or (
        f"@{su.username}" if su.username else str(su.user_id)
    )
    await do_kick(bot, message, job["chat_id"], job["title"], su.user_id, name)


@dp.callback_query(F.data.startswith("unban:"))
async def on_unban(cb: CallbackQuery, bot: Bot) -> None:
    try:
        _, chat_id_s, target_id_s = cb.data.split(":")
        chat_id, target_id = int(chat_id_s), int(target_id_s)
    except ValueError:
        await cb.answer("Geçersiz istek", show_alert=True)
        return
    err = await kick_perm_error(bot, chat_id, cb.from_user.id)
    if err:
        await cb.answer("Yetki sorunu var — bota özelden bak.", show_alert=True)
        return
    try:
        await bot.unban_chat_member(chat_id, target_id, only_if_banned=True)
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        await cb.answer(f"Olmadı: {e}", show_alert=True)
        return
    await cb.answer("Yasak kaldırıldı")
    try:
        await cb.message.edit_text(
            cb.message.html_text + "\n↩️ <i>Yasak kaldırıldı — tekrar katılabilir.</i>"
        )
    except (TelegramBadRequest, AttributeError, TypeError):
        pass


# ------------------------------------------------------------------ akışlar

async def prepare_job(
    message: Message,
    bot: Bot,
    chat_ref: Union[int, str],
    anchor_id: int,
    show_preview: bool = True,
) -> None:
    user_id = message.from_user.id
    try:
        chat = await bot.get_chat(chat_ref)
    except (TelegramBadRequest, TelegramForbiddenError):
        await message.answer(
            "❌ Bu kanala ulaşamıyorum. Önce beni kanala <b>yönetici</b> olarak ekle, "
            "sonra linki tekrar gönder."
        )
        return
    if chat.type not in ("channel", "supergroup", "group"):
        await message.answer("❌ Bu bir kanal/grup mesajı linki değil.")
        return

    try:
        admins = await bot.get_chat_administrators(chat.id)
    except (TelegramBadRequest, TelegramForbiddenError):
        await message.answer(
            "❌ Yönetici listesine bakamadım — kanalda yönetici olduğumdan emin ol."
        )
        return

    me_admin = next((a for a in admins if a.user.id == bot.id), None)
    if me_admin is None or not getattr(me_admin, "can_delete_messages", False):
        await message.answer(
            "❌ Bende <b>Mesajları sil</b> yetkisi yok.\n"
            "Kanal → Yöneticiler → bot → <i>Mesajları sil</i> iznini aç, tekrar dene."
        )
        return
    if next((a for a in admins if a.user.id == user_id), None) is None:
        await message.answer(
            "⛔ Bu kanalda yönetici görünmüyorsun; güvenlik gereği işlemi başlatamam."
        )
        return

    note = ""
    if show_preview:
        try:
            await bot.forward_message(
                user_id, chat.id, anchor_id, disable_notification=True
            )
            note = "⬆️ Ana mesaj bu — burada duracağım.\n\n"
        except TelegramBadRequest as e:
            if "not found" in str(e).lower():
                note = (
                    "⚠️ Bu ID'de mesaj bulamadım (silinmiş olabilir). "
                    "Yine de bu ID'yi sınır kabul edebilirim.\n\n"
                )

    job = {
        "chat_id": chat.id,
        "anchor_id": anchor_id,
        "title": chat.title or str(chat.id),
        "username": chat.username,  # sessiz MTProto taraması için (özel kanalda None)
    }
    pending[user_id] = job
    await save_last_job(user_id, job)

    await message.answer(
        f"{note}"
        f"📋 Kanal: <b>{html.escape(job['title'])}</b>\n"
        f"🎯 Ana mesaj ID: <code>{anchor_id}</code>\n\n"
        "Hangi yönde sileyim? (Ana mesajın kendisi <b>silinmez</b>)",
        reply_markup=confirm_kb(),
    )


@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    await message.answer(START_TEXT)


@dp.message(Command("tekrar"))
async def cmd_tekrar(message: Message, bot: Bot) -> None:
    job = load_jobs().get(str(message.from_user.id))
    if not job:
        await message.answer("Kayıtlı bir işin yok. Önce ana mesajın linkini gönder.")
        return
    await prepare_job(message, bot, job["chat_id"], job["anchor_id"])


@dp.message(F.chat.type == "private", F.forward_origin.as_("origin"))
async def handle_forward(message: Message, bot: Bot, origin) -> None:
    if isinstance(origin, MessageOriginChannel):
        await prepare_job(
            message, bot, origin.chat.id, origin.message_id, show_preview=False
        )
    else:
        await message.answer(
            "Bu ileti bir kanaldan gelmemiş. Ana mesajı doğrudan kanaldan ilet "
            "ya da linkini gönder."
        )


@dp.message(F.chat.type == "private", F.text)
async def handle_text(message: Message, bot: Bot) -> None:
    parsed = parse_message_link(message.text)
    if parsed is None:
        await message.answer(
            "Ana mesajın <b>linkini</b> gönder ya da mesajı bana <b>ilet</b>.\n"
            "Link kopyalamak için: kanalda mesaja bas → <i>Bağlantıyı Kopyala</i>.\n"
            "Örnek: <code>https://t.me/kanalim/123</code>"
        )
        return
    await prepare_job(message, bot, parsed[0], parsed[1])


@dp.callback_query(F.data.startswith("clean:"))
async def on_clean_button(cb: CallbackQuery, bot: Bot) -> None:
    action = cb.data.split(":", 1)[1]

    if action == "cancel":
        pending.pop(cb.from_user.id, None)
        await cb.answer("İptal edildi")
        try:
            await cb.message.edit_text("❌ Vazgeçildi. Yeni bir link gönderebilirsin.")
        except TelegramBadRequest:
            pass
        return

    job = pending.get(cb.from_user.id)
    if job is None:
        await cb.answer(
            "Aktif iş bulunamadı — linki yeniden gönder ya da /tekrar yaz.",
            show_alert=True,
        )
        return

    chat_id, anchor_id = job["chat_id"], job["anchor_id"]
    if chat_id in active_chats:
        await cb.answer("Bu kanalda temizlik zaten sürüyor, bitmesini bekle.", show_alert=True)
        return

    # Ekstra mesaj yok: küçük bir bildirim baloncuğu gösterip sessizce işe başla
    await cb.answer("🧹 Başladım, bitince haber veririm")
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest:
        pass

    active_chats.add(chat_id)
    try:
        if action == "after":
            deleted, failed = await clean_after(
                bot, chat_id, anchor_id, job.get("username")
            )
        else:
            deleted, failed = await clean_before(bot, chat_id, anchor_id)
        if deleted <= 0 and failed == 0:
            text = "✅ Silinecek mesaj yoktu; orası zaten temiz."
        else:
            text = (
                f"✅ Bitti! ~<b>{deleted}</b> mesajlık aralık temizlendi.\n"
                f"🎯 Ana mesaj (<code>{anchor_id}</code>) yerinde duruyor."
            )
            if failed:
                text += (
                    f"\n⚠️ {failed} grup Telegram limitleri yüzünden atlandı — "
                    "aynı linki gönderip bir daha başlatırsan kalanları da temizlerim."
                )
        try:
            await bot.send_message(cb.from_user.id, text)
        except Exception:  # rapor gönderilemese de temizlik tamamlanmıştır
            log.warning("Sonuç mesajı gönderilemedi (user=%s)", cb.from_user.id)
        log.info("Temizlik bitti: chat=%s taranan=%s atlanan_grup=%s", chat_id, deleted, failed)
    except TelegramForbiddenError:
        try:
            await bot.send_message(
                cb.from_user.id,
                "❌ Kanala erişimim gitti (atılmış ya da yetkim alınmış olabilir).",
            )
        except Exception:
            pass
    except Exception as e:  # noqa: BLE001
        log.exception("Temizlik sırasında hata")
        try:
            await bot.send_message(
                cb.from_user.id,
                f"❌ Hata: <code>{html.escape(str(e))}</code>\n"
                "Aynı linki gönderip yeniden başlatırsan kaldığı yerden toparlar.",
            )
        except Exception:
            pass
    finally:
        active_chats.discard(chat_id)


@dp.message(F.chat.type == "private")
async def handle_other(message: Message) -> None:
    await message.answer(
        "Bunu anlayamadım. Ana mesajın <b>linkini</b> gönder ya da mesajı bana <b>ilet</b>. "
        "Yardım için: /start"
    )


@dp.channel_post()
async def on_channel_post(message: Message) -> None:
    """Kanal postlarını takip ederek son mesaj ID'sini sessizce öğrenir."""
    await remember_seen(message.chat.id, message.message_id)


@dp.message.outer_middleware()
async def stale_guard(handler, event: Message, data: dict):
    """Bot kapalıyken birikmiş eski özel mesajları sessizce yok sayar.

    (Kanal post birikimi ise İSTENEN şey: yeniden başlayınca son mesaj ID
    takibini kendiliğinden tamamlar; bu koruma yalnızca 'message' türüne uygulanır.)
    """
    if BOOT_TS and event.date and event.date.timestamp() < BOOT_TS - 60:
        return None
    return await handler(event, data)


# --------------------------------------------------------------------- main

async def _start_health_server() -> None:
    """Render gibi 'web servisi' platformları ve uyanık-tutucu için minik HTTP
    sunucusu. Yalnızca $PORT tanımlıysa açılır (yerelde/başka yerde sessiz geçer)."""
    port = os.getenv("PORT")
    if not port:
        return
    from aiohttp import web

    async def ok(_request):
        return web.Response(text="ok - kanal temizlik botu ayakta")

    app = web.Application()
    app.router.add_get("/", ok)
    app.router.add_get("/health", ok)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(port)).start()
    log.info("Sağlık sunucusu %s portunda açıldı", port)


_bg_tasks: list = []


async def _self_ping_loop() -> None:
    """Render ücretsiz katmanında uykuya geçmeyi önler: 10 dakikada bir kendi
    genel adresine istek atar. RENDER_EXTERNAL_URL yoksa hiç çalışmaz."""
    url = os.getenv("RENDER_EXTERNAL_URL")
    if not url:
        return
    import aiohttp

    while True:
        await asyncio.sleep(600)
        try:
            async with aiohttp.ClientSession() as s:
                await s.get(url, timeout=aiohttp.ClientTimeout(total=20))
        except Exception:  # noqa: BLE001 — ping başarısızsa sonraki turda dener
            pass


async def main() -> None:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        print("HATA: BOT_TOKEN bulunamadı!")
        print("1) Telegram'da @BotFather'dan bot oluşturup token al")
        print("2) Bu klasördeki .env dosyasına şunu yaz: BOT_TOKEN=123456:ABC...")
        sys.exit(1)

    global BOOT_TS
    BOOT_TS = time.time()
    await _start_health_server()
    _bg_tasks.append(asyncio.create_task(_self_ping_loop()))
    bot = Bot(token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    # drop_pending_updates YOK: birikmiş kanal postları son mesaj ID takibini
    # kendiliğinden günceller; eski özel mesajları stale_guard eliyor.
    await bot.delete_webhook()
    me = await bot.get_me()
    log.info("Bot başladı: @%s", me.username)
    await dp.start_polling(bot, allowed_updates=["message", "channel_post", "callback_query"])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot durduruldu.")
