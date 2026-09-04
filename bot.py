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

from . import yedek

dp = Dispatcher()

pending: dict[int, dict] = {}      # user_id -> onay bekleyen temizlik işi
pending_kick: dict[int, dict] = {}  # user_id -> kişi seçimi bekleyen atma işi
active_chats: set[int] = set()     # şu an temizlik yürüyen kanallar
_jobs_lock = asyncio.Lock()        # son_isler.json'a eşzamanlı yazma koruması
_seen_lock = asyncio.Lock()        # son_gorulen.json yazma koruması

KICK_REQUEST_ID = 1001  # kişi seçme butonunun kimliği
BOOT_TS = 0.0           # bot açılış zamanı; birikmiş eski özel mesajları elemek için

IZIN_FILE = Path(__file__).with_name("izinliler.json")
_izin_lock = asyncio.Lock()
_hesap_sahibi_id: Optional[int] = None   # USER_SESSION'ın sahibi; ilk kullanımda dolar


def _izinlileri_oku() -> set[int]:
    """Otomatik yetkilendirmeyi tetikleyebilecek kişiler.

    Bot HERKESE AÇIK. Otomatik mod olmasaydı hesabın bir kanala girmesi için bir
    insanın elle yetki vermesi gerekirdi; o adım doğal bir kapıydı. Otomatik modda
    o kapı yok — bu liste onun yerini alır. Listede olmayan biri, kendi kanalının
    yöneticisi bile olsa, hesabı kanalına otomatik çektiremez.
    """
    try:
        return {int(x) for x in json.loads(IZIN_FILE.read_text(encoding="utf-8"))}
    except (OSError, json.JSONDecodeError, ValueError, TypeError):
        return set()


async def _izinlileri_yaz(kisiler: set[int]) -> None:
    async with _izin_lock:
        try:
            IZIN_FILE.write_text(json.dumps(sorted(kisiler)), encoding="utf-8")
        except OSError:
            log.warning("izinliler.json yazılamadı")


async def hesap_sahibi_id() -> Optional[int]:
    """Bağlı kullanıcı hesabının sahibi — her zaman izinlidir."""
    global _hesap_sahibi_id
    if _hesap_sahibi_id is not None:
        return _hesap_sahibi_id
    client = await get_user_client()
    if client is None:
        return None
    try:
        _hesap_sahibi_id = (await client.get_me()).id
    except Exception:
        log.debug("Hesap sahibi okunamadı", exc_info=True)
    return _hesap_sahibi_id


async def otomatik_izinli_mi(kisi_id: int) -> bool:
    return kisi_id == await hesap_sahibi_id() or kisi_id in _izinlileri_oku()


# Yetkisi YEDEK hesap tarafından otomatik verilmiş kanallar. İş biter bitmez
# yetki geri alınır ve kanaldan çıkılır — kalıcı yetki bırakılmaz.
_yedek_verdi: set[int] = set()


async def yedek_kapat(bot: Bot, chat_id: int) -> str:
    """Otomatik verilen yetkiyi geri alır ve kanaldan çıkar. Rapor satırı döner."""
    if chat_id not in _yedek_verdi:
        return ""
    _yedek_verdi.discard(chat_id)

    client = await get_user_client()
    ben_id = None
    if client is not None:
        try:
            ben_id = (await client.get_me()).id
        except Exception:
            log.debug("Hesap kimliği okunamadı", exc_info=True)

    alindi = await yedek.yetki_al(bot, chat_id, ben_id) if ben_id else False
    cikildi = await yedek.kanaldan_cik(client, chat_id) if client else False

    if alindi and cikildi:
        return "🔒 Yetkim geri alındı, kanaldan çıktım."
    if alindi:
        return "🔒 Yetkim geri alındı (kanaldan çıkamadım)."
    return "⚠️ Yetkiyi geri alamadım — kanaldan elle kaldırman gerekebilir."


START_TEXT = (
    "👋 Merhaba! Ben <b>kanal temizlik botuyum</b>.\n\n"
    "🎯 Bana bir <b>ana mesaj</b> gösterirsin; kanaldaki mesajları o mesaja kadar "
    "silerim. Ana mesaja dokunmam, orada dururum.\n\n"
    "<b>Kurulum — bir kez yapılır:</b>\n"
    "Beni kanalına <b>yönetici</b> yap ve şu üç yetkiyi ver:\n"
    "✅ Mesajları sil\n"
    "✅ Yeni yöneticiler ekle\n"
    "✅ Davet linki oluştur\n\n"
    "<b>Kullanım:</b>\n"
    "1️⃣ Kanalda ana mesaja bas → <i>Bağlantıyı Kopyala</i> → linki bana gönder\n"
    "     (ya da ana mesajı bana doğrudan <b>ilet/forward</b>)\n"
    "2️⃣ Çıkan butondan silme yönünü seç, gerisi bende 🧹\n\n"
    "<b>Komutlar:</b>\n"
    "/tekrar — son işi aynı ana mesajla yeniden başlat\n"
    "/kanaldanat — kanaldan kişi at (örn: <code>/kanaldanat @kullanici</code>)\n\n"
    "<i>Bot sahibine özel:</i> /izinver, /izinal, /izinliler\n\n"
    "ℹ️ <b>Neden üç yetki?</b> Telegram botların 48 saatten eski mesajları "
    "silmesine izin vermiyor. Bu sınır kullanıcı hesaplarında yok. O yüzden iş "
    "geldiğinde bağlı hesabı kanala kendim alıp silme yetkisi veriyorum, iş "
    "bitince yetkiyi geri alıp kanaldan çıkarıyorum — böylece her yaştaki mesaj "
    "silinebiliyor ve kimse kalıcı yetki taşımıyor.\n\n"
    "🔒 Silmeyi yalnızca o kanalın yöneticileri başlatabilir.\n"
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

TOO_OLD = "can't be deleted"

# --------------------------------------------- kendi hesabınla silme (48s yok)
#
# Telegram BOTLARIN 48 saatten eski mesajları silmesine izin vermiyor. Kullanıcı
# hesaplarında böyle bir sınır yok. USER_SESSION tanımlıysa silme işi kullanıcı
# hesabı üzerinden yapılır; tanımlı değilse bot API'sine düşer (48s sınırıyla).
#
# Hız bilerek düşük tutuldu: hesabın kısıtlanmaması için.

_user_client = None
_user_lock = asyncio.Lock()
_user_failed = False

USER_BATCH = 90       # kullanıcı hesabı da tek çağrıda ~100 mesaj siliyor
USER_GAP = 1.0        # gruplar arası bekleme — hesap güvenliği için yavaş


async def get_user_client():
    """Kullanıcı hesabı istemcisi; yoksa/açılamazsa None."""
    global _user_client, _user_failed
    if _user_failed:
        return None
    if _user_client is not None:
        return _user_client
    session = os.getenv("USER_SESSION", "").strip()
    api_id = os.getenv("API_ID", "").strip()
    api_hash = os.getenv("API_HASH", "").strip()
    if not (session and api_id.isdigit() and api_hash):
        return None
    async with _user_lock:
        if _user_client is not None:
            return _user_client
        try:
            from telethon import TelegramClient
            from telethon.sessions import StringSession

            c = TelegramClient(StringSession(session), int(api_id), api_hash)
            await c.connect()
            if not await c.is_user_authorized():
                log.warning("USER_SESSION geçersiz — bot moduna dönülüyor")
                await c.disconnect()
                _user_failed = True
                return None
            me = await c.get_me()
            _user_client = c
            log.info(
                "Silme işlemleri KENDİ HESABINLA yapılacak: %s (48 saat sınırı yok)",
                me.first_name or me.id,
            )
            return _user_client
        except Exception:
            log.exception("Kullanıcı hesabı açılamadı — bot moduna dönülüyor")
            _user_failed = True
            return None


_dialogs_loaded = False


async def _warm_dialogs(client) -> None:
    """Sohbet listesini bir kez gezerek kanal kimliklerini önbelleğe alır.

    Taze bir oturumun önbelleği boştur; bu yüzden kanal ID'sinden doğrudan
    çözüm yapılamaz ("Could not find the input entity"). Sohbetleri bir kez
    dolaşmak gerekli erişim anahtarlarını yerleştirir.
    """
    global _dialogs_loaded
    if _dialogs_loaded:
        return
    try:
        adet = 0
        async for _ in client.iter_dialogs(limit=None):
            adet += 1
        log.info("Hesabın sohbet listesi önbelleğe alındı (%s sohbet)", adet)
        _dialogs_loaded = True
    except Exception:
        log.exception("Sohbet listesi alınamadı")


async def _user_entity(client, chat_id: int):
    from telethon.tl.types import PeerChannel

    s = str(chat_id)
    internal = int(s[4:]) if s.startswith("-100") else abs(chat_id)

    for deneme in (1, 2):
        try:
            return await client.get_input_entity(PeerChannel(internal))
        except Exception:
            pass
        try:
            return await client.get_entity(chat_id)
        except Exception:
            pass
        if deneme == 1:
            # Önbellek boş olabilir — sohbetleri gezip tekrar dene
            await _warm_dialogs(client)
    log.warning("Kanal kullanıcı hesabında çözülemedi (chat=%s)", chat_id)
    return None


async def user_sweep(chat_id: int, start: int, stop: int) -> Optional[tuple[int, int]]:
    """Kullanıcı hesabıyla siler (yaş sınırı yok). Kullanılamıyorsa None."""
    client = await get_user_client()
    if client is None:
        return None
    entity = await _user_entity(client, chat_id)
    if entity is None:
        log.warning("Kanal kullanıcı hesabında bulunamadı (chat=%s)", chat_id)
        return None

    import telethon.errors as terr

    silinen = 0
    for batch in iter_batches(start, stop, USER_BATCH):
        for deneme in range(6):
            try:
                sonuc = await client.delete_messages(entity, batch)
                # GERÇEK silinen sayısını Telegram'ın döndürdüğü pts_count verir.
                # len(batch) saymak yanlış: aralıkta zaten silinmiş ID'ler olabilir
                # ve kullanıcıya olduğundan fazla rakam bildirilir.
                try:
                    silinen += sum(getattr(r, "pts_count", 0) for r in (sonuc or []))
                except TypeError:
                    silinen += getattr(sonuc, "pts_count", 0)
                break
            except terr.FloodWaitError as e:
                bekle = min(e.seconds + 2, 300)
                log.info("Hesap flood limiti: %s sn bekleniyor", bekle)
                await asyncio.sleep(bekle)
            except Exception:
                log.exception("Grup silinemedi (%s..%s)", batch[0], batch[-1])
                break
        await asyncio.sleep(USER_GAP)
    return silinen, 0


async def _delete_one(bot: Bot, chat_id: int, mid: int) -> str:
    """Tek mesajı siler. Dönen: 'silindi' | 'eski' | 'yok' | 'hata'."""
    for _ in range(4):
        try:
            await bot.delete_message(chat_id, mid)
            return "silindi"
        except TelegramRetryAfter as e:
            await asyncio.sleep(min(float(e.retry_after) + 1.0, 120.0))
        except TelegramBadRequest as e:
            s = str(e)
            if TOO_OLD in s:
                return "eski"       # 48 saatten eski — Telegram izin vermiyor
            return "yok"            # zaten silinmiş / geçersiz ID
        except TelegramForbiddenError:
            raise
        except Exception:
            await asyncio.sleep(2)
    return "hata"


async def delete_batch(bot: Bot, chat_id: int, batch: list[int]) -> tuple[int, int]:
    """Bir 90'lık grubu siler. Dönen: (gerçekten_silinen, 48_saatten_eski).

    Flood limitine takılınca TEK DOĞRU HAMLE beklemektir: Telegram'ın söylediği
    süre kadar bekleyip AYNI grubu yeniden deneriz.

    ÖNEMLİ — Telegram 48 saatten eski mesajları bota SİLDİRMİYOR ("message can't
    be deleted"). Toplu silmede grupta tek bir eski mesaj varsa TÜM istek
    reddediliyor. Bu yüzden reddedilen grubu tek tek eleyip gerçekten kaç tanesini
    sildiğimizi sayıyoruz — eskiden hepsi silinmiş sayılıp kullanıcıya yanlış
    "temizlendi" raporu veriliyordu.
    """
    if not batch:
        return 0, 0
    tries = 0
    while True:
        try:
            await bot.delete_messages(chat_id=chat_id, message_ids=batch)
            return len(batch), 0
        except TelegramRetryAfter as e:
            tries += 1
            if tries >= 8:
                log.warning("Grup %s..%s: flood 8 kez üst üste, atlanıyor", batch[0], batch[-1])
                return 0, 0
            wait = min(float(e.retry_after) + 1.0, 120.0)
            log.info("Flood limiti: %.0f sn bekleniyor (chat=%s)", wait, chat_id)
            await asyncio.sleep(wait)
        except TelegramBadRequest:
            break  # grupta silinemeyen mesaj var -> tek tek ele ve say
        except TelegramForbiddenError:
            raise
        except Exception:  # ağ kopması vb. geçici hatalar temizliği ÖLDÜRMEZ
            tries += 1
            if tries >= 8:
                log.exception("Grup %s..%s: kalıcı ağ hatası, atlanıyor", batch[0], batch[-1])
                return 0, 0
            log.warning("Geçici hata, 3 sn sonra aynı grup yeniden denenecek")
            await asyncio.sleep(3)

    silinen = eski = 0
    for mid in batch:
        sonuc = await _delete_one(bot, chat_id, mid)
        if sonuc == "silindi":
            silinen += 1
        elif sonuc == "eski":
            eski += 1
        await asyncio.sleep(0.05)
    return silinen, eski


async def sweep(bot: Bot, chat_id: int, start: int, stop: int) -> tuple[int, int]:
    """start'tan stop'a (stop HARİÇ) ID aralığını 90'ar 90'ar temizler.

    Dönen: (gerçekten_silinen, 48_saatten_eski_olduğu_için_silinemeyen).

    Mesajlar yeniden eskiye doğru silindiği için 48 saatlik duvara bir kez
    çarpınca daha eskisi de silinemez — o noktada durup binlerce boş istek
    atmıyoruz.
    """
    # Önce kendi hesabınla dene — 48 saat sınırı olmadığı için tercih edilen yol
    sonuc = await user_sweep(chat_id, start, stop)
    if sonuc is not None:
        return sonuc

    silinen = 0
    eski = 0
    ard_arda_eski = 0

    for batch in iter_batches(start, stop):
        try:
            b_silinen, b_eski = await delete_batch(bot, chat_id, batch)
        except TelegramForbiddenError:
            raise  # kanaldan atılmışız — devam etmenin anlamı yok
        except Exception:
            log.exception("Grup beklenmedik şekilde patladı, atlanıp devam ediliyor")
            b_silinen, b_eski = 0, 0

        silinen += b_silinen
        eski += b_eski

        if b_eski and not b_silinen:
            ard_arda_eski += 1
            if ard_arda_eski >= 2:
                # 48 saat duvarını geçtik; geri kalanı Telegram zaten sildirmez
                kalan = max(0, batch[-1] - stop - 1)
                eski += kalan
                log.info(
                    "48 saat sınırına ulaşıldı (chat=%s). Silinen=%s, eski=%s",
                    chat_id, silinen, eski,
                )
                break
        else:
            ard_arda_eski = 0

        await asyncio.sleep(BATCH_DELAY)

    return silinen, eski


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
    """Ana mesajdan SONRAKİ (daha yeni) her şeyi siler; (taranan, atlanan_grup) döner.

    Tarama bir turda sınıra takılabilir (yedek reaksiyon taraması sınırlı) ya da
    temizlik sürerken kanala yeni mesaj düşebilir. Bu yüzden yeni bir şey
    bulunamayana kadar tur tekrarlanır — tek komutla iş gerçekten biter.
    """
    total_done = 0
    total_failed = 0
    ceiling = anchor_id
    for _ in range(12):
        latest = await find_latest_id(bot, chat_id, ceiling, username)
        if latest <= ceiling:
            break
        done, failed = await sweep(bot, chat_id, start=latest, stop=ceiling)
        total_done += done
        total_failed += failed
        ceiling = latest  # bir sonraki tur yalnızca bunun ÜSTÜNE bakar
        if failed:
            break  # limitlere takıldık; kullanıcıya bildirilecek
    return total_done, total_failed


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

async def _kanal_basligi(client, entity, chat_id: int) -> str:
    try:
        bilgi = await client.get_entity(entity)
        return getattr(bilgi, "title", None) or str(chat_id)
    except Exception:
        return str(chat_id)


async def _istekci_yonetici_mi(bot: Bot, chat_id: int, istekci_id: int) -> Optional[bool]:
    """Linki gönderen kişi kanalda yönetici mi? Karar verilemezse None.

    Bot herkese açık olduğu için bu kontrol şart: olmadan, hesabımızın ya da
    botun yönetici olduğu bir kanalın linkini yabancı biri gönderip silme
    başlatabilirdi.
    """
    try:
        uye = await bot.get_chat_member(chat_id, istekci_id)
        return getattr(uye, "status", "") in ("administrator", "creator")
    except (TelegramBadRequest, TelegramForbiddenError):
        return None


async def _hesap_yetki_kontrol(bot: Bot, chat_id: int, istekci_id: int) -> tuple[bool, str]:
    """Hesap yolu için güvenlik kapısı ve gerekiyorsa otomatik yetkilendirme.

    İki şart birden sağlanmalı:
      • Bağlı kullanıcı hesabı kanalda silme yapabilmeli
      • Linki GÖNDEREN kişi de o kanalda yönetici olmalı

    Hesabın yetkisi yoksa ve bot kanalda kalıcı yönetici duruyorsa, yetkiyi bot
    otomatik verir (bkz. yedek.py). İş bitince yedek_kapat geri alır.

    Dönen: (uygun_mu, başlık_veya_hata_mesajı)
    """
    client = await get_user_client()
    if client is None:
        return False, (
            "❌ Bu kanala ulaşamıyorum. Beni kanala <b>yönetici</b> olarak ekle, "
            "sonra linki tekrar gönder."
        )

    try:
        ben = await client.get_me()
    except Exception:
        log.warning("Hesap kimliği okunamadı", exc_info=True)
        return False, "❌ Bağlı hesaba ulaşamadım, biraz sonra tekrar dene."
    sahibi_mi = ben is not None and istekci_id == ben.id

    global _dialogs_loaded
    _dialogs_loaded = False  # yeni verilen yetkiler görünsün
    entity = await _user_entity(client, chat_id)

    # --- Hesabın yetkisi yok: bot kalıcı yönetici ise yetkiyi o versin --------
    if entity is None:
        uygun, hata = await yedek.kullanilabilir_mi(bot, chat_id)
        if not uygun:
            return False, hata

        if not sahibi_mi:
            # İKİ ŞART birden: hem bu kanalın yöneticisi olacak, hem de otomatik
            # moda izinli listesinde olacak. İkincisi olmadan, botu kendi kanalına
            # ekleyen herhangi bir yabancı hesabı içeri çektirebilirdi.
            if not await otomatik_izinli_mi(istekci_id):
                return False, (
                    "⛔ Otomatik mod için izinli değilsin.\n\n"
                    "Bot sahibinin seni izinli listesine eklemesi gerekiyor. "
                    f"Ona bu numarayı ilet: <code>{istekci_id}</code>\n\n"
                    "Alternatif: bağlı hesabı kanalda elle <b>yönetici</b> yapıp "
                    "<i>Mesajları sil</i> yetkisi ver — o zaman izin gerekmez."
                )
            yetkili = await _istekci_yonetici_mi(bot, chat_id, istekci_id)
            if yetkili is not True:
                return False, (
                    "⛔ Bu kanalda yönetici görünmüyorsun; güvenlik gereği "
                    "işlemi başlatamam."
                )

        ok, hata = await yedek.yetki_ver(bot, client, chat_id, ben.id)
        if not ok:
            return False, hata

        _dialogs_loaded = False
        entity = await _user_entity(client, chat_id)
        if entity is None:
            await yedek.yetki_al(bot, chat_id, ben.id)  # yarım kalan yetkiyi bırakma
            return False, (
                "❌ Yetki verildi ama kanalı hâlâ göremiyorum. "
                "Birkaç saniye sonra linki tekrar gönder."
            )

        _yedek_verdi.add(chat_id)
        log.info("Otomatik yetkilendirme tamam: chat=%s", chat_id)
        return True, await _kanal_basligi(client, entity, chat_id)

    # --- Hesabın zaten yetkisi var -------------------------------------------
    # İsteği yapan bağlı hesabın SAHİBİ ise ek kontrole gerek yok: kendi
    # hesabıyla, kendi yetkisiyle siliyor. (Bazı kanallarda hesap mesaj
    # silebiliyor ama yönetici listesini okuyamıyor.)
    if sahibi_mi:
        return True, await _kanal_basligi(client, entity, chat_id)

    yetkili = await _istekci_yonetici_mi(bot, chat_id, istekci_id)

    if yetkili is None:
        try:
            from telethon.tl.functions.channels import GetParticipantRequest
            from telethon.tl.types import (
                ChannelParticipantAdmin,
                ChannelParticipantCreator,
            )

            res = await client(
                GetParticipantRequest(channel=entity, participant=istekci_id)
            )
            yetkili = isinstance(
                res.participant, (ChannelParticipantAdmin, ChannelParticipantCreator)
            )
        except Exception:
            log.debug("Tekil yönetici sorgusu olmadı (chat=%s)", chat_id, exc_info=True)

    if yetkili is None:
        try:
            from telethon.tl.types import ChannelParticipantsAdmins

            yoneticiler = [
                u.id
                async for u in client.iter_participants(
                    entity, filter=ChannelParticipantsAdmins
                )
            ]
            yetkili = istekci_id in yoneticiler
        except Exception:
            log.warning("Yönetici listesi okunamadı (chat=%s)", chat_id)
            return False, (
                "⛔ Bu kanalda yönetici olduğunu doğrulayamadım; güvenlik gereği "
                "işlemi başlatamam. Kanal sahibiysen bağlı hesabı kullan."
            )

    if not yetkili:
        return False, (
            "⛔ Bu kanalda yönetici görünmüyorsun; güvenlik gereği işlemi başlatamam."
        )

    return True, await _kanal_basligi(client, entity, chat_id)


async def prepare_job(
    message: Message,
    bot: Bot,
    chat_ref: Union[int, str],
    anchor_id: int,
    show_preview: bool = True,
) -> None:
    user_id = message.from_user.id

    # İki yol var:
    #   1) BOT yolu   — bot kanalda yönetici. Klasik; ama 48 saat sınırı var.
    #   2) HESAP yolu — bot kanalda YOK ama bağlı kullanıcı hesabı yönetici.
    #                   Yaş sınırı yok. Bot herkese açık olduğu için burada
    #                   güvenlik şart: linki GÖNDEREN kişi de o kanalda yönetici
    #                   olmalı, yoksa yabancı biri başkasının kanalını sildirebilir.
    chat = None
    try:
        chat = await bot.get_chat(chat_ref)
    except (TelegramBadRequest, TelegramForbiddenError):
        chat = None

    if chat is not None:
        if chat.type not in ("channel", "supergroup", "group"):
            await message.answer("❌ Bu bir kanal/grup mesajı linki değil.")
            return
        try:
            admins = await bot.get_chat_administrators(chat.id)
        except (TelegramBadRequest, TelegramForbiddenError):
            admins = []
        me_admin = next((a for a in admins if a.user.id == bot.id), None)
        istekci_admin = next((a for a in admins if a.user.id == user_id), None)
        bot_silebilir = bool(me_admin and getattr(me_admin, "can_delete_messages", False))

        # Hesap yolu ÖNCE denenir: botun 48 saat sınırı var, hesabın yok.
        # Bot kanalda yönetici olsa bile hesapla silmek her zaman daha iyi.
        ok, baslik = False, ""
        if os.getenv("USER_SESSION", "").strip():
            ok, baslik = await _hesap_yetki_kontrol(bot, chat.id, user_id)
            if ok:
                chat_id, kullanici_adi = chat.id, chat.username

        if not ok:
            if not bot_silebilir:
                await message.answer(baslik if baslik else (
                    "❌ Bu kanalda silme yetkim yok. Beni <b>yönetici</b> yap ve "
                    "<i>Mesajları sil</i> yetkisini ver."
                ))
                return
            if istekci_admin is None:
                await message.answer(
                    "⛔ Bu kanalda yönetici görünmüyorsun; güvenlik gereği işlemi başlatamam."
                )
                return
            chat_id, baslik, kullanici_adi = chat.id, chat.title, chat.username
    else:
        # Bot kanalda hiç yok — yalnızca hesap yolu denenebilir
        if not isinstance(chat_ref, int):
            await message.answer(
                "❌ Bu kanala ulaşamıyorum. Beni kanala <b>yönetici</b> olarak ekle "
                "ya da bağlı hesabı yönetici yap."
            )
            return
        ok, baslik = await _hesap_yetki_kontrol(bot, chat_ref, user_id)
        if not ok:
            await message.answer(baslik)
            return
        chat_id, kullanici_adi = chat_ref, None

    note = ""
    if show_preview and chat is not None:
        try:
            await bot.forward_message(
                user_id, chat_id, anchor_id, disable_notification=True
            )
            note = "⬆️ Ana mesaj bu — burada duracağım.\n\n"
        except TelegramBadRequest as e:
            if "not found" in str(e).lower():
                note = (
                    "⚠️ Bu ID'de mesaj bulamadım (silinmiş olabilir). "
                    "Yine de bu ID'yi sınır kabul edebilirim.\n\n"
                )

    job = {
        "chat_id": chat_id,
        "anchor_id": anchor_id,
        "title": baslik or str(chat_id),
        "username": kullanici_adi,  # sessiz MTProto taraması için (özel kanalda None)
    }
    pending[user_id] = job
    await save_last_job(user_id, job)

    # Hangi yolla silineceğini kullanıcıya baştan söyle
    hesap_var = await get_user_client() is not None
    yol = (
        "🔓 Hesap modu — yaş sınırı yok, eski mesajlar da silinir."
        if hesap_var
        else "🤖 Bot modu — Telegram kuralı gereği yalnızca 48 saatten yeni mesajlar silinebilir."
    )

    await message.answer(
        f"{note}"
        f"📋 Kanal: <b>{html.escape(job['title'])}</b>\n"
        f"🎯 Ana mesaj ID: <code>{anchor_id}</code>\n"
        f"{yol}\n\n"
        "Hangi yönde sileyim? (Ana mesajın kendisi <b>silinmez</b>)",
        reply_markup=confirm_kb(),
    )


@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    metin = START_TEXT
    # Otomatik mod izne bağlı; kişi onay için numarasını aramasın diye burada gösteriyoruz.
    if not await otomatik_izinli_mi(message.from_user.id):
        metin += (
            f"\n\n🆔 Senin numaran: <code>{message.from_user.id}</code>\n"
            "Otomatik modu kullanacaksan bu numarayı bot sahibine gönder."
        )
    await message.answer(metin)


@dp.message(Command("izinver", "izinal", "izinliler"))
async def on_izin(message: Message) -> None:
    """Otomatik moda kimin erişebileceğini yönetir — YALNIZCA hesap sahibi.

    Bot herkese açık olduğu için otomatik yetkilendirme davete bağlı: hesabı
    kanalına çektirebilecek kişileri sahibi tek tek onaylar.
    """
    sahip = await hesap_sahibi_id()
    if sahip is None:
        await message.answer(
            "❌ Bağlı hesap yok; otomatik mod zaten kapalı. "
            "(<code>USER_SESSION</code> tanımlı değil.)"
        )
        return
    if message.from_user.id != sahip:
        await message.answer("⛔ Bu komut yalnızca bot sahibine açık.")
        return

    parcalar = (message.text or "").split()
    komut = parcalar[0].lstrip("/").split("@")[0]
    izinliler = _izinlileri_oku()

    if komut == "izinliler":
        if not izinliler:
            await message.answer(
                "📋 İzinli kimse yok — otomatik modu şu an yalnızca sen kullanabilirsin.\n\n"
                "Eklemek için: <code>/izinver 123456789</code>"
            )
            return
        satirlar = "\n".join(f"• <code>{k}</code>" for k in sorted(izinliler))
        await message.answer(f"📋 <b>Otomatik moda izinli kişiler</b>\n{satirlar}")
        return

    if len(parcalar) < 2 or not parcalar[1].lstrip("-").isdigit():
        await message.answer(
            f"Kullanım: <code>/{komut} 123456789</code>\n\n"
            "Numarayı arkadaşın öğrenebilir: bota <code>/start</code> yazıp "
            "reddedilirse bot numarasını kendisi söyler. "
            "Ya da @userinfobot'a yazsın."
        )
        return

    hedef = int(parcalar[1])
    if komut == "izinver":
        if hedef in izinliler:
            await message.answer(f"ℹ️ <code>{hedef}</code> zaten izinli.")
            return
        izinliler.add(hedef)
        await _izinlileri_yaz(izinliler)
        log.info("Otomatik moda izin verildi: %s", hedef)
        await message.answer(
            f"✅ <code>{hedef}</code> artık otomatik modu kullanabilir.\n\n"
            "Kendi <b>yönetici olduğu</b> kanallarda, bot o kanalda yönetici olmak "
            "şartıyla temizlik başlatabilir."
        )
    else:
        if hedef not in izinliler:
            await message.answer(f"ℹ️ <code>{hedef}</code> zaten izinli değil.")
            return
        izinliler.discard(hedef)
        await _izinlileri_yaz(izinliler)
        log.info("Otomatik mod izni kaldırıldı: %s", hedef)
        await message.answer(f"🚫 <code>{hedef}</code> için otomatik mod kapatıldı.")


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

    # Servis kapalıyken basılmış ESKİ butonlar, bot geri gelince Telegram
    # tarafından tekrar oynatılıyor. Böyle bir tıklamayla silme BAŞLATILMAMALI.
    if BOOT_TS and cb.message and cb.message.date:
        if cb.message.date.timestamp() < BOOT_TS - 60:
            try:
                await cb.answer(
                    "Bu buton eski bir oturumdan kalma. Linki tekrar gönder.",
                    show_alert=True,
                )
            except TelegramBadRequest:
                pass
            return

    # Ekstra mesaj yok: küçük bir bildirim baloncuğu gösterip sessizce işe başla
    try:
        await cb.answer("🧹 Başladım, bitince haber veririm")
    except TelegramBadRequest:
        # "query is too old" — tıklama zaman aşımına uğramış; işe başlamıyoruz
        log.info("Eski buton tıklaması yok sayıldı (chat=%s)", chat_id)
        return
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest:
        pass

    active_chats.add(chat_id)
    try:
        if action == "after":
            deleted, too_old = await clean_after(
                bot, chat_id, anchor_id, job.get("username")
            )
        else:
            deleted, too_old = await clean_before(bot, chat_id, anchor_id)

        if deleted <= 0 and too_old <= 0:
            text = "✅ Silinecek mesaj yoktu; orası zaten temiz."
        elif deleted <= 0:
            text = (
                "⚠️ <b>Hiçbir mesaj silinemedi.</b>\n\n"
                f"Bu aralıktaki mesajların hepsi <b>48 saatten eski</b>. "
                "Telegram, botların 48 saatten eski mesajları silmesine izin vermiyor — "
                "bu bir yetki sorunu değil, Telegram'ın kuralı.\n\n"
                "Eski mesajlar için: kanalda mesajlara uzun bas → seç → sil, "
                "ya da kanal ayarlarından geçmişi temizle."
            )
        else:
            text = (
                f"✅ Bitti! <b>{deleted}</b> mesaj silindi.\n"
                f"🎯 Ana mesaj (<code>{anchor_id}</code>) yerinde duruyor."
            )
            if too_old:
                text += (
                    f"\n\n⚠️ <b>{too_old}</b> mesaj silinemedi çünkü <b>48 saatten eski</b>. "
                    "Telegram botların bu kadar eski mesajları silmesine izin vermiyor."
                )
                hesap = os.getenv("USER_SESSION", "").strip()
                if hesap:
                    text += (
                        "\n\n💡 Bunu aşmanın yolu var: bağlı <b>kullanıcı hesabını</b> bu kanalda "
                        "<b>yönetici</b> yapın ve <i>Mesajları sil</i> yetkisini verin. "
                        "O zaman eski mesajlar da silinebilir — kullanıcı hesaplarında "
                        "48 saat sınırı yok."
                    )
        try:
            await bot.send_message(cb.from_user.id, text)
        except Exception:  # rapor gönderilemese de temizlik tamamlanmıştır
            log.warning("Sonuç mesajı gönderilemedi (user=%s)", cb.from_user.id)
        log.info(
            "Temizlik bitti: chat=%s SILINEN=%s eski(48s+)=%s", chat_id, deleted, too_old
        )
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
        kapanis = await yedek_kapat(bot, chat_id)
        if kapanis:
            try:
                await bot.send_message(cb.from_user.id, kapanis.strip())
            except Exception:
                log.debug("Kapanış bilgisi gönderilemedi", exc_info=True)


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


async def run_polling() -> None:
    """Botu MEVCUT event loop içinde çalıştırır (kendi sunucusunu açmaz).

    Honeypot Radar ile aynı süreçte yaşayabilmesi için var: iki ayrı servis
    ücretsiz barındırma kotasını ikiye katlıyordu (ayda 730 saat yerine 1460).
    Tek süreçte birleşince tek servis kotasına sığıyor.

    BOT_TOKEN tanımlı değilse sessizce hiçbir şey yapmaz.
    """
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        log.info("BOT_TOKEN yok — kanal temizlik botu devre dışı")
        return

    global BOOT_TS
    BOOT_TS = time.time()
    bot = Bot(token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await bot.delete_webhook()
    me = await bot.get_me()
    log.info("Kanal temizlik botu başladı: @%s", me.username)
    await dp.start_polling(
        bot, allowed_updates=["message", "channel_post", "callback_query"]
    )


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
