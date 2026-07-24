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
PROGRESS_EVERY = 5     # kaç grupta bir ilerleme mesajı güncellenir
DATA_FILE = Path(__file__).with_name("son_isler.json")
SEEN_FILE = Path(__file__).with_name("son_gorulen.json")
FRONTIER_MISS = 3      # art arda bu kadar boş ID görünce "son mesaj bu" der
FRONTIER_CAP = 150     # sessiz taramada en fazla bu kadar ID kontrol edilir

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
    "ℹ️ Mesajları 90'lık gruplar halinde, flood korumalı silerim.\n"
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


async def safe_edit(msg: Message, text: str) -> None:
    for _ in range(2):
        try:
            await msg.edit_text(text)
            return
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.5)
        except TelegramBadRequest:
            return


# --------------------------------------------------------------- silme işi

async def delete_batch(bot: Bot, chat_id: int, batch: list[int]) -> None:
    """Bir grubu siler; flood beklemesi ve toplu silme patlarsa tek tek dener."""
    if not batch:
        return
    for _ in range(3):
        try:
            await bot.delete_messages(chat_id=chat_id, message_ids=batch)
            return
        except TelegramRetryAfter as e:
            log.info("Flood limiti: %s sn bekleniyor", e.retry_after)
            await asyncio.sleep(e.retry_after + 1)
        except TelegramBadRequest:
            break  # toplu silme olmadı -> tek tek dene
    for mid in batch:
        try:
            await bot.delete_message(chat_id, mid)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
            try:
                await bot.delete_message(chat_id, mid)
            except (TelegramBadRequest, TelegramRetryAfter):
                pass
        except TelegramBadRequest:
            pass  # zaten yok / silinemez -> atla
        await asyncio.sleep(0.03)


async def sweep(bot: Bot, chat_id: int, start: int, stop: int, status: Optional[Message] = None) -> int:
    """start'tan stop'a (stop hariç) tüm ID aralığını temizler, sayaç döner."""
    total = max(0, start - stop)
    done = 0
    for i, batch in enumerate(iter_batches(start, stop)):
        await delete_batch(bot, chat_id, batch)
        done += len(batch)
        if status is not None and (i + 1) % PROGRESS_EVERY == 0 and done < total:
            await safe_edit(
                status, f"🧹 Siliniyor... {done}/{total} (%{done * 100 // total})"
            )
        if done < total:
            await asyncio.sleep(BATCH_DELAY)
    return done


async def message_exists(bot: Bot, chat_id: int, message_id: int) -> bool:
    """Mesaj var mı? Boş reaksiyon listesi göndererek kontrol eder — kanalda hiçbir iz bırakmaz."""
    while True:
        try:
            await bot.set_message_reaction(chat_id=chat_id, message_id=message_id, reaction=[])
            return True
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.5)
        except TelegramBadRequest as e:
            s = str(e).lower()
            return not ("not found" in s or "message_id_invalid" in s)


async def find_latest_id(bot: Bot, chat_id: int) -> int:
    """Kanaldaki son mesaj ID'sini bulur.

    Normal yol: kanal postlarından takip edilen ID'den başlayıp görünmez reaksiyon
    kontrolüyle ileri tarama (tamamen sessiz). Takip verisi yoksa (ör. bot yeni
    yeniden başladı ve kanala hiç post düşmedi) son çare olarak sessiz bildirimli
    sonda mesajı atılıp anında silinir.
    """
    base = last_seen.get(chat_id, 0)
    if base > 0:
        latest, misses, nxt, steps = base, 0, base + 1, 0
        while misses < FRONTIER_MISS and steps < FRONTIER_CAP:
            if await message_exists(bot, chat_id, nxt):
                latest, misses = nxt, 0
            else:
                misses += 1
            nxt += 1
            steps += 1
        if steps < FRONTIER_CAP:
            await remember_seen(chat_id, latest)
            return latest
        log.warning("Sessiz tarama sınıra takıldı, sonda mesajına dönülüyor: %s", chat_id)
    probe = await bot.send_message(chat_id, "🧹", disable_notification=True)
    await delete_batch(bot, chat_id, [probe.message_id])
    await remember_seen(chat_id, probe.message_id)
    return probe.message_id - 1


async def clean_after(bot: Bot, chat_id: int, anchor_id: int, status: Optional[Message] = None) -> int:
    """Ana mesajdan SONRAKİ (daha yeni) her şeyi siler."""
    latest = await find_latest_id(bot, chat_id)
    if latest <= anchor_id:
        return 0
    return await sweep(bot, chat_id, start=latest, stop=anchor_id, status=status)


async def clean_before(bot: Bot, chat_id: int, anchor_id: int, status: Optional[Message] = None) -> int:
    """Ana mesajdan ÖNCEKİ (daha eski) her şeyi siler."""
    if anchor_id <= 1:
        return 0
    return await sweep(bot, chat_id, start=anchor_id - 1, stop=0, status=status)


# --------------------------------------------------- @kullanıcıadı çözümleme

# Bot API @kullanıcıadı -> kişi çözmeye izin vermez; API_ID/API_HASH tanımlıysa
# aynı bot token'ıyla MTProto üzerinden çözeriz. Tanımlı değilse None döner ve
# akış kişi seçme butonuna düşer.
_mtproto = None  # None: henüz açılmadı, False: açılamadı (tekrar deneme)
_mtproto_lock = asyncio.Lock()


async def _get_mtproto():
    global _mtproto
    if _mtproto is False:
        return None
    api_id = os.getenv("API_ID", "").strip()
    api_hash = os.getenv("API_HASH", "").strip()
    token = os.getenv("BOT_TOKEN", "").strip()
    if not (api_id.isdigit() and api_hash and token):
        return None
    async with _mtproto_lock:
        if _mtproto is None:
            try:
                from telethon import TelegramClient
                from telethon.sessions import MemorySession

                client = TelegramClient(MemorySession(), int(api_id), api_hash)
                await client.start(bot_token=token)
                _mtproto = client
                log.info("MTProto çözümleyici hazır")
            except Exception:
                log.exception("MTProto istemcisi açılamadı — kişi seçici kullanılacak")
                _mtproto = False
                return None
    return _mtproto or None


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

    job = {"chat_id": chat.id, "anchor_id": anchor_id, "title": chat.title or str(chat.id)}
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
            deleted = await clean_after(bot, chat_id, anchor_id)
        else:
            deleted = await clean_before(bot, chat_id, anchor_id)
        if deleted <= 0:
            text = "✅ Silinecek mesaj yoktu; orası zaten temiz."
        else:
            text = (
                f"✅ Bitti! ~<b>{deleted}</b> mesajlık aralık temizlendi.\n"
                f"🎯 Ana mesaj (<code>{anchor_id}</code>) yerinde duruyor."
            )
        await bot.send_message(cb.from_user.id, text)
        log.info("Temizlik bitti: chat=%s aralık=%s", chat_id, deleted)
    except TelegramForbiddenError:
        await bot.send_message(
            cb.from_user.id,
            "❌ Kanala erişimim gitti (atılmış ya da yetkim alınmış olabilir).",
        )
    except Exception as e:  # noqa: BLE001
        log.exception("Temizlik sırasında hata")
        await bot.send_message(
            cb.from_user.id,
            f"❌ Hata: <code>{html.escape(str(e))}</code>\n"
            "Aynı linki gönderip yeniden başlatırsan kaldığı yerden toparlar.",
        )
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
