"""bot.py içindeki saf/mantık fonksiyonlarının hızlı testleri.

Çalıştırmak için:  .venv/bin/python test_bot.py
"""
import asyncio

import bot


def test_parse():
    p = bot.parse_message_link
    # herkese açık kanal linkleri
    assert p("https://t.me/kanalim/123") == ("@kanalim", 123)
    assert p("t.me/kanalim/123") == ("@kanalim", 123)
    assert p("https://telegram.me/kanalim/9?single") == ("@kanalim", 9)
    assert p("https://t.me/s/kanalim/77") == ("@kanalim", 77)
    # özel kanal linkleri (t.me/c/...)
    assert p("https://t.me/c/2483061605/55") == (-1002483061605, 55)
    assert p("https://t.me/c/1234567/100/2050") == (-1001234567, 2050)  # konu linki
    # metin içinde geçen link
    assert p("şu mesaja kadar sil: https://t.me/kanalim/500 lütfen") == ("@kanalim", 500)
    # geçersizler
    assert p("merhaba") is None
    assert p("https://t.me/kanalim") is None
    assert p("https://google.com/abc/1") is None
    assert p("notat.me/abc/5") is None


def test_normalize_username():
    n = bot.normalize_username
    assert n("@abc") == "abc"
    assert n("t.me/abc") == "abc"
    assert n("https://t.me/abc?x=1") == "abc"
    assert n(" @Abc_123 ") == "Abc_123"


def test_batches():
    # 100 mesaj -> 90 + 10
    batches = list(bot.iter_batches(100, 0, size=90))
    assert [len(b) for b in batches] == [90, 10]
    flat = [i for b in batches for i in b]
    assert flat == list(range(100, 0, -1))  # yeniden eskiye, eksiksiz, tekrarsız

    # küçük aralık -> tek grup
    batches = list(bot.iter_batches(455, 400, size=90))
    assert [len(b) for b in batches] == [55]
    assert batches[0][0] == 455 and batches[0][-1] == 401

    # boş aralıklar
    assert list(bot.iter_batches(5, 5)) == []
    assert list(bot.iter_batches(5, 7)) == []

    # BÜYÜK aralık (10.000 mesaj): tüm ID'ler eksiksiz, tekrarsız, hiçbir grup 90'ı geçmez,
    # ana mesaj (anchor=1000) asla listede olmaz. "Sadece 90 siliniyor" hatasının tersini kanıtlar.
    batches = list(bot.iter_batches(11000, 1000))
    flat = [i for b in batches for i in b]
    assert len(flat) == 10000
    assert len(set(flat)) == 10000            # tekrar yok
    assert 1000 not in flat                    # anchor korunur
    assert min(flat) == 1001 and max(flat) == 11000
    assert max(len(b) for b in batches) == 90  # hiçbir grup 90'ı geçmez
    assert sum(len(b) for b in batches) == 10000
    assert bot.BATCH_SIZE == 90


def _run_mt(existing, floor):
    """_mt_find_latest'i sahte bir telethon istemcisiyle çalıştırır."""
    class FakeMsg:
        def __init__(self, mid):
            self.id = mid

    class FakeClient:
        def __init__(self, ex):
            self.existing = set(ex)
            self.calls = 0

        async def get_messages(self, entity, ids):
            self.calls += 1
            return [FakeMsg(i) if i in self.existing else None for i in ids]

    c = FakeClient(existing)
    latest = asyncio.run(bot._mt_find_latest(c, "kanal", floor))
    return latest, c.calls


def test_mt_scan():
    """_mt_find_latest: silinmiş büyük ID boşluklarını aşıp gerçek son mesajı bulmalı."""
    # 1) ardışık dolu mesajlar
    latest, _ = _run_mt(range(1000, 1501), 1000)
    assert latest == 1500, latest

    # 2) BÜYÜK boşluk: önceki temizlik 1001..4999'u silmiş, yeniler 5000..5040
    latest, _ = _run_mt(list(range(5000, 5041)) + [1000], 1000)
    assert latest == 5040, latest

    # 3) hiç yeni mesaj yok -> floor döner
    latest, _ = _run_mt([1000], 1000)
    assert latest == 1000, latest

    # 4) tolerans içindeki uç boşluk (15k) bile aşılmalı
    gap = bot.EMPTY_TOLERANCE - 5000
    latest, _ = _run_mt([1000, 1000 + gap, 1000 + gap + 1], 1000)
    assert latest == 1000 + gap + 1, latest

    # 5) boşluk ortasında TEK mesaj: sıçramalı taramanın kaçırdığını yoğun tarama bulur
    latest, _ = _run_mt([1000, 8123], 1000)
    assert latest == 8123, latest


def test_mt_scan_hata_yutulmaz():
    """get_messages hata verirse _mt_find_latest None dönmeli (yanlış-küçük sonuç ÜRETMEZ)."""
    class BozukClient:
        async def get_messages(self, entity, ids):
            raise RuntimeError("ağ koptu")

    latest = asyncio.run(bot._mt_find_latest(BozukClient(), "kanal", 1000))
    assert latest is None, latest


def test_sweep_floodda_devam_eder():
    """sweep: ilk grup floodda bekleyip başarılı olmalı; TÜM gruplar silinmeli, yarıda kalmamalı.

    Eski hatanın (sadece ilk 90) geri gelmediğini kanıtlar.
    """
    silinen = []
    durum = {"flood_kaldi": 2}

    class FakeBot:
        id = 1

        async def delete_messages(self, chat_id, message_ids):
            if durum["flood_kaldi"] > 0:
                durum["flood_kaldi"] -= 1
                raise bot.TelegramRetryAfter(
                    method=None, message="Flood", retry_after=0
                )
            silinen.extend(message_ids)

    # 250 mesaj -> 90 + 90 + 70 = 3 grup; ilk grup 2 kez flood yiyip 3. denemede geçmeli
    done, failed = asyncio.run(
        bot.sweep(FakeBot(), chat_id=-100123, start=1250, stop=1000)
    )
    assert failed == 0, failed
    assert done == 250, done
    assert len(silinen) == 250 and len(set(silinen)) == 250
    assert min(silinen) == 1001 and max(silinen) == 1250   # anchor(1000) korunur


if __name__ == "__main__":
    test_parse()
    test_normalize_username()
    test_batches()
    test_mt_scan()
    test_mt_scan_hata_yutulmaz()
    test_sweep_floodda_devam_eder()
    print("OK - tum testler gecti")
