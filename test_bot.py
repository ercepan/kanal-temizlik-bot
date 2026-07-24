"""bot.py içindeki saf fonksiyonların hızlı testleri.

Çalıştırmak için:  .venv/bin/python test_bot.py
"""
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

    # 1000 mesaj: hiçbir grup 90'ı geçmesin, ana mesaj (2000) asla listede olmasın
    batches = list(bot.iter_batches(3000, 2000))
    flat = [i for b in batches for i in b]
    assert 2000 not in flat
    assert min(flat) == 2001 and max(flat) == 3000
    assert max(len(b) for b in batches) == 90
    assert bot.BATCH_SIZE == 90


if __name__ == "__main__":
    test_parse()
    test_batches()
    print("OK - tum testler gecti")
