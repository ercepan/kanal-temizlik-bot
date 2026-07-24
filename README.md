# 🧹 Kanal Temizlik Botu

Telegram kanalındaki mesajları, senin belirlediğin **ana mesaja kadar** siler.
Ana mesaj silinmez — bot orayı "durma noktası" kabul eder.

- Mesajları **90'lık gruplar** halinde siler (Telegram limiti 100 ama garanti olsun diye 90).
- Flood limitine takılırsa bekler, kaldığı yerden devam eder.
- Toplu silme hata verirse o grubu **tek tek** silerek toparlar.
- Sadece **kanal yöneticileri** botu kullanabilir (güvenlik kontrolü var).
- İşlem yarıda kalırsa aynı linki gönderip tekrar başlat — silinenleri atlar, kaldığı yerden toparlar.

## Kurulum

**1) Bot oluştur**

- Telegram'da [@BotFather](https://t.me/BotFather)'a git → `/newbot` → isim ver → **token**'ı kopyala.

**2) Token'ı tanıt**

Bu klasörde `.env` dosyası oluştur (örnek: `.env.example`):

```
BOT_TOKEN=123456789:AAaa-senin-tokenin
```

**3) Kütüphaneleri kur ve çalıştır**

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python bot.py
```

Bot çalıştığı sürece komutları dinler. Sunucuda 7/24 çalıştırmak istersen `screen`, `tmux` ya da `systemd` kullanabilirsin.

**4) Botu kanala ekle**

- Kanal → Yöneticiler → Yönetici Ekle → botunu bul.
- **"Mesajları sil"** yetkisini mutlaka aç.

## Kullanım

1. Kanalda **ana mesaja** bas → **Bağlantıyı Kopyala**.
2. Linki bota **özel mesajdan** gönder (ya da ana mesajı bota **ilet/forward** et).
3. Bot ana mesajı gösterip onay butonları sunar:
   - **🧹 Sonrakileri sil** → ana mesajdan SONRA atılan her şeyi siler, ana mesajda durur. *(tipik kullanım)*
   - **🗑 Öncekileri sil** → ana mesajdan ÖNCEKİ eski mesajları siler.
4. İlerlemeyi özel mesajdan takip et. Bitince rapor verir.

`/tekrar` → en son kullandığın kanal + ana mesajla işi yeniden başlatır (ör. her gün aynı sabit mesaja kadar temizlik).

### Kanaldan kişi atma

`/kanaldanat @kullanici` → kişiyi kanaldan atar ve yasaklar (tekrar giremez).

- Bot yasaklayabilmek için kanalda **"Kullanıcıları yasakla"** yetkisine de sahip olmalı.
- Telegram, botlara her kullanıcı adını çözme izni vermez; ad çözülemezse bot bir
  **kişi seçme butonu** gösterir — listeden arayıp seçersin, anında atar.
- `/kanaldanat 123456789` şeklinde sayısal ID ile de çalışır.
- Yanlış kişiyi attıysan sonuç mesajındaki **↩️ Geri al** butonuyla yasağı kaldırırsın.
- Hangi kanaldan atacağını, bota en son link gönderdiğin kanaldan anlar; farklı kanal
  için önce o kanaldan bir mesaj linki gönder.

## Nasıl çalışıyor?

Bot API kanal geçmişini okuyamaz; ama kanal mesaj ID'leri sıralı arttığı için bot
son mesaj ID'sini **kanala hiçbir şey atmadan** bulur:

1. **MTProto taraması** (API_ID/API_HASH varsa): 100'lük pencerelerle mesaj varlığı
   sorgulanır. Önceki temizliklerden kalan silinmiş-ID boşlukları aşılır.
2. **Reaksiyon taraması** (yedek): boş reaksiyon listesi gönderilerek mesajın var olup
   olmadığı anlaşılır — kanalda hiçbir iz bırakmaz.

Sonra `ana mesaj ID + 1` ile `son ID` arasındaki tüm ID'ler **90'lık gruplar** halinde
`deleteMessages` ile temizlenir. Arada zaten silinmiş ID'ler varsa Telegram bunları
kendiliğinden atlar.

**Flood (hız) limiti:** Telegram limit koyduğunda bot **bekler ve aynı 90'lık grubu
yeniden dener** — tek tek silmeye düşmez (o, limiti 90 katına çıkarıp mesajların
atlanmasına yol açıyordu). Tarama sınıra takılsa bile turlar tekrarlanır, böylece tek
komutla ana mesaja kadar her şey temizlenir.

> ℹ️ Bot kanala **hiçbir mesaj göndermez**. Tüm bildirimler sana özelden gelir ve
> temizlik sırasında ara mesaj atılmaz; yalnızca iş bitince tek bir özet gönderilir.

## Sorun giderme

| Sorun | Çözüm |
|---|---|
| "Bu kanala ulaşamıyorum" | Botu kanala **yönetici** olarak eklemedin. |
| "Mesajları sil yetkim yok" | Kanal → Yöneticiler → bot → *Mesajları sil* iznini aç. |
| "Yönetici görünmüyorsun" | Botu sadece o kanalın yöneticileri kullanabilir. |
| Silme yavaş | Normal: flood yememek için gruplar arasında kısa bekleme var. |
| İşlem yarıda kaldı | Aynı linki tekrar gönder; silinenler atlanır, devam eder. |

> ⚠️ **Dikkat:** Silinen mesajlar geri getirilemez. Butona basmadan önce doğru
> kanal ve doğru ana mesaj olduğundan emin ol.
