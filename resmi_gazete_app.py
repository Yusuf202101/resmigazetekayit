import streamlit as st
import feedparser
import requests
from bs4 import BeautifulSoup
import pdfplumber
import sqlite3
import io
import re
from datetime import datetime

# ---------------------------------------------------------------------------
# VERİTABANI FONKSİYONLARI
# ---------------------------------------------------------------------------

DB_PATH = "/tmp/resmi_gazete.db"

def init_db():
    """Veritabanını ve tabloları oluşturur (yoksa)."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS pdf_kayitlari (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            haber_basligi   TEXT,
            haber_url       TEXT,
            pdf_url         TEXT UNIQUE,
            pdf_icerik      TEXT,
            sayfa_sayisi    INTEGER,
            kayit_tarihi    TEXT,
            enerji_epdk     INTEGER DEFAULT 0   -- 0=hayır, 1=evet (ilerisi için)
        )
    """)
    conn.commit()
    conn.close()


def pdf_zaten_kayitli(pdf_url: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT 1 FROM pdf_kayitlari WHERE pdf_url = ?", (pdf_url,))
    sonuc = c.fetchone() is not None
    conn.close()
    return sonuc


def pdf_kaydet(haber_basligi, haber_url, pdf_url, pdf_icerik, sayfa_sayisi):
    """PDF içeriğini veritabanına kaydeder. Aynı URL varsa atlar."""
    # Basit enerji/EPDK anahtar kelime tespiti (ilerisi için)
    anahtar_kelimeler = ["epdk", "enerji", "elektrik", "doğalgaz", "doğal gaz",
                         "boru hattı", "petrol", "nükleer", "yenilenebilir",
                         "lisans", "tarife"]
    icerik_lower = pdf_icerik.lower()
    enerji_epdk = int(any(k in icerik_lower for k in anahtar_kelimeler))

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("""
            INSERT INTO pdf_kayitlari
                (haber_basligi, haber_url, pdf_url, pdf_icerik, sayfa_sayisi, kayit_tarihi, enerji_epdk)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            haber_basligi,
            haber_url,
            pdf_url,
            pdf_icerik,
            sayfa_sayisi,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            enerji_epdk,
        ))
        conn.commit()
        return True, enerji_epdk
    except sqlite3.IntegrityError:
        return False, enerji_epdk  # UNIQUE kısıtı: zaten kayıtlı
    finally:
        conn.close()


def veritabani_ozeti():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM pdf_kayitlari")
    toplam = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM pdf_kayitlari WHERE enerji_epdk = 1")
    enerji = c.fetchone()[0]
    conn.close()
    return toplam, enerji


def son_kayitlari_getir(limit=20):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT id, haber_basligi, pdf_url, sayfa_sayisi, kayit_tarihi, enerji_epdk
        FROM pdf_kayitlari
        ORDER BY id DESC
        LIMIT ?
    """, (limit,))
    rows = c.fetchall()
    conn.close()
    return rows


def pdf_icerik_getir(pdf_id: int):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT pdf_icerik FROM pdf_kayitlari WHERE id = ?", (pdf_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else ""


# ---------------------------------------------------------------------------
# PDF OKUMA FONKSİYONU
# ---------------------------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.resmigazete.gov.tr/",
}

def url_getir(url: str, timeout: int = 30) -> requests.Response | None:
    """
    Retry + farklı header kombinasyonları ile URL'yi çeker.
    Başarısız olursa None döner.
    """
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://",  HTTPAdapter(max_retries=retry))

    try:
        resp = session.get(url, headers=HEADERS, timeout=timeout, verify=True)
        resp.raise_for_status()
        return resp
    except Exception:
        pass

    # SSL doğrulamasını devre dışı bırakarak tekrar dene
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        resp = session.get(url, headers=HEADERS, timeout=timeout, verify=False)
        resp.raise_for_status()
        return resp
    except Exception:
        return None


def htm_icerisindeki_pdf_linklerini_bul(htm_url: str) -> list[str]:
    """
    Resmi Gazete .htm sayfasını açar, içindeki .pdf linklerini döner.
    Örnek: https://www.resmigazete.gov.tr/ilanlar/eskiilanlar/2026/04/20260406-5.htm
    """
    resp = url_getir(htm_url)
    if not resp:
        return []
    soup = BeautifulSoup(resp.content, "html.parser")
    pdf_linkleri = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".pdf"):
            # Göreceli URL'yi mutlak yap
            if href.startswith("http"):
                pdf_linkleri.append(href)
            else:
                from urllib.parse import urljoin
                pdf_linkleri.append(urljoin(htm_url, href))
    # Ham HTML'de de ara
    for url in re.findall(r'https?://[^\s\'"<>]+\.pdf', resp.text):
        if url not in pdf_linkleri:
            pdf_linkleri.append(url)
    return pdf_linkleri


def pdf_oku(pdf_url: str) -> tuple[str, int]:
    """
    Verilen URL'den PDF veya HTM indirir, pdfplumber ile metin çıkarır.
    .htm uzantılıysa önce içindeki PDF linkini bulur.
    (metin, sayfa_sayisi) döner. Hata durumunda (hata_mesaji, 0) döner.
    """
    # .htm ise önce PDF linkini çöz
    gercek_pdf_urller = []
    if pdf_url.lower().endswith(".htm") or pdf_url.lower().endswith(".html"):
        gercek_pdf_urller = htm_icerisindeki_pdf_linklerini_bul(pdf_url)
        if not gercek_pdf_urller:
            return f"[HTM sayfasında PDF bulunamadı: {pdf_url}]", 0
    else:
        gercek_pdf_urller = [pdf_url]

    tum_metinler = []
    toplam_sayfa = 0
    for gurl in gercek_pdf_urller:
        resp = url_getir(gurl)
        if not resp:
            tum_metinler.append(f"[İndirilemedi: {gurl}]")
            continue
        try:
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                toplam_sayfa += len(pdf.pages)
                for sayfa in pdf.pages:
                    metin = sayfa.extract_text()
                    if metin:
                        tum_metinler.append(metin)
        except Exception as e:
            tum_metinler.append(f"[PDF parse hatası: {e}]")

    return "\n\n".join(tum_metinler), toplam_sayfa


def pdf_linklerini_bul(soup: BeautifulSoup, ham_html: str = "") -> list[str]:
    """
    BeautifulSoup nesnesinden PDF bağlantılarını çıkarır.
    4 farklı yöntemle arar:
      1. <a href> etiketleri
      2. Tüm etiketlerin tüm attribute'ları (data-url, src, vb.)
      3. Sayfa metninde düz URL olarak geçen linkler
      4. Ham HTML üzerinde regex (JavaScript içi, encoded linkler vb.)
    """
    bulunanlar = set()

    def ekle(url: str):
        url = url.strip().rstrip("'\"\\")
        if not url.startswith("http"):
            return
        u = url.lower()
        # resmigazete.gov.tr altındaki .pdf, .htm, .html linkleri al
        if "resmigazete.gov.tr" in u:
            bulunanlar.add(url)
        elif u.endswith(".pdf"):
            bulunanlar.add(url)

    # 1) <a href>
    for a in soup.find_all("a", href=True):
        ekle(a["href"])

    # 2) Tüm etiketlerin tüm attribute'ları
    for tag in soup.find_all(True):
        for attr_val in tag.attrs.values():
            if isinstance(attr_val, str):
                ekle(attr_val)
            elif isinstance(attr_val, list):
                for v in attr_val:
                    ekle(v)

    # 3) Sayfa düz metnindeki URL'ler
    metin = soup.get_text(" ")
    for url in re.findall(r'https?://\S+', metin):
        ekle(url)

    # 4) Ham HTML üzerinde regex (JS değişkenleri, data attribute'ları vb.)
    kaynak = ham_html or str(soup)
    for url in re.findall(r'https?://[^\s\'"<>]+', kaynak):
        ekle(url)

    return list(bulunanlar)


# ---------------------------------------------------------------------------
# UYGULAMA
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Resmi Gazete Takip",
    page_icon="📰",
    layout="wide",
)

init_db()

st.title("📰 Bloomberg HT – Resmi Gazete Takip & Veritabanı")
st.write(
    "Bloomberg HT RSS akışından Resmi Gazete haberlerini tarar, "
    "bulunan PDF'leri okur ve **SQLite veritabanına** kaydeder."
)

# ---------------------------------------------------------------------------
# Üst bilgi paneli
# ---------------------------------------------------------------------------
toplam_kayit, enerji_kayit = veritabani_ozeti()
col1, col2, col3 = st.columns(3)
col1.metric("🗄️ Toplam PDF Kaydı", toplam_kayit)
col2.metric("⚡ Enerji / EPDK Kaydı", enerji_kayit)
col3.metric("📋 Diğer Kayıtlar", toplam_kayit - enerji_kayit)

st.divider()

# ---------------------------------------------------------------------------
# Ana sekmeler
# ---------------------------------------------------------------------------
sekme1, sekme2, sekme3 = st.tabs(["🔍 Haberleri Tara & Kaydet", "🗄️ Veritabanı", "🔎 İçerik Ara"])

# ── SEKME 1: Tarama ─────────────────────────────────────────────────────────
with sekme1:
    col_a, col_b = st.columns([2, 1])
    with col_a:
        sadece_yeni = st.checkbox(
            "Yalnızca yeni PDF'leri kaydet (daha önce kaydedilenleri atla)",
            value=True,
        )
    with col_b:
        max_pdf = st.number_input(
            "Haber başına max PDF sayısı", min_value=1, max_value=20, value=5
        )

    if st.button("🚀 Haberleri Tara ve PDF'leri Kaydet", type="primary"):

        rss_url = "https://www.bloomberght.com/rss"

        with st.spinner("RSS verisi çekiliyor..."):
            feed = feedparser.parse(rss_url)
            st.info(f"📡 Toplam **{len(feed.entries)}** haber tarandı.")

            resmi_gazete_haberleri = [
                e for e in feed.entries
                if "resmi gazete" in e.title.lower()
                or ("description" in e and "resmi gazete" in e.description.lower())
            ]

        if not resmi_gazete_haberleri:
            st.warning("Şu anki RSS akışında 'Resmi Gazete' haberi bulunamadı.")
        else:
            st.success(f"🎉 {len(resmi_gazete_haberleri)} adet Resmi Gazete haberi bulundu!")

            for i, haber in enumerate(resmi_gazete_haberleri, 1):
                tarih = haber.get("published", "Tarih belirtilmemiş")
                st.markdown(f"### {i}. [{haber.title}]({haber.link})")

                with st.expander(f"📅 {tarih} | PDF'leri Tara ve Kaydet"):
                    try:
                        resp = requests.get(haber.link, headers=HEADERS, timeout=15)
                        ham_html = resp.text
                        soup = BeautifulSoup(resp.content, "html.parser")

                        # İçerik metni (linkler korunarak)
                        paragraflar = soup.find_all("p")
                        for p in paragraflar:
                            for a_tag in p.find_all("a", href=True):
                                if a_tag["href"].startswith("http"):
                                    a_tag.replace_with(
                                        f"[{a_tag.text}]({a_tag['href']})"
                                    )
                        icerik_metni = "\n\n".join(
                            p.text.strip()
                            for p in paragraflar
                            if len(p.text.strip()) > 30
                        )
                        if icerik_metni:
                            st.markdown(icerik_metni)
                        else:
                            st.info("Haber özeti:")
                            st.write(haber.get("description", ""))

                        # PDF linklerini bul (ham HTML dahil 4 yöntemle)
                        pdf_linkleri = pdf_linklerini_bul(soup, ham_html)[:max_pdf]

                        if not pdf_linkleri:
                            st.warning("Bu haberde PDF linki bulunamadı.")
                            tum_linkler = [a["href"] for a in soup.find_all("a", href=True) if a["href"].startswith("http")]
                            if tum_linkler:
                                with st.expander("🔍 Sayfadaki tüm linkler (hata ayıklama)"):
                                    for lnk in tum_linkler[:30]:
                                        st.code(lnk)
                        else:
                            st.markdown(f"**📎 {len(pdf_linkleri)} PDF bulundu – işleniyor...**")
                            ilerleme = st.progress(0)

                            for j, pdf_url in enumerate(pdf_linkleri):
                                ilerleme.progress((j + 1) / len(pdf_linkleri))

                                # Zaten kayıtlı mı?
                                if sadece_yeni and pdf_zaten_kayitli(pdf_url):
                                    st.info(f"⏭️ Zaten kayıtlı, atlandı: `{pdf_url[-60:]}`")
                                    continue

                                with st.spinner(f"PDF okunuyor ({j+1}/{len(pdf_linkleri)})..."):
                                    icerik, sayfa_sayisi = pdf_oku(pdf_url)

                                basarili, enerji_flag = pdf_kaydet(
                                    haber_basligi=haber.title,
                                    haber_url=haber.link,
                                    pdf_url=pdf_url,
                                    pdf_icerik=icerik,
                                    sayfa_sayisi=sayfa_sayisi,
                                )

                                if basarili:
                                    etiket = " ⚡ Enerji/EPDK" if enerji_flag else ""
                                    st.success(
                                        f"✅ Kaydedildi{etiket} | "
                                        f"{sayfa_sayisi} sayfa | "
                                        f"`{pdf_url[-70:]}`"
                                    )
                                else:
                                    st.warning(f"⚠️ Zaten kayıtlıydı: `{pdf_url[-70:]}`")

                            ilerleme.empty()

                    except Exception as e:
                        st.error(f"Haber içeriği çekilemedi: {e}")

                st.divider()

# ── SEKME 2: Veritabanı ─────────────────────────────────────────────────────
with sekme2:
    st.subheader("🗄️ Son Kaydedilen PDF'ler")

    limit = st.slider("Gösterilecek kayıt sayısı", 5, 100, 20)
    kayitlar = son_kayitlari_getir(limit)

    if not kayitlar:
        st.info("Henüz veritabanında kayıt yok. Önce 'Haberleri Tara' sekmesini kullanın.")
    else:
        for row in kayitlar:
            pdf_id, baslık, pdf_url, sayfa, tarih, enerji = row
            renk = "🟢" if enerji else "⚪"
            etiket = " | ⚡ **Enerji/EPDK**" if enerji else ""

            with st.expander(f"{renk} #{pdf_id} – {baslık[:80]}...{etiket}"):
                st.markdown(f"- **PDF URL:** [{pdf_url[-80:]}]({pdf_url})")
                st.markdown(f"- **Sayfa Sayısı:** {sayfa}")
                st.markdown(f"- **Kayıt Tarihi:** {tarih}")

                if st.button(f"📄 İçeriği Göster (ID: {pdf_id})", key=f"icerik_{pdf_id}"):
                    icerik = pdf_icerik_getir(pdf_id)
                    st.text_area("PDF İçeriği", icerik[:3000] + ("..." if len(icerik) > 3000 else ""),
                                 height=300)

# ── SEKME 3: Arama ──────────────────────────────────────────────────────────
with sekme3:
    st.subheader("🔎 Veritabanında İçerik Ara")
    arama_terimi = st.text_input("Arama terimi girin (örn: EPDK, lisans, tarife...)")

    if arama_terimi:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            SELECT id, haber_basligi, pdf_url, sayfa_sayisi, kayit_tarihi, enerji_epdk,
                   SUBSTR(pdf_icerik, MAX(1, INSTR(LOWER(pdf_icerik), LOWER(?)) - 100), 400) AS kesit
            FROM pdf_kayitlari
            WHERE LOWER(pdf_icerik) LIKE LOWER(?)
               OR LOWER(haber_basligi) LIKE LOWER(?)
            ORDER BY id DESC
            LIMIT 50
        """, (arama_terimi, f"%{arama_terimi}%", f"%{arama_terimi}%"))
        sonuclar = c.fetchall()
        conn.close()

        if not sonuclar:
            st.warning(f"'{arama_terimi}' için sonuç bulunamadı.")
        else:
            st.success(f"**{len(sonuclar)}** kayıtta bulundu.")
            for row in sonuclar:
                pdf_id, baslık, pdf_url, sayfa, tarih, enerji, kesit = row
                etiket = " ⚡" if enerji else ""
                with st.expander(f"#{pdf_id}{etiket} – {baslık[:80]}"):
                    st.markdown(f"[PDF Linki]({pdf_url}) | {sayfa} sayfa | {tarih}")
                    if kesit:
                        # Arama terimini vurgula
                        vurgulu = re.sub(
                            f"({re.escape(arama_terimi)})",
                            r"**\1**",
                            kesit,
                            flags=re.IGNORECASE,
                        )
                        st.markdown(f"...{vurgulu}...")
