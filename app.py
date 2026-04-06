import streamlit as st
import feedparser
import requests
import sqlite3
import io
from PyPDF2 import PdfReader

# --- 1. Veri Tabanı Kurulumu ---
def init_db():
    conn = sqlite3.connect('enerji_piyasalari.db')
    c = conn.cursor()
    # Tabloyu oluştur (Eğer yoksa)
    c.execute('''CREATE TABLE IF NOT EXISTS kararlar
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  baslik TEXT,
                  link TEXT,
                  tarih TEXT,
                  icerik TEXT)''')
    conn.commit()
    return conn

# Veri tabanında bu link daha önce kayıtlı mı kontrolü (Aynı PDF'i 2 kez kaydetmemek için)
def link_kayitli_mi(conn, link):
    c = conn.cursor()
    c.execute("SELECT 1 FROM kararlar WHERE link=?", (link,))
    return c.fetchone() is not None

# --- Sayfa Ayarları ---
st.set_page_config(page_title="Enerji Piyasası RG Botu", page_icon="⚡")

st.title("⚡ Enerji Piyasaları - PDF Tarayıcı ve Arşivleyici")
st.write("Bu araç Resmi Gazete PDF'lerinin içine girer, metinleri okur, **Enerji Piyasası** ile ilgili olanları bulup veri tabanına kaydeder.")

# --- Tarama ve Kaydetme İşlemi ---
if st.button("Haberleri Tara ve Veri Tabanını Güncelle", type="primary"):
    
    rss_url = "https://www.bloomberght.com/rss"
    # Enerji piyasasını yakalamak için anahtar kelimeler
    anahtar_kelimeler = ["enerji", "epdk", "elektrik piyasası", "doğal gaz", "petrol piyasası"]
    
    # Veri tabanı bağlantısını aç
    db_conn = init_db()
    db_cursor = db_conn.cursor()

    with st.spinner("Sistem çalışıyor: RSS çekiliyor ve PDF'ler analiz ediliyor..."):
        try:
            feed = feedparser.parse(rss_url)
            st.info(f"📡 Toplam {len(feed.entries)} duyuru RSS'ten çekildi.")

            kaydedilen_sayisi = 0

            for entry in feed.entries:
                title = entry.title.lower()
                description = entry.description.lower() if 'description' in entry else ""
                
                # Sadece Resmi Gazete ile ilgili olanları al
                if "resmi gazete" in title or "resmi gazete" in description:
                    link = entry.link
                    tarih = entry.published if 'published' in entry else 'Tarih yok'
                    
                    # 1. Kontrol: Bu PDF'i zaten tarayıp kaydettik mi?
                    if link_kayitli_mi(db_conn, link):
                        continue # Kayıtlıysa atla, diğerine geç
                    
                    try:
                        # 2. PDF'i indir (Bilgisayara değil, RAM belleğe)
                        response = requests.get(link, timeout=10)
                        pdf_dosyasi = io.BytesIO(response.content)
                        
                        # 3. PDF'i Oku
                        okuyucu = PdfReader(pdf_dosyasi)
                        tam_metin = ""
                        for sayfa in okuyucu.pages:
                            tam_metin += sayfa.extract_text() + "\n"
                        
                        tam_metin_kucuk = tam_metin.lower()
                        
                        # 4. Anahtar kelime taraması (Enerji piyasalarını ilgilendiriyor mu?)
                        if any(kelime in tam_metin_kucuk for kelime in anahtar_kelimeler):
                            
                            # 5. Eşleşme bulunduysa veri tabanına kaydet!
                            db_cursor.execute('''INSERT INTO kararlar (baslik, link, tarih, icerik)
                                                 VALUES (?, ?, ?, ?)''', 
                                              (entry.title, link, tarih, tam_metin))
                            db_conn.commit()
                            kaydedilen_sayisi += 1
                            
                            st.success(f"✅ Enerji Kararı Bulundu ve Kaydedildi: [{entry.title}]({link})")
                            
                    except Exception as e:
                        st.warning(f"Bir PDF okunamadı: {link} - Hata: {e}")
            
            st.write("---")
            if kaydedilen_sayisi > 0:
                st.success(f"🎉 İşlem Tamam! Veri tabanına yeni **{kaydedilen_sayisi}** adet karar eklendi.")
            else:
                st.info("İşlem Tamam. Mevcut akışta enerji piyasalarını ilgilendiren YENİ bir karar bulunamadı.")

        except Exception as e:
            st.error(f"Sisteme bağlanırken kritik hata: {e}")
        
        finally:
            db_conn.close() # İşimiz bitince veri tabanını kapatıyoruz

# --- Veri Tabanını Görüntüleme ---
st.divider()
st.markdown("### 🗄️ Veri Tabanındaki Kayıtlı Enerji Kararları")
if st.button("Kayıtları Getir"):
    conn = sqlite3.connect('enerji_piyasalari.db')
    import pandas as pd
    
    try:
        df = pd.read_sql_query("SELECT id, baslik, tarih, link FROM kararlar", conn)
        if not df.empty:
            st.dataframe(df, use_container_width=True)
        else:
            st.info("Veri tabanı şu an boş.")
    except:
        st.info("Henüz oluşturulmuş bir veri tabanı yok. Önce tarama yapın.")
    finally:
        conn.close()
