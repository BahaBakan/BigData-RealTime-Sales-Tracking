import snowflake.connector
import os

# =================================================================
# 
# =================================================================
SNOWFLAKE_CONFIG = {
    'user': 'BAHABKN',  
    'password': 'Mekanikas1234.',      
    'account': 'KCQQDCQ-VH91417',    
    'warehouse': 'COMPUTE_WH',               
    'database': 'BIGDATA_ODEV_DB',           
    'schema': 'PUBLIC'
}
# =================================================================

def upload_to_snowflake():
    print("1. Snowflake'e baglaniliyor...")
    
    try:
        
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_CONFIG['user'],
            password=SNOWFLAKE_CONFIG['password'],
            account=SNOWFLAKE_CONFIG['account'],
            warehouse=SNOWFLAKE_CONFIG['warehouse']
        )
        cur = conn.cursor()
        print("   [BASARILI] Baglanti kuruldu.")

        print("2. Veritabani ve Tablo Kurulumu...")
        # Veritabanını oluştur
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_CONFIG['database']}")
        cur.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        cur.execute("USE SCHEMA PUBLIC")

        # Rapor Tablosunu Oluştur (Bölgesel Ciro Analizi)
        cur.execute("""
        CREATE OR REPLACE TABLE BOLGESEL_CIRO (
            BOLGE STRING,
            TOPLAM_CIRO FLOAT
        )
        """)
        print("   - BOLGESEL_CIRO tablosu olusturuldu.")

        print("3. Veri Yukleme (CSV -> Snowflake)...")
        
        
        # Dosya yolu: temiz_veri/analiz_bolgesel_ciro.csv
        csv_path = os.path.abspath(os.path.join("temiz_veri", "analiz_bolgesel_ciro.csv"))
        
        # Windows ters slash (\) sorununu düzelt
        csv_path = csv_path.replace("\\", "/")

        if not os.path.exists(csv_path):
            print(f"   [HATA] Dosya bulunamadi: {csv_path}")
            return

        print(f"   - Dosya staging alanina gonderiliyor: {csv_path}")
        
       
        try:
            put_query = f"PUT file://{csv_path} @%BOLGESEL_CIRO"
            cur.execute(put_query)
            
          
            copy_query = """
            COPY INTO BOLGESEL_CIRO 
            FROM @%BOLGESEL_CIRO 
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            """
            cur.execute(copy_query)
            print("   [BASARILI] Veri Snowflake tablosuna islendi!")
            
        except Exception as e:
            print(f"   [HATA] Yukleme sirasinda sorun: {e}")

        # Test Sorgusu
        print("\n4. SONUC KONTROLU (Snowflake'ten veri cekiliyor):")
        cur.execute("SELECT * FROM BOLGESEL_CIRO")
        rows = cur.fetchall()
        
        print("-" * 30)
        print(f"{'BOLGE':<15} | {'TOPLAM CIRO':<15}")
        print("-" * 30)
        for row in rows:
            print(f"{row[0]:<15} | {row[1]:<15}")
        print("-" * 30)

    except Exception as e:
        print(f"\n[GENEL HATA] Baglanti veya islem hatasi: {e}")
        print("Lutfen kullanici adi, sifre ve account bilgilerini kontrol et.")

    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    upload_to_snowflake()