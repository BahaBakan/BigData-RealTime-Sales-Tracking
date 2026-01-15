import dask.dataframe as dd
import pandas as pd
import os
import shutil

# Klasör Ayarları
INPUT_DIR = 'simule_veri'
OUTPUT_DIR = 'temiz_veri'

# Eğer temiz_veri klasörü varsa temizle, yoksa oluştur
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)
os.makedirs(OUTPUT_DIR)

print("1. Dask ile veriler okunuyor (Lazy Loading)...")


ddf_sales = dd.read_parquet(os.path.join(INPUT_DIR, 'sales_parquet'), engine='pyarrow')
ddf_stock = dd.read_parquet(os.path.join(INPUT_DIR, 'stock_data.parquet'), engine='pyarrow')

print("   - Satış verisi ve Stok verisi belleğe sanal olarak yüklendi.")

# ---------------------------------------------------------
#  Veriyi Temizle (Clean Data)
# ---------------------------------------------------------
print("2. Veri Temizliği planlanıyor...")


mean_sales = ddf_sales['Satış Adedi'].mean() 
ddf_sales_clean = ddf_sales.fillna({'Satış Adedi': mean_sales})


ddf_stock_clean = ddf_stock.copy()
ddf_stock_clean['Mevcut Stok'] = dd.to_numeric(ddf_stock_clean['Mevcut Stok'], errors='coerce')
ddf_stock_clean['Mevcut Stok'] = ddf_stock_clean['Mevcut Stok'].fillna(0).astype('int32')

# ---------------------------------------------------------
#  Analizler (Aggregations)
# ---------------------------------------------------------
print("3. Analitik hesaplamalar tanımlanıyor...")


region_analysis = ddf_sales_clean.groupby('Bölge')['Fiyat'].sum().rename('Toplam Ciro')


ddf_sales_clean['Zaman'] = dd.to_datetime(ddf_sales_clean['Zaman'])

daily_trend = ddf_sales_clean.set_index('Zaman')['Satış Adedi'].resample('D').sum()

# ---------------------------------------------------------
#  Kaydetme (Compute & Save)
# ---------------------------------------------------------
print("4. İŞLEM BAŞLIYOR: Hesaplamalar yapılıyor ve diske yazılıyor...")

ddf_sales_clean.to_parquet(os.path.join(OUTPUT_DIR, 'sales_clean_parquet'), write_index=False)
ddf_stock_clean.to_parquet(os.path.join(OUTPUT_DIR, 'stock_clean_parquet'), write_index=False)


print("   - Analizler hesaplanıyor...")
region_analysis.compute().to_csv(os.path.join(OUTPUT_DIR, 'analiz_bolgesel_ciro.csv'))
daily_trend.compute().to_csv(os.path.join(OUTPUT_DIR, 'analiz_gunluk_trend.csv'))

print(f"----------------------------------------------------------------")
print(f"BAŞARILI! Tüm temizlenmiş veriler ve analizler '{OUTPUT_DIR}' klasörüne kaydedildi.")
print(f"----------------------------------------------------------------")