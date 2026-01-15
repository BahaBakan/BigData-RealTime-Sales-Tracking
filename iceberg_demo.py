import pyarrow.parquet as pq
import pandas as pd
import os
from pyiceberg.catalog.sql import SqlCatalog
import shutil
import pyarrow as pa

# ---------------- Ayarlar ----------------
WAREHOUSE_PATH = os.path.join(os.getcwd(), "iceberg_warehouse")
CATALOG_DB = "sqlite:///iceberg_catalog.db"

# Temizlik
if os.path.exists(WAREHOUSE_PATH):
    try:
        shutil.rmtree(WAREHOUSE_PATH)
    except:
        pass
os.makedirs(WAREHOUSE_PATH, exist_ok=True)

print("1. Iceberg Katalogu (SqlCatalog) kuruluyor...")
catalog = SqlCatalog("my_local_catalog", **{
    "uri": CATALOG_DB,
    "warehouse": f"file://{WAREHOUSE_PATH}",
})

try:
    catalog.create_namespace("default")
except:
    pass

print("2. Temizlenmis Satis Verisi Okunuyor...")
clean_sales_dir = os.path.join("temiz_veri", "sales_clean_parquet")
try:
    parquet_files = [f for f in os.listdir(clean_sales_dir) if f.endswith('.parquet')]
    df_sales = pd.read_parquet(os.path.join(clean_sales_dir, parquet_files[0]))
except FileNotFoundError:
    print("HATA: Veri dosyasi bulunamadi. Lutfen once dask_etl.py calistirin.")
    exit()

print(f"   - Veri yuklendi. Ilk 5 satir:\n{df_sales.head()}")

print("\n3. Iceberg Tablosu Olusturuluyor: 'default.sales_table'")
try:
    catalog.drop_table("default.sales_table")
except:
    pass

# Pandas -> PyArrow
pa_table = pa.Table.from_pandas(df_sales)

# Zaman sutunu duzeltmesi
new_schema = []
for field in pa_table.schema:
    if field.name == 'Zaman':
        new_schema.append(field.with_type(pa.timestamp('us')))
    else:
        new_schema.append(field)

pa_table = pa_table.cast(pa.schema(new_schema), safe=False)

# Tabloyu olustur
tbl = catalog.create_table(
    "default.sales_table",
    schema=pa_table.schema,
)
tbl.append(pa_table)

print(f"   - Tablo olusturuldu! Guncel Snapshot ID: {tbl.current_snapshot().snapshot_id}")

# ----------------------------------------------------------------
#  Veri Guncelleme ve Zaman Yolculugu
# ----------------------------------------------------------------
print("\n4. SENARYO: Veri Guncellemesi Yapiliyor...")

first_snapshot_id = tbl.current_snapshot().snapshot_id

# Yeni veri
new_data = {
    'Ürün ID': ['P99999'],
    'Müşteri ID': ['C99999'],
    'Zaman': [pd.Timestamp.now()],
    'Bölge': ['Uzay'],
    'Satış Adedi': [1000.0],
    'Fiyat': [9999.99]
}
df_new = pd.DataFrame(new_data)

# Sütun Sıralama
df_new = df_new[pa_table.column_names]

pa_new = pa_table.from_pandas(df_new)

# Veri tipi donusumu
pa_new = pa_new.cast(pa_table.schema, safe=False)

tbl.append(pa_new)

print(f"   - Yeni veri eklendi! (Uzay bolgesinden satis)")
print(f"   - Yeni Snapshot ID: {tbl.current_snapshot().snapshot_id}")

print("\n5. ZAMAN YOLCULUGU (Time Travel) Testi...")

# Guncel veriyi sorgula
print("   -> Guncel Durum (Uzay satisi var mi?):")
scan_result = tbl.scan().to_pandas()
print(f"      Toplam Satir Sayisi: {len(scan_result)}")

print(f"      'Uzay' bolgesi var mi?: {str('Uzay' in scan_result['Bölge'].values)}")

# GECMISE DON
print(f"\n   -> GECMISE DONULUYOR (Snapshot ID: {first_snapshot_id})...")
scan_past = tbl.scan(snapshot_id=first_snapshot_id).to_pandas()
print(f"      Gecmisteki Toplam Satir Sayisi: {len(scan_past)}")
print(f"      'Uzay' bolgesi var mi?: {str('Uzay' in scan_past['Bölge'].values)}")

if len(scan_result) > len(scan_past):
    print("\n[BASARILI] Iceberg sayesinde gecmis versiyona sorgu atabildik.")
else:
    print("\n[HATA] Versiyon farki gorulemedi.")