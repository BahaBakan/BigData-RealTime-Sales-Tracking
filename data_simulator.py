import pandas as pd
import numpy as np
from faker import Faker
import pyarrow.parquet as pq
import pyarrow as pa
import os


FAKER_TR = Faker(['tr_TR'])
NUM_ROWS_SALES = 2000000  # 2 Milyon Satış Kaydı
NUM_ROWS_STOCK = 5000     # 5 Bin Stok Kaydı
START_DATE = '2023-01-01'
END_DATE = '2024-11-20'
REGIONS = ['İstanbul', 'Ankara', 'İzmir', 'Bursa', 'Antalya', 'Adana']
DEPOTS = ['Depo A', 'Depo B', 'Depo C']
OUTPUT_DIR = 'simule_veri'


def create_sales_data(num_rows):
    print("Satış verisi hazırlanıyor (biraz sürebilir)...")
    # Ürün ID havuzu 10000 yapıldı
    data = {
        'Ürün ID': [f'P{i:05d}' for i in np.random.randint(1, 10000, num_rows)],
        'Müşteri ID': [f'C{i:06d}' for i in np.random.randint(1, 50000, num_rows)],
        'Zaman': pd.to_datetime(pd.date_range(START_DATE, END_DATE, periods=num_rows)),
        'Bölge': np.random.choice(REGIONS, num_rows, p=[0.3, 0.2, 0.15, 0.1, 0.1, 0.15]), 
        'Satış Adedi': np.random.randint(1, 15, num_rows),
        'Fiyat': np.round(np.random.uniform(50, 5000, num_rows), 2)
    }
    df = pd.DataFrame(data)
    # Buraya kasıtlı boş değer ekledim
    df.loc[df.sample(frac=0.01).index, 'Satış Adedi'] = np.nan 
    return df

def create_stock_data(num_rows):
    print("Stok verisi hazırlanıyor...")
    possible_ids = range(1, 10000)
    
    data = {
        'Ürün ID': [f'P{i:05d}' for i in np.random.choice(possible_ids, num_rows, replace=False)],
        'Depo ID': np.random.choice(DEPOTS, num_rows),
        'Mevcut Stok': np.random.randint(50, 5000, num_rows),
        'Güncelleme Tarihi': pd.to_datetime([FAKER_TR.date_time_between(start_date='-1y', end_date='now') for _ in range(num_rows)])
    }
    df = pd.DataFrame(data)
    
    
    df['Mevcut Stok'] = df['Mevcut Stok'].astype(str) 
    
    # Kasıtlı hatalı veri ekle (%0.5)
    df.loc[df.sample(frac=0.005).index, 'Mevcut Stok'] = 'Hatalı' 
    return df

# Klasörü oluştur
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Veri setlerini oluştur
sales_df = create_sales_data(NUM_ROWS_SALES)
stock_df = create_stock_data(NUM_ROWS_STOCK)

print("Dosyalar kaydediliyor...")

# 1. CSV Kaydet
sales_df.to_csv(os.path.join(OUTPUT_DIR, 'sales_data.csv'), index=False)
stock_df.to_csv(os.path.join(OUTPUT_DIR, 'stock_data.csv'), index=False)

# 2. PARQUET Kaydet
sales_table = pa.Table.from_pandas(sales_df)
stock_table = pa.Table.from_pandas(stock_df) 

# Satış verisini partition yaparak kaydet
pq.write_to_dataset(sales_table, root_path=os.path.join(OUTPUT_DIR, 'sales_parquet'), partition_cols=['Bölge']) 
pq.write_table(stock_table, os.path.join(OUTPUT_DIR, 'stock_data.parquet'))

print(f"İşlem Tamam! Veriler '{OUTPUT_DIR}' klasöründe oluşturuldu.")