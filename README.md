# 🚀 Gerçek Zamanlı Satış ve Stok Takip Sistemi (Big Data Pipeline)

Bu proje, modern e-ticaret sistemlerinde karşılaşılan büyük veri yönetimi sorunlarına çözüm üretmek amacıyla geliştirilmiş ölçeklenebilir bir veri boru hattı (pipeline) çalışmasıdır. Proje kapsamında 2 milyon satırlık veri işlenmiş, versiyonlanmış ve bulut ortamına taşınarak görselleştirilmiştir.

## 📋 Proje Özeti

Bu projede Python kullanılarak sentetik olarak üretilen satış ve stok verileri, bellek darboğazı yaşanmadan **Apache Dask** ile işlenmiş, **Apache Iceberg** ile veri bütünlüğü ve versiyon kontrolü (Time Travel) sağlanmış, son olarak **Snowflake** bulut veri ambarına aktarılarak **Power BI** üzerinden analiz edilmiştir.

### 🎯 Temel Amaçlar
* **Ölçeklenebilirlik:** Büyük veri setlerinin (2M+ satır) bellek darboğazı olmadan işlenmesi.
* **Veri Bütünlüğü:** Apache Iceberg kullanılarak ACID transaction ve Time Travel özelliklerinin uygulanması.
* **Bulut Entegrasyonu:** Yerel ortamda işlenen verilerin Snowflake Cloud Data Warehouse'a aktarılması.
* **İş Zekası:** Elde edilen sonuçların karar destek mekanizmaları için görselleştirilmesi.

---

## 🛠️ Kullanılan Teknolojiler (Tech Stack)

Projede aşağıdaki teknoloji yığını kullanılmıştır:

* **Veri Üretimi (Simulation):** Python (Faker, NumPy) - Parquet formatında bölümlenmiş veri üretimi.
* **ETL (Extract, Transform, Load):** Apache Dask - "Lazy Evaluation" ile dağıtık veri işleme ve temizleme.
* **Data Lake (Veri Gölü):** Apache Iceberg - Veri versiyonlama, şema yönetimi ve zaman yolculuğu (Time Travel).
* **Data Warehouse (Veri Ambarı):** Snowflake - Bulut tabanlı veri depolama (Internal Stage & Bulk Load).
* **Görselleştirme (BI):** Power BI - Canlı veri bağlantısı ve dashboard tasarımı.

---

## ⚙️ Sistem Mimarisi

Veri akışı şu adımlardan oluşmaktadır:
1.  **Simülasyon:** Satış ve stok verilerinin üretilmesi (İstanbul ve Ankara ağırlıklı dağılım).
2.  **ETL:** Eksik verilerin (NaN) doldurulması, tip dönüşümleri ve bölgesel ciro hesaplamaları.
3.  **Data Lake:** Verinin Iceberg tablolarına yazılması ve geçmiş versiyon sorgularının test edilmesi.
4.  **Warehouse:** İşlenmiş verinin Snowflake'e aktarılması.
5.  **Dashboard:** Sonuçların raporlanması.

---

## 🚀 Kurulum ve Çalıştırma

Projeyi yerel ortamınızda çalıştırmak için aşağıdaki adımları izleyebilirsiniz:

### 1. Gereksinimler
Proje Python tabanlıdır. Gerekli kütüphaneleri yükleyin:
```bash
pip install -r requirements.txt
