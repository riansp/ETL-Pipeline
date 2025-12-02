📦 ETL Pipeline for Microservices + External API + Google Sheet

Pipeline ini melakukan proses Extract → Transform → Load (ETL) dari beberapa sumber data:

Microservices (PostgreSQL per domain)

External APIs (Insurance Eligibility, Logistics Tracking)

Google Sheets (manual adjustments)

Manufacturing SKU normalization

Pipeline menyatukan seluruh data menjadi fact table dan SKU dimension table dalam Data Warehouse.

🚀 Fitur Utama
1. Extract

Mengambil data dari berbagai sumber:

✅ PostgreSQL (per-domain Microservice DB)

Sales Service → sales_leads

Appointment Service → appointments, slips, slip_treatment

Medical Record Service → medical_records

Manufacturing Service → manufacturing_orders

✅ External API

Insurance Eligibility API

Logistics Tracking API

✅ Google Sheets

Manual Adjustments Sheet

2. Transform

Transformasi dilakukan dalam dua tahap:

a. transform_int() (internal join)

Menggabungkan:

leads → appointments → slips → slip_treatment

Normalisasi tipe waktu (datetime)

Mapping slip → invoice

Menyiapkan struktur awal fact table

b. transform_ext() (external enrichment)

Memperkaya data dengan:

Status eligibility insurance

Provider insurance

Logistics status shipment invoice

Normalize Manufacturing SKU (uppercase + replace spaces)

3. Load

Memasukkan hasil transformasi ke Data Warehouse:

fact_appointment_flow

dim_manufacturing_sku

Menggunakan SQLAlchemy (to_sql()).

📁 Struktur Pipeline
ETL Pipeline
├── extraction()
│   ├── extract_from_postgres()
│   ├── extract_insurance_eligibility()
│   ├── extract_logistics_tracking()
│   └── extract_from_google_sheet()
│
├── transform_int()
├── transform_ext()
│
└── load()

⚙️ Konfigurasi
Database Config

Diatur dalam dictionary DB_CONFIG:

DB_CONFIG = {
    'Sales Service': { ... },
    'Schedule Service': { ... },
    'Appointment Service': { ... },
    ...
}

API Config
API_CONFIG = {
    'Insurance Eligibility API': { ... },
    'Logistics Tracking API': { ... }
}

Google Sheet Config
GSHEET_CONFIG = {
    'Manual Adjustments Sheet': {
        'spreadsheet_id': '...',
        'worksheet_name': 'Adjustments',
        'service_account_file': '/path/to/sa.json'
    }
}

🔧 Cara Kerja Pipeline
1️⃣ Extraction
data = extraction()


Output berupa dictionary berisi seluruh DataFrame internal + external.

2️⃣ Internal Transform
df_main = transform_int(data)


Hasil: DataFrame ter-join lengkap (lead → appointment → slip → treatment).

3️⃣ External Enrichment
df_main_final, df_mfg_final = transform_ext(
    df_main.copy(),
    data['manufacturing_orders'].copy())

4️⃣ Load to Data Warehouse
load(df_main_final, df_mfg_final)

▶️ Menjalankan Pipeline

Pipeline full run:

etl_pipeline_full_run()


Pipeline otomatis melakukan:

Extract semua microservices, API & Google Sheet

Transform internal

Transform external (API enrichment + SKU normalization)

Load ke Data Warehouse
