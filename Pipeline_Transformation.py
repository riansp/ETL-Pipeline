import pandas as pd
import requests
import psycopg2
import gspread
from typing import Dict, List, Any
from sqlalchemy import create_engine
import numpy as np

# CONFIG
DB_CONFIG = {
    'Sales Service': {
        'host': 'sales-db.company.id', 'user': 'etl_sales',
        'password': 'sales_password', 'dbname': 'sales_db'
    },
    'Schedule Service': {
        'host': 'schedule-db.company.id', 'user': 'etl_schedule',
        'password': 'schedule_password', 'dbname': 'schedule_db'
    },
    'Appointment Service': {
        'host': 'appointment-db.company.id', 'user': 'etl_appointment',
        'password': 'appointment_password', 'dbname': 'appointment_db'
    },
    'Medical Record Service': {
        'host': 'mrecord-db.company.id', 'user': 'etl_mrecord',
        'password': 'mrecord_password', 'dbname': 'mrecord_db'
    },
    'Manufacturing Service': {
        'host': 'mfg-db.company.id', 'user': 'etl_mfg',
        'password': 'mfg_password', 'dbname': 'mfg_db'
    }
}

API_CONFIG = {
    'Insurance Eligibility API': {
        'url': 'https://api.insurance.com/eligibility',
        'api_key': 'XYZ123'
    },
    'Logistics Tracking API': {
        'url': 'https://api.logistics.com/tracking',
        'api_key': 'ABC456'
    }
}

GSHEET_CONFIG = {
    'Manual Adjustments Sheet': {
        'spreadsheet_id': '1ABC-XYZ...',
        'worksheet_name': 'Adjustments',
        'service_account_file': '/path/to/sa.json'
    }
}

# DB EXTRACT
def connect_to_postgres(config: Dict) -> Any:
    return psycopg2.connect(**config)
    
def extract_from_postgres(service_name: str, table_name: str) -> pd.DataFrame:
    config = DB_CONFIG.get(service_name)
    if not config:
        raise ValueError(f"Config {service_name} tidak ditemukan")
    query = f"SELECT * FROM {table_name}"
    try:
        engine = create_engine(
            f"postgresql://{config['user']}:{config['password']}@{config['host']}/{config['dbname']}")
        return pd.read_sql(query, engine)
    except Exception as e:
        print(f"Error extract {table_name}: {e}")
        return pd.DataFrame()

# API EXTRACT
def extract_insurance_eligibility(customer_name: str) -> Dict:
    """Mock call — real system akan call API provider"""
    cfg = API_CONFIG['Insurance Eligibility API']
    try:
        resp = requests.get(
            cfg['url'],
            headers={'Authorization': f"Bearer {cfg['api_key']}"},
            params={'customer_name': customer_name})
        if resp.status_code == 200:
            return resp.json()
    except:
        pass
    return {"insurance_status": "unknown", "provider": None}

def extract_logistics_tracking(invoice_id: int) -> Dict:
    """Mock call — real system akan call logistic provider"""
    cfg = API_CONFIG['Logistics Tracking API']
    try:
        resp = requests.get(
            cfg['url'],
            headers={'Authorization': f"Bearer {cfg['api_key']}"},
            params={'invoice_id': invoice_id})
        if resp.status_code == 200:
            return resp.json()
    except:
        pass
    return {"tracking_status": "N/A"}

# GSHEET Extract
def extract_from_google_sheet(sheet_name: str) -> pd.DataFrame:
    cfg = GSHEET_CONFIG.get(sheet_name)
    if not cfg:
        raise ValueError(f"Config gsheet {sheet_name} tidak ditemukan")
    try:
        gc = gspread.service_account(filename=cfg['service_account_file'])
        ws = gc.open_by_key(cfg['spreadsheet_id']).worksheet(cfg['worksheet_name'])
        return pd.DataFrame(ws.get_all_records())
    except Exception as e:
        print(f"GSheet error: {e}")
        return pd.DataFrame()

#  SKU normalization
def normalize_sku(sku: str) -> str:
    if sku is None:
        return None
    return sku.strip().upper().replace(" ", "-")

# EXTRACTION
def extraction() -> Dict[str, pd.DataFrame]:
    return {
        'leads': extract_from_postgres('Sales Service', 'sales_leads'),
        'appointments': extract_from_postgres('Appointment Service', 'appointments'),
        'slips': extract_from_postgres('Appointment Service', 'slips'),
        'slip_treatments': extract_from_postgres('Appointment Service', 'slip_treatment'),
        'medical_records': extract_from_postgres('Medical Record Service', 'medical_records'),
        'manufacturing_orders': extract_from_postgres('Manufacturing Service', 'manufacturing_orders'),
        'manual_adjustments': extract_from_google_sheet('Manual Adjustments Sheet')}

# TRANSFORMATION
def transform_int(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    df = (
        data['leads']
        .merge(data['appointments'], on='lead_id', how='left')
        .merge(data['slips'], on='appointment_id', how='left')
        .merge(data['slip_treatments'], on='slip_id', how='left'))
    for col in df.columns:
        if "date" in col.lower() or "created" in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')
    df.rename(columns={
        'slip_id': 'invoice_id',
        'total_cost': 'invoice_total_cost',
        'payment_method': 'invoice_payment_method',
        'created_at': 'invoice_created_at'
    }, inplace=True, errors='ignore')
    return df

def transform_ext(df_main: pd.DataFrame, df_mfg: pd.DataFrame):
    df_main['insurance_status'] = df_main['customer_name'].apply(
        lambda x: extract_insurance_eligibility(x).get("insurance_status"))
    df_main['insurance_provider'] = df_main['customer_name'].apply(
        lambda x: extract_insurance_eligibility(x).get("provider"))
    df_main['logistics_status'] = df_main['invoice_id'].apply(
        lambda x: extract_logistics_tracking(x).get("tracking_status")
        if pd.notna(x) else "Not Applicable")
    df_mfg['sku_normalized'] = df_mfg['sku'].apply(normalize_sku)
    return df_main, df_mfg

# LOAD
def load(df_fact: pd.DataFrame, df_mfg_dim: pd.DataFrame):
    engine = create_engine("postgresql://user:pass@dwh-db.company.id/dwh_db")
    df_fact.to_sql("fact_appointment_flow", engine, if_exists='append', index=False)
    df_mfg_dim.to_sql("dim_manufacturing_sku", engine, if_exists='replace', index=False)

def etl_pipeline_full_run():
    data = extraction()
    df_main = transform_int(data)
    df_main_final, df_mfg_final = transform_ext(
        df_main.copy(),
        data['manufacturing_orders'].copy())
    load(df_main_final, df_mfg_final)
    
etl_pipeline_full_run()
