import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

DB_HOST = "localhost"
DB_USER = "username"
DB_PASSWORD = "password"
DB_NAME = "dbname"

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
    )

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

## set checkpointtime
def get_delta_dates():
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=3)
    return start_date, end_date

start_date, end_date = get_delta_dates()

def extract_data():
    conn = get_connection()
    query = f"""
       SELECT
            I.ID AS INTERACTION_ID,
            I.DATE AS INTERACTION_DATE,
            I.SCHEDULE_ID,
            I.CUSTOMER_ID,
            I.STAFF_ID,
            O.ID AS ORDER_ID,
            OI.ID AS ORDER_ITEM_ID,
            OI.TREATMENT_NAME,
            OI.QTY,
            OI.PRICE,
            C.NAME AS CUSTOMER_NAME,
            S.NAME AS DOCTOR_NAME,
            SLI.SLIP_ID,
            SL.STATE AS SLIP_STATE,
            SL.PAID_AT
        FROM INTERACTION I
        LEFT JOIN "ORDER" O
            ON O.SCHEDULE_ID = I.SCHEDULE_ID
        LEFT JOIN ORDER_ITEM OI
            ON OI.ORDER_ID = O.ID
        LEFT JOIN CUSTOMER C
            ON C.ID = I.CUSTOMER_ID
        LEFT JOIN STAFF S
            ON S.ID = I.STAFF_ID
        LEFT JOIN SLIP_ITEM SLI
            ON SLI.ORDER_ITEM_ID = OI.ID
        LEFT JOIN SLIP SL
            ON SL.ID = SLI.SLIP_ID
        WHERE I.DATE BETWEEN '{START_DATE}' AND '{END_DATE}';
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df

df_raw = extract_data()

def transform(df):
    df["payment_status"] = df["slip_state"].apply(
        lambda x: "Sudah Dibayar" if x == "paid" else "Belum Dibayar"
    )

    df["total_amount_per_treatment"] = df["price"] * df["qty"]

    final_df = df[[
        "interaction_id",
        "interaction_date",
        "customer_name",
        "doctor_name",
        "treatment_name",
        "qty",
        "price",
        "total_amount_per_treatment",
        "payment_status",
        "paid_at"
    ]]

    return final_df

df_final = transform(df_raw)

def load_to_dm(df):

    conn = get_connection()
    cur = conn.cursor()
    delete_sql = f"""
        DELETE FROM dm_interaction_treatment_payment
        WHERE interaction_date BETWEEN '{start_date}' AND '{end_date}';
    """
    cur.execute(delete_sql)

    df.to_sql(
        "dm_interaction_treatment_payment",
        engine,
        if_exists="append",
        index=False
    )

    conn.commit()
    cur.close()
    conn.close()


def run_etl():

    df_extract = extract_data()
    df_transformed = transform(df_extract)
    load_to_dm(df_transformed)

run_etl()


