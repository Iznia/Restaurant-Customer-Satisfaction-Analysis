'''
=============================================================================
# Restaurant Customer Satisfaction Analysis

Nama  : Iznia Azyati

File python ini dirancang untukmengotomatisasi proses transformasi pemuatan, 
pengambilan, pembersihan, dan pengunggahan data dari CSV, PostgreSQL, dan 
Elasticsearch sebagai bagian dari alur pemrosesan data. 
Setiap tugas dalam DAG melakukan langkah spesifik dalam alur kerja, membantu 
menyederhanakan dan mengotomatiskan alur kerja data.

===============================================================================
'''

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
from elasticsearch import Elasticsearch


def fetch_data_from_database():
    '''
    Fetches data dari PostgreSQL, dengan nama 'table_m3' dan menyimpannya ke CSV.

    Fungsi ini membuat koneksi ke database PostgreSQL yang ditentukan oleh parameter:
    - database: Nama database ('project-m3')
    - username: Nama pengguna untuk otentikasi ('airflow' secara default)
    - password: Kata sandi untuk otentikasi ('airflow' secara default)
    - host: Nama host atau alamat IP server database ('localhost' secara default)

    DataFrame kemudian disimpan ke file CSV dengan nama 'P2M3_Iznia_Azyati_data_raw.csv' 

    Returns:
        None
    '''

    # fetch data
    database = "project-m3"
    username = "airflow"
    password = "airflow"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Gunakan URL ini saat membuat koneksi
    engine = create_engine(postgres_url)
    conn = engine.connect()
    df = pd.read_sql_query("SELECT * FROM table_m3", conn)
    df.to_csv('/opt/airflow/dags/P2M3_Iznia_Azyati_data_raw.csv', sep=',', index=False)
    conn.close()            #####


def preprocessing():
    '''
    Fungsi untuk data cleaning.
    
    Expected Output:
    Fungsi ini membaca file CSV yang sudah di fetched.
    Kemudian melakukan operasi cleaning data termasuk:
    - Menghapus missing values (NaN) menggunakan dropna().
    - Menghapus baris duplikat menggunakan drop_duplicates().
    - Standarisasi nama kolom dengan mengubahnya menjadi huruf kecil, mengganti spasi, 
      tanda hubung, dan titik dengan garis bawah, dan menghapus karakter non-alfanumerik.
    DataFrame yang dibersihkan kemudian disimpan ke file CSV baru bernama "P2M3_Iznia_Azyati_data_clean.csv" 

    Returns:
        None
    '''
    # Pembersihan data
    data = pd.read_csv('/opt/airflow/dags/P2M3_Iznia_Azyati_data_raw.csv')

    # bersihkan data 
    data.dropna(inplace=True)               # Drop missing values
    data.drop_duplicates(inplace=True)      # Drop duplicate rows

    cleaned_columns = []
    for column in data.columns:
        cleaned_column = column.lower().strip().replace(' ', '_').replace('-', '_').replace('.', '')
        cleaned_column = ''.join(e for e in cleaned_column if e.isalnum() or e == '_')
        cleaned_columns.append(cleaned_column)
    data.columns = cleaned_columns

    data.to_csv('/opt/airflow/dags/P2M3_Iznia_Azyati_data_clean.csv', index=False)


def upload_to_elasticsearch():
    '''
    Fungsi untuk upload data ke Elasticsearch.

    Fungsi ini membuat connection ke server Elasticsearch yang terletak di "http://elasticsearch:9200".
    Membaca data dari file CSV yang telah dibersihkan bernama "P2M3_Iznia_Azyati_data_clean.csv".
    Data kemudian diunggah ke indeks Elasticsearch bernama "table_m3".

    Returns:
        None
    '''
    es = Elasticsearch("http://elasticsearch:9200", timeout=240)
    df = pd.read_csv('/opt/airflow/dags/P2M3_Iznia_Azyati_data_clean.csv')

    for i, r in df.iterrows():
        doc = r.to_dict()               
        es.index(index="table_m3", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {es.index(index='table_m3', id=i+1, body=doc)}")


# Define default arguments for the DAG
default_args = {
    'owner': 'Iznia',
    'start_date': datetime(2024, 7, 19, 6, 30)-timedelta(hours=7),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    "P2M3_iznia_azyati_DAG",                         # Project name
    description='Milestone 3 DAG by Iznia Azyati',
    schedule_interval='30 6 * * *',                  # Scheduling every day at 06:30
    default_args=default_args,
    catchup=False,
) as dag:
    # Task 1: Mengambil Data dari PostgreSQL
    fetch_data_task = PythonOperator(
        task_id='fetch_data_from_database',
        python_callable=fetch_data_from_database)
     
    # Task 2: Cleaning Data
    clean_data_task = PythonOperator(
        task_id='data_cleaning',
        python_callable=preprocessing)

    # Task 3: Upload Data ke Elasticsearch
    upload_data_task = PythonOperator(
        task_id='upload_to_elasticsearch',
        python_callable=upload_to_elasticsearch)


    # Alur DAG
    fetch_data_task >> clean_data_task >> upload_data_task