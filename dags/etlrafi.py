from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

dag_path = os.path.dirname(__file__)

def extract():
    house_path = os.path.join(dag_path, 'house_data_revised.csv')
    review_path = os.path.join(dag_path, 'review_data_revised.csv')

    house_df = pd.read_csv(house_path)
    review_df = pd.read_csv(review_path)

    # Pastikan kolom date dan created_at dalam format datetime
    house_df['date'] = pd.to_datetime(house_df['date'], errors='coerce')
    review_df['created_at'] = pd.to_datetime(review_df['created_at'], errors='coerce')

    house_df.to_csv('/tmp/extracted_house.csv', index=False)
    review_df.to_csv('/tmp/extracted_review.csv', index=False)

def transform():
    house_df = pd.read_csv('/tmp/extracted_house.csv')
    review_df = pd.read_csv('/tmp/extracted_review.csv')

    house_df['date'] = pd.to_datetime(house_df['date'], errors='coerce')
    review_df['created_at'] = pd.to_datetime(review_df['created_at'], errors='coerce')

    # Tambah kolom year dan month untuk OLAP analysis
    house_df['year'] = house_df['date'].dt.year
    house_df['month'] = house_df['date'].dt.month

    review_df['year'] = review_df['created_at'].dt.year
    review_df['month'] = review_df['created_at'].dt.month

    # Gabungkan agregasi review dan house berdasarkan bulan & tahun
    review_agg = review_df.groupby(['year', 'month']).agg({
        'stars': 'mean',
        'thumbs_up': 'sum',
        'thumbs_down': 'sum',
        'reply_count': 'sum',
        'best_score': 'mean'
    }).reset_index()

    house_agg = house_df.groupby(['year', 'month']).agg({
        'price': 'mean',
        'area': 'mean',
        'bedrooms': 'mean',
        'bathrooms': 'mean'
    }).reset_index()

    merged_df = pd.merge(house_agg, review_agg, on=['year', 'month'], how='outer')
    merged_df.to_csv('/tmp/transformed_olap.csv', index=False)

def load():
    df = pd.read_csv('/tmp/transformed_olap.csv')
    csv_path = os.path.join(dag_path, 'OLAP_datarafi.csv')
    df.to_csv(csv_path, index=False)

with DAG(
    dag_id='etl_house_olap',
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['olap', 'house', 'review']
) as dag:

    t1 = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    t2 = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    t3 = PythonOperator(
        task_id='load',
        python_callable=load
    )

    t1 >> t2 >> t3
