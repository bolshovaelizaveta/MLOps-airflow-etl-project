import pandas as pd
import pendulum
import os
import sys
from airflow.decorators import dag, task


PROJECT_PATH = '/home/bolsh/my_mlops_project'
DATA_PATH = os.path.join(PROJECT_PATH, 'data', 'profit_table.csv')
SCRIPT_PATH = os.path.join(PROJECT_PATH, 'scripts')
OUTPUT_PATH = os.path.join(PROJECT_PATH, 'data', 'flags_activity.csv')

# Пути к скриптам
sys.path.append(SCRIPT_PATH)
from transform_script import transform

# Базовые аргументы DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Определение DAG
@dag(
    dag_id='Elizaveta_Bolshova',
    default_args=default_args,
    description='A simple ETL DAG to process client activity',
    schedule_interval='0 0 5 * *',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=['mlops_hw', 'etl'],
)
def etl_process_dag():
    
    @task()
    def process_data(**context):
        # Получаем дату из контекста
        report_date = context['ds'] # 'ds' это дата в формате 'YYYY-MM-DD'
        print(f"Starting ETL process for date: {report_date}")

        print(f"Reading data from {DATA_PATH}")
        df = pd.read_csv(DATA_PATH)
        
        print(f"Applying transformation for date: {report_date}")
        result_df = transform(df, date=report_date) 
        print("Transformation complete.")
        
        print(f"Loading data to {OUTPUT_PATH}")
        file_exists = os.path.exists(OUTPUT_PATH)
        result_df.to_csv(OUTPUT_PATH, mode='a', index=False, header=not file_exists)
        print("Data loaded successfully.")

    # Создаем поток
    process_data()

# Создаем экземпляр DAG
etl_dag = etl_process_dag()
