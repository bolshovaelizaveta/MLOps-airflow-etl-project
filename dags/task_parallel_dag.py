import pandas as pd
import pendulum
import os
import sys
from airflow.decorators import dag, task


PROJECT_PATH = '/home/bolsh/my_mlops_project'
DATA_PATH = os.path.join(PROJECT_PATH, 'data', 'profit_table.csv')
SCRIPT_PATH = os.path.join(PROJECT_PATH, 'scripts')
OUTPUT_PATH = os.path.join(PROJECT_PATH, 'data', 'flags_activity_parallel.csv') 
TMP_DATA_PATH = '/tmp/extracted_data.parquet'

sys.path.append(SCRIPT_PATH)
try:
    from transform_script import transform
except ImportError:
    transform = None

PRODUCTS = [chr(ord('a') + i) for i in range(10)]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

@dag(
    dag_id='Elizaveta_Bolshova_parallel_dag',
    default_args=default_args,
    description='Parallel ETL DAG using temporary file and dynamic mapping.',
    schedule_interval='0 0 5 * *',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=['mlops_hw', 'etl', 'parallel'],
)
def parallel_etl_dag():
    
    @task
    def extract(**context):
        # Сохранение исходных данных во временный файл формата Parquet для быстрой работы
        df = pd.read_csv(DATA_PATH)
        df.to_parquet(TMP_DATA_PATH)
        context['ti'].xcom_push(key='tmp_file_path', value=TMP_DATA_PATH)

    @task
    def start_parallel_transform():
        # Пустая задача, чтобы обойти ограничение Airflow
        # на передачу данных в динамически создаваемые задачи
        pass

    @task
    def transform_per_product(product: str, **context):
        # Получение пути к временному файлу из XCom, а не из аргументов
        tmp_path = context['ti'].xcom_pull(key='tmp_file_path', task_ids='extract')
        report_date = context['ds']

        df = pd.read_parquet(tmp_path)
        
        if transform is None:
            raise ImportError("transform_script could not be imported")
        
        result_df = transform(df, date=report_date)

        # Каждая параллельная задача возвращает только свой результат
        final_product_df = result_df[['id', 'date', f'flag_{product}']]
        return final_product_df

    @task
    def load(processed_dfs: list):
        # Последовательное объединение результатов от всех 10 задач в один DataFrame
        final_df = processed_dfs[0]
        for i in range(1, len(processed_dfs)):
            final_df = pd.merge(final_df, processed_dfs[i], on=['id', 'date'], how='outer')

        file_exists = os.path.exists(OUTPUT_PATH)
        final_df.to_csv(OUTPUT_PATH, mode='a', index=False, header=not file_exists)

    @task
    def cleanup_tmp_file(**context):
        # Удаление временного файла после завершения работы
        tmp_path = context['ti'].xcom_pull(key='tmp_file_path', task_ids='extract')
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    # Определение зависимостей между задачами
    extract_task = extract()
    start_task = start_parallel_transform()
    transformed_data_list = transform_per_product.expand(product=PRODUCTS)
    load_task = load(processed_dfs=transformed_data_list)
    cleanup_task = cleanup_tmp_file()

    extract_task >> start_task >> transformed_data_list >> load_task >> cleanup_task

parallel_etl_dag_instance = parallel_etl_dag()
