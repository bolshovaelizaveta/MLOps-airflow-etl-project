import pandas as pd
import pendulum
import os
import sys
from airflow.decorators import dag, task


PROJECT_PATH = '/home/bolsh/my_mlops_project'
DATA_PATH = os.path.join(PROJECT_PATH, 'data', 'profit_table.csv')
SCRIPT_PATH = os.path.join(PROJECT_PATH, 'scripts')
# Итоговый файл для этого DAG
OUTPUT_PATH = os.path.join(PROJECT_PATH, 'data', 'flags_activity_parallel_simple.csv')
# Временный файл для обмена данными между задачами
TMP_DATA_PATH = '/tmp/bolshova_temp_data.csv'

# Добавляем путь к скриптам
sys.path.append(SCRIPT_PATH)

from transform_script import transform

PRODUCTS = [chr(ord('a') + i) for i in range(10)]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Определение DAG
@dag(
    dag_id='Elizaveta_Bolshova_parallel_simple_fixed',
    default_args=default_args,
    description='A simple and robust parallel DAG using a temporary file.',
    schedule='0 0 5 * *',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=['mlops_hw', 'etl', 'parallel', 'simple'],
)
def simple_parallel_dag():
    
    @task
    def extract():
        """
        Просто читает исходный файл и сохраняет его во временный.
        Возвращает путь к этому файлу.
        """
        df = pd.read_csv(DATA_PATH)
        df.to_csv(TMP_DATA_PATH, index=False)
        print(f"Data extracted and saved to temporary file: {TMP_DATA_PATH}")
        return TMP_DATA_PATH

    @task
    def transform_per_product(product: str, tmp_file_path: str, **context):
        """
        Принимает путь к временному файлу, читает его и считает флаги.
        Затем возвращает DataFrame только с нужными колонками.
        """
        report_date = context['ds']
        
        # Каждая задача читает один и тот же временный файл
        df = pd.read_csv(tmp_file_path)
        
        result_df = transform(df, date=report_date)

        # Возвращаем DataFrame только с id, date и флагом для нашего продукта
        final_product_df = result_df[['id', 'date', f'flag_{product}']]
        return final_product_df

    @task
    def load(processed_dfs: list):
        """
        Собирает результаты от всех 10 задач и объединяет их.
        """
        # Первый DataFrame в списке берем за основу
        final_df = processed_dfs[0]
        
        # Последовательно присоединяем остальные 9
        for i in range(1, len(processed_dfs)):
            # how='outer' на случай, если у каких-то клиентов не будет данных по всем продуктам
            final_df = pd.merge(final_df, processed_dfs[i], on=['id', 'date'], how='outer')

        file_exists = os.path.exists(OUTPUT_PATH)
        final_df.to_csv(OUTPUT_PATH, mode='a', index=False, header=not file_exists)
        print(f"Data loaded to {OUTPUT_PATH}")

    @task
    def cleanup_tmp_file(tmp_file_path: str):
        """
        Удаляет временный файл после завершения работы.
        """
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
            print(f"Temporary file {tmp_file_path} removed.")

    # Определение потока выполнения
    temp_path = extract()
    
    transformed_data_list = transform_per_product.partial(tmp_file_path=temp_path).expand(product=PRODUCTS)
    
    load_task = load(processed_dfs=transformed_data_list)
    
    cleanup_task = cleanup_tmp_file(tmp_file_path=temp_path)

    load_task >> cleanup_task


# Создание экземпляра DAG
simple_parallel_dag_instance = simple_parallel_dag()
