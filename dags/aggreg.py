
from airflow import DAG
from airflow.operators.python_operator import PythonOperator # type: ignore
from datetime import datetime
import pandas as pd # type: ignore
import random




def generate_data_1():
    data_1 = {
        'ID': [i for i in range(1, 101)],
        'Сумма': [random.randint(100, 500) for _ in range(100)],
        'Категория': [random.choice(['Электроника', 'Одежда', 'Продукты']) for _ in range(100)],
    }
    df_1 = pd.DataFrame(data_1)
    df_1.to_csv(f'sample_data_1.csv', index=False, encoding='utf-8')

# Функция для генерации данных и сохранения их в файл CSV 2
def generate_data_2():
    data_2 = {
        'ID': [i for i in range(51, 151)],
        'Количество': [random.randint(1, 20) for _ in range(100)],
        'Категория': [random.choice(['Электроника', 'Косметика', 'Одежда']) for _ in range(100)],
    }
    df_2 = pd.DataFrame(data_2)
    df_2.to_csv(f'sample_data_2.csv', index=False, encoding='utf-8')

# Функция для генерации данных и сохранения их в файл CSV 3
def generate_data_3():
    data_3 = {
        'ID': [i for i in range(26, 126)],
        'Цена': [random.uniform(10.0, 500.0) for _ in range(100)],
        'Категория': [random.choice(['Одежда', 'Косметика', 'Электроника']) for _ in range(100)],
    }
    df_3 = pd.DataFrame(data_3)
    df_3.to_csv(f'sample_data_3.csv', index=False, encoding='utf-8')

# Функция для агрегации данных из всех трех файлов
def aggregate_data():
    df_1 = pd.read_csv(f'sample_data_1.csv', encoding='utf-8')
    df_2 = pd.read_csv(f'sample_data_2.csv', encoding='utf-8')
    df_3 = pd.read_csv(f'sample_data_3.csv', encoding='utf-8')
    
    # Объединение данных по общему полю 'ID'
    merged_df = pd.merge(df_1, df_2, on='ID', how='outer')
    merged_df = pd.merge(merged_df, df_3, on='ID', how='outer')

    # Проведение агрегации
    aggregated_df = merged_df.groupby('Категория').agg({'Сумма': 'sum', 'Количество': 'sum', 'Цена': 'sum'}).reset_index()
    aggregated_df.to_csv(f'aggregated_data.csv', index=False, encoding='utf-8')

# Определение DAG
dag = DAG('generate_data_dag', description='Generate and aggregate sample data',
          schedule_interval='@once', start_date=datetime(2023, 10, 1), catchup=False)

# Определение задач
task_generate_data_1 = PythonOperator(task_id='generate_data_1', python_callable=generate_data_1, dag=dag)
task_generate_data_2 = PythonOperator(task_id='generate_data_2', python_callable=generate_data_2, dag=dag)
task_generate_data_3 = PythonOperator(task_id='generate_data_3', python_callable=generate_data_3, dag=dag)
task_aggregate_data = PythonOperator(task_id='aggregate_data', python_callable=aggregate_data, dag=dag)

# Установка зависимостей между задачами
task_generate_data_1 >> task_generate_data_2 >> task_generate_data_3 >> task_aggregate_data