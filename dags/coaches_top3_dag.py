"""
DAG для поиска топ-3 тренеров по количеству первых мест спортсменов
Вариант задания №10

Автор: Студент
Дата: 2025
"""

from datetime import datetime, timedelta
import pandas as pd
import json
import sqlite3
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email

# ----------------------------- Конфигурация DAG -----------------------------

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['test@example.com']
}

dag = DAG(
    'coaches_top3_analysis',
    default_args=default_args,
    description='Поиск топ-3 тренеров по количеству первых мест их спортсменов',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'sports', 'coaches', 'variant_10']
)

# ----------------------------- Пути к файлам -----------------------------

DATA_DIR = '/opt/airflow/dags/data'
DB_PATH = '/opt/airflow/top_coaches.db'

# ----------------------------- Этап Extract -----------------------------

def extract_athletes_data(**context):
    """Извлечение данных о спортсменах из CSV"""
    csv_path = os.path.join(DATA_DIR, 'athletes.csv')
    df = pd.read_csv(csv_path)
    context['task_instance'].xcom_push(key='athletes_data', value=df.to_dict('records'))
    print(f"Извлечено {len(df)} спортсменов")


def extract_coaches_data(**context):
    """Извлечение данных о тренерах из Excel"""
    excel_path = os.path.join(DATA_DIR, 'coaches.xlsx')
    df = pd.read_excel(excel_path)
    context['task_instance'].xcom_push(key='coaches_data', value=df.to_dict('records'))
    print(f"Извлечено {len(df)} тренеров")


def extract_results_data(**context):
    """Извлечение данных о результатах соревнований из JSON"""
    json_path = os.path.join(DATA_DIR, 'results.json')
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    context['task_instance'].xcom_push(key='results_data', value=data)
    print(f"Извлечено {len(data)} записей результатов")


# ----------------------------- Этап Transform -----------------------------

def transform_data(**context):
    """Объединение данных и расчёт топ-3 тренеров по первым местам"""
    athletes_data = context['task_instance'].xcom_pull(key='athletes_data', task_ids='extract_athletes')
    coaches_data = context['task_instance'].xcom_pull(key='coaches_data', task_ids='extract_coaches')
    results_data = context['task_instance'].xcom_pull(key='results_data', task_ids='extract_results')

    df_athletes = pd.DataFrame(athletes_data)
    df_coaches = pd.DataFrame(coaches_data)
    df_results = pd.DataFrame(results_data)

    # Фильтруем только первые места
    winners = df_results[df_results['place'] == 1]

    # Соединяем таблицы
    merged = winners.merge(df_athletes, on='athlete_id').merge(df_coaches, on='coach_id')

    # Считаем количество первых мест на тренера
    top_coaches = (
        merged.groupby('coach_name')
        .size()
        .reset_index(name='first_places')
        .sort_values('first_places', ascending=False)
        .head(3)
    )

    context['task_instance'].xcom_push(key='top_coaches', value=top_coaches.to_dict('records'))
    print("Топ-3 тренеров:")
    print(top_coaches)
    return f"Рассчитано {len(top_coaches)} записей"


# ----------------------------- Этап Load -----------------------------

def load_to_database(**context):
    """Загрузка результатов анализа в SQLite"""
    data = context['task_instance'].xcom_pull(key='top_coaches', task_ids='transform_data')
    df = pd.DataFrame(data)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS top_coaches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            coach_name TEXT,
            first_places INTEGER,
            analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("DELETE FROM top_coaches")
    df.to_sql('top_coaches', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()

    print(f"Данные сохранены в {DB_PATH}")
    return f"Загружено {len(df)} записей в базу данных"


# ----------------------------- Этап Report -----------------------------

def generate_report(**context):
    """Генерация текстового и CSV отчёта"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT coach_name, first_places FROM top_coaches ORDER BY first_places DESC", conn)
    conn.close()

    report = f"""ОТЧЕТ: Топ-3 тренеров по количеству первых мест
===============================================================
Дата анализа: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

"""
    for i, row in df.iterrows():
        report += f"{i+1}. {row['coach_name']} — {row['first_places']} первых мест\n"

    report_file = '/opt/airflow/top_coaches_report.txt'
    csv_file = '/opt/airflow/top_coaches_data.csv'

    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    df.to_csv(csv_file, index=False, encoding='utf-8')

    context['task_instance'].xcom_push(key='report', value=report)
    print(report)
    print(f"Отчёт сохранён: {report_file}")
    print(f"CSV сохранён: {csv_file}")
    return "Отчёт успешно сгенерирован"


# ----------------------------- Этап Email -----------------------------

def send_email_with_results(**context):
    """Отправка email с результатами анализа"""
    report = context['task_instance'].xcom_pull(key='report', task_ids='generate_report')
    html_content = f"""
    <h2>Анализ спортивных результатов завершён успешно</h2>
    <p><strong>DAG:</strong> coaches_top3_analysis</p>
    <p><strong>Дата выполнения:</strong> {context['ds']}</p>
    <p><strong>Топ-3 тренеров по количеству первых мест:</strong></p>
    <pre>{report}</pre>
    <hr>
    <p>Файлы отчёта прикреплены:</p>
    <ul>
        <li>top_coaches_report.txt</li>
        <li>top_coaches_data.csv</li>
    </ul>
    """

    files = ['/opt/airflow/top_coaches_report.txt', '/opt/airflow/top_coaches_data.csv']

    send_email(
        to=['test@example.com'],
        subject='Топ-3 тренеров по первым местам — результаты анализа',
        html_content=html_content,
        files=files
    )
    print("Email успешно отправлен")
    return "Email отправлен"


# ----------------------------- Определение задач -----------------------------

extract_athletes_task = PythonOperator(
    task_id='extract_athletes',
    python_callable=extract_athletes_data,
    dag=dag
)

extract_coaches_task = PythonOperator(
    task_id='extract_coaches',
    python_callable=extract_coaches_data,
    dag=dag
)

extract_results_task = PythonOperator(
    task_id='extract_results',
    python_callable=extract_results_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag
)

email_task = PythonOperator(
    task_id='send_email_with_results',
    python_callable=send_email_with_results,
    dag=dag
)

[extract_athletes_task, extract_coaches_task, extract_results_task] >> transform_task
transform_task >> load_task >> report_task >> email_task
