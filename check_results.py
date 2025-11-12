#!/usr/bin/env python3
"""
Скрипт для проверки результатов анализа топ-3 тренеров по количеству первых мест
Работает как с локальной базой, так и с базой внутри Docker контейнера
"""

import sqlite3
import pandas as pd
import os
import subprocess
import sys

DB_PATH = 'top_coaches.db'
CONTAINER_DB_PATH = '/opt/airflow/top_coaches.db'


def check_docker_container():
    """Проверка наличия запущенного контейнера scheduler"""
    try:
        result = subprocess.run(
            ['sudo', 'docker', 'ps', '--format', '{{.Names}}'],
            capture_output=True, text=True, check=True
        )
        containers = result.stdout.strip().split('\n')
        scheduler_containers = [c for c in containers if 'scheduler' in c]
        return scheduler_containers[0] if scheduler_containers else None
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def copy_db_from_container(container_name):
    """Копирование базы данных из контейнера на хост"""
    try:
        print(f"Копируем базу данных из контейнера {container_name}...")
        subprocess.run(
            ['sudo', 'docker', 'cp',
             f'{container_name}:{CONTAINER_DB_PATH}',
             DB_PATH],
            check=True
        )
        print("База данных успешно скопирована из контейнера.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при копировании базы данных: {e}")
        return False


def check_database():
    """Проверка результатов в базе данных"""

    # Проверяем наличие локальной базы данных
    if not os.path.exists(DB_PATH):
        print(f"База данных {DB_PATH} не найдена локально.")

        # Пытаемся скопировать из контейнера
        container_name = check_docker_container()
        if container_name:
            print(f"Найден контейнер scheduler: {container_name}")
            if not copy_db_from_container(container_name):
                print("Не удалось скопировать базу данных из контейнера.")
                print(f"Попробуйте вручную: sudo docker cp {container_name}:{CONTAINER_DB_PATH} {DB_PATH}")
                return
        else:
            print("Контейнер scheduler не найден. Убедитесь, что DAG выполнен и контейнеры запущены.")
            return

    try:
        conn = sqlite3.connect(DB_PATH)

        # Проверяем наличие таблицы
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        print("Найденные таблицы в базе данных:")
        for table in tables:
            print(f"  - {table[0]}")

        if ('top_coaches',) not in tables:
            print("\nТаблица 'top_coaches' не найдена!")
            return

        # Извлекаем данные
        query = """
        SELECT 
            coach_name,
            first_places,
            analysis_date
        FROM top_coaches
        ORDER BY first_places DESC
        """

        df = pd.read_sql_query(query, conn)

        print("\nРезультаты анализа топ-3 тренеров по первым местам:")
        print("=" * 70)
        print(df.to_string(index=False))

        # Общая статистика
        total_first_places = df['first_places'].sum()
        print(f"\nОбщее количество первых мест у всех тренеров: {total_first_places}")

        # Лучший тренер
        best_coach = df.iloc[0]
        print(f"\nЛучший тренер: {best_coach['coach_name']} ({best_coach['first_places']} первых мест)")

        conn.close()

    except Exception as e:
        print(f"Ошибка при проверке базы данных: {str(e)}")


def copy_result_files():
    """Копирование файлов результатов из контейнера"""
    container_name = check_docker_container()
    if not container_name:
        print("Контейнер scheduler не найден для копирования файлов.")
        return

    files_to_copy = [
        ('/opt/airflow/top_coaches_report.txt', 'top_coaches_report.txt'),
        ('/opt/airflow/top_coaches_data.csv', 'top_coaches_data.csv')
    ]

    print(f"\nКопируем файлы результатов из контейнера {container_name}...")

    for container_path, local_path in files_to_copy:
        try:
            subprocess.run(
                ['sudo', 'docker', 'cp',
                 f'{container_name}:{container_path}',
                 local_path],
                check=True
            )
            print(f"Скопирован файл: {local_path}")

            if os.path.exists(local_path):
                size = os.path.getsize(local_path)
                print(f"   Размер файла: {size} байт")

        except subprocess.CalledProcessError:
            print(f"Файл {container_path} не найден в контейнере.")

    print("\nДоступные файлы результатов:")
    for _, local_path in files_to_copy:
        if os.path.exists(local_path):
            print(f"  - {local_path}")
        else:
            print(f"  - {local_path} не найден.")


def show_help():
    """Показать справку по использованию скрипта"""
    print("""
СКРИПТ ПРОВЕРКИ РЕЗУЛЬТАТОВ АНАЛИЗА ТРЕНЕРОВ

Использование:
    python3 check_results.py [опция]

Опции:
    (без параметров)  - Проверить результаты в базе данных
    --files           - Скопировать файлы результатов из контейнера
    --help            - Показать эту справку

Примеры:
    python3 check_results.py           # Проверить базу данных
    python3 check_results.py --files   # Скопировать файлы результатов
    python3 check_results.py --help    # Показать справку

Файлы результатов:
    - top_coaches_report.txt    # Текстовый отчет
    - top_coaches_data.csv      # CSV-файл с данными
    - top_coaches.db            # База данных SQLite

Примечание:
    Скрипт автоматически ищет контейнер scheduler и копирует файлы из него.
    Требуются права sudo для работы с Docker.
    """)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == '--files':
            copy_result_files()
        elif sys.argv[1] == '--help':
            show_help()
        else:
            print(f"Неизвестная опция: {sys.argv[1]}")
            print("Используйте --help для справки")
    else:
        check_database()
