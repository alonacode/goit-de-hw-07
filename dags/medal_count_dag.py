"""DAG: підрахунок випадково обраного типу медалей та перевірка свіжості запису."""

import os
import random
import time
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

# Ідентифікатор, який додається до назв DAG і таблиці, щоб вони не дублювалися
MY_NAME = os.getenv("MY_NAME", "alona")
# ID підключення до MySQL, налаштованого в Airflow
MYSQL_CONN_ID = os.getenv("MYSQL_CONN_ID", "goit_mysql_db")
# Схема, в якій створюється таблиця з результатами
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA", "neo_data")

TABLE = f"{TARGET_SCHEMA}.{MY_NAME}_medal_counts"
SOURCE_TABLE = "olympic_dataset.athlete_event_results"
MEDALS = ["Bronze", "Silver", "Gold"]

# Максимально допустимий вік найновішого запису, секунди
MAX_RECORD_AGE = 30


def pick_random_medal():
    """Випадково обирає тип медалі; значення потрапляє в XCom."""
    medal = random.choice(MEDALS)
    print(f"Обрана медаль: {medal}")
    return medal


def choose_branch(ti):
    """Повертає task_id завдання, яке відповідає обраній медалі."""
    medal = ti.xcom_pull(task_ids="pick_medal")
    return f"count_{medal.lower()}"


def delay_execution(params):
    """Затримує виконання наступного завдання."""
    seconds = params["delay_seconds"]
    print(f"Затримка {seconds} с")
    time.sleep(seconds)


with DAG(
    dag_id=f"{MY_NAME}_medal_count",
    description="Підрахунок медалей з розгалуженням, затримкою та сенсором",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    # Паралельний запуск міг би додати свіжий запис і приховати падіння сенсора
    max_active_runs=1,
    params={
        "delay_seconds": Param(
            5,
            type="integer",
            minimum=0,
            description=f"Понад {MAX_RECORD_AGE} с — сенсор завершиться помилкою",
        ),
    },
    tags=[MY_NAME],
) as dag:

    # 1. Створення таблиці
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10) NOT NULL,
                `count` INT NOT NULL,
                created_at DATETIME NOT NULL
            );
        """,
    )

    # 2. Випадковий вибір медалі
    pick_medal = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_random_medal,
    )

    # 3. Розгалуження
    pick_medal_branch = BranchPythonOperator(
        task_id="pick_medal_branch",
        python_callable=choose_branch,
    )

    # 4. Підрахунок записів і збереження результату
    count_tasks = [
        SQLExecuteQueryOperator(
            task_id=f"count_{medal.lower()}",
            conn_id=MYSQL_CONN_ID,
            sql=f"""
                INSERT INTO {TABLE} (medal_type, `count`, created_at)
                SELECT '{medal}', COUNT(*), NOW()
                FROM {SOURCE_TABLE}
                WHERE medal = '{medal}';
            """,
        )
        for medal in MEDALS
    ]

    # 5. Затримка: запускається, якщо одна з трьох гілок виконалась успішно
    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=delay_execution,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # 6. Сенсор: найновіший запис не старший за 30 секунд
    check_latest_record = SqlSensor(
        task_id="check_latest_record",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
            SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= {MAX_RECORD_AGE}
            FROM {TABLE};
        """,
        mode="poke",
        poke_interval=5,
        timeout=15,
    )

    create_table >> pick_medal >> pick_medal_branch >> count_tasks
    count_tasks >> generate_delay >> check_latest_record
