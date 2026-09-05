import json
import requests
from datetime import datetime, timedelta
import logging
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from db_utils import S3MaxDateManager

logger = logging.getLogger("airflow.task")


default_args = {
    'owner': 'loader',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='Load_API__earthquake',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='API earthquake to S3',
    tags=['earthquake', 's3', 'airflow']
)

def loading_date(starttime: str, endtime: str, manager: S3MaxDateManager):

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    hook = S3Hook(aws_conn_id='minios3_conn')
    bucket='prod'

    for dt in pd.date_range(starttime, endtime, inclusive="left"):
        load_date = dt.strftime("%Y-%m-%d")
        params = {
            "format": "geojson",
            "starttime": load_date,
            "endtime": (dt + timedelta(days=1)).strftime("%Y-%m-%d")
        }

        logger.info(f"Загрузка данных за {load_date}")

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if len(data["features"]) == 0:
            logger.info(f"Нет событий за {load_date}")
            continue                                    # ← исправление

        filename = f"api/earthquake/events_{load_date}.json"

        hook.load_string(
            string_data=json.dumps(data),
            key=filename,
            bucket_name=bucket,
            replace=True
        )

        manager.update_max_date(load_date)
        logger.info(f"Данные за {load_date} загружены")


def fetch_and_upload():
    table_name = "earthquake"
    init_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    manager = S3MaxDateManager(table_name, init_date=init_date)

    # Получаем последнюю дату из переменной
    starttime = manager.get_max_date()
    endtime = datetime.now().strftime("%Y-%m-%d")

    try:
        loading_date(starttime, endtime, manager)
    except Exception as e:
        logger.error(f"Ошибка запроса или загрузки: {e}", exc_info=True)
        raise


fetch_and_upload_task = PythonOperator(
    task_id='fetch_and_upload',
    python_callable=fetch_and_upload,
    dag=dag
)