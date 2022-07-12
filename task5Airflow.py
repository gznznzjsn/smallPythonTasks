import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from pymongo import MongoClient


def load_variables(ti):
    load_dotenv()
    db_name = os.getenv("DB_NAME")
    db_host = os.getenv("DB_HOST")
    db_port = int(os.getenv("DB_PORT"))
    source_file_path = os.getenv("SOURCE_FILE_PATH")
    tmp_file_path = os.getenv("TMP_FILE_PATH")

    ti.xcom_push(key="db_name", value=db_name)
    ti.xcom_push(key="db_host", value=db_host)
    ti.xcom_push(key="db_port", value=db_port)
    ti.xcom_push(key="source_file_path", value=source_file_path)
    ti.xcom_push(key="tmp_file_path", value=tmp_file_path)


def prepare_data(ti):
    source_file_path = ti.xcom_pull(key="source_file_path", task_ids="load_variables")
    tmp_file_path = ti.xcom_pull(key="tmp_file_path", task_ids="load_variables")
    print(source_file_path)
    with open(source_file_path) as file:
        data_frame = pd.read_csv(file)
    data_frame.dropna(inplace=True)
    data_frame.fillna("-", inplace=True)
    data_frame.sort_values(by="at", inplace=True)
    data_frame["content"].replace(r"[^\s\w,.!?'\\-]", "", regex=True, inplace=True)
    data_frame.to_csv(tmp_file_path)


def push_data(ti):
    db_name = ti.xcom_pull(key="db_name", task_ids="load_variables")
    db_host = ti.xcom_pull(key="db_host", task_ids="load_variables")
    db_port = ti.xcom_pull(key="db_port", task_ids="load_variables")
    tmp_file_path = ti.xcom_pull(key="tmp_file_path", task_ids="load_variables")
    with open(tmp_file_path) as file:
        data_frame = pd.read_csv(file)
    client = MongoClient(host=db_host, port=db_port)
    db = client[db_name]
    db.reviews.drop()
    db.reviews.insert_many(list(data_frame.T.to_dict().values()))


with DAG("reviews_processing", schedule_interval="@once", start_date=datetime(2021, 1, 1, 1, 1)) as dag:
    task_load = PythonOperator(task_id="load_variables", python_callable=load_variables)
    task_prepare = PythonOperator(task_id="prepare_data", python_callable=prepare_data)
    task_push = PythonOperator(task_id="push_data", python_callable=push_data)

    task_load >> task_prepare >> task_push

