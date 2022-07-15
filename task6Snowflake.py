from contextlib import closing
from datetime import timedelta, datetime
import os

from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import pandas as pd

load_dotenv()
file_path = os.getenv("6_FILE_PATH")
conn_id = os.getenv("6_SNOWFLAKE_CONNECTION_ID")


def write_csv_to_table():
    df = pd.read_csv(file_path)
    hook_connection = SnowflakeHook(snowflake_conn_id=conn_id)
    engine = hook_connection.get_sqlalchemy_engine()
    connection = engine.connect()
    start = 0
    while start < len(df) // 16000:
        df.loc[start * 16000:(start + 1) * 16000].to_sql('raw_table', if_exists="append", con=engine,
                                                             index=False)
        start += 1
    df.loc[start * 16000:].to_sql('raw_table', if_exists="append", con=engine,
                                  index=False)

    connection.close()
    engine.dispose()


with DAG("SNOWFLAKE", schedule_interval=None, catchup=False, start_date=datetime(2022, 7, 15, 0, 0),
         dagrun_timeout=timedelta(minutes=10)) as dag:
    task_load = PythonOperator(task_id="write_csv_to_table", python_callable=write_csv_to_table)

    task_create = SnowflakeOperator(
        task_id='create_tables_and_streams',
        snowflake_conn_id='snowflake_connection',
        sql="""create or replace TABLE raw_table (
            "_ID" VARCHAR(16777216),
            "IOS_App_Id" NUMBER(38,0),
            "Title" VARCHAR(16777216),
            "Developer_Name" VARCHAR(16777216),
            "Developer_IOS_Id" FLOAT,
            "IOS_Store_Url" VARCHAR(16777216),
            "Seller_Official_Website" VARCHAR(16777216),
            "Age_Rating" VARCHAR(16777216),
            "Total_Average_Rating" FLOAT,
            "Total_Number_of_Ratings" FLOAT,
            "Average_Rating_For_Version" FLOAT,
            "Number_of_Ratings_For_Version" NUMBER(38,0),
            "Original_Release_Date" VARCHAR(16777216),
            "Current_Version_Release_Date" VARCHAR(16777216),
            "Price_USD" FLOAT,
            "Primary_Genre" VARCHAR(16777216),
            "All_Genres" VARCHAR(16777216),
            "Languages" VARCHAR(16777216),
            "Description" VARCHAR(16777216)
        );
        CREATE OR REPLACE TABLE stage_table LIKE raw_table;
        CREATE OR REPLACE TABLE master_table LIKE raw_table;
        CREATE OR REPLACE STREAM raw_stream ON TABLE raw_table;
        CREATE OR REPLACE STREAM stage_stream ON TABLE stage_table;"""
    )

    task_insert_stage = SnowflakeOperator(
        task_id='insert_stage_table',
        snowflake_conn_id='snowflake_connection',
        sql="""insert into stage_table select "_ID",
            "IOS_App_Id",
            "Title",
            "Developer_Name",
            "Developer_IOS_Id",
            "IOS_Store_Url",
            "Seller_Official_Website",
            "Age_Rating",
            "Total_Average_Rating",
            "Total_Number_of_Ratings",
            "Average_Rating_For_Version",
            "Number_of_Ratings_For_Version",
            "Original_Release_Date",
            "Current_Version_Release_Date",
            "Price_USD",
            "Primary_Genre",
            "All_Genres",
            "Languages",
            "Description" from raw_stream""",
    )
    task_insert_master = SnowflakeOperator(
        task_id='insert_master_table',
        snowflake_conn_id='snowflake_connection',
        sql="""insert into master_table select "_ID",
            "IOS_App_Id",
            "Title",
            "Developer_Name",
            "Developer_IOS_Id",
            "IOS_Store_Url",
            "Seller_Official_Website",
            "Age_Rating",
            "Total_Average_Rating",
            "Total_Number_of_Ratings",
            "Average_Rating_For_Version",
            "Number_of_Ratings_For_Version",
            "Original_Release_Date",
            "Current_Version_Release_Date",
            "Price_USD",
            "Primary_Genre",
            "All_Genres",
            "Languages",
            "Description" from stage_stream""",
    )

    task_create >> task_load >> task_insert_stage >> task_insert_master
