from airflow import DAG
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from os import getenv
from sqlalchemy import create_engine
from datetime import datetime
from sqlalchemy.sql.ddl import CreateSchema
import yadisk

DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False}
DEFAULT_POSTGRES_CONN_ID = "postgres_default"
AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')

DAG_ID = "SIMPLE"
schedule = "@hourly"

y = yadisk.YaDisk(token="AQAAAAAwjC82AAfpNcYIk1DAeEtrjuOYolwmPCY")

def download_data(table_name):
    path_yandex = "/BigDataCourse/" + str(table_name) + ".csv"
    new_table = pd.read_csv(y.get_download_link(path_yandex))
    return new_table

def create_schema(conn_id, schemaName):
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
    jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
               f"{conn_object.host}:{conn_object.port}/postgres"
    engine = create_engine(jdbc_url)

    if not engine.dialect.has_schema(engine, schemaName):
        engine.execute(CreateSchema(schemaName))

def load_csv_pandas(table_name: str, schema: str = "raw", conn_id: str = None) -> None:
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
    jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
               f"{conn_object.host}:{conn_object.port}/postgres"
    df = download_data(table_name)
    engine = create_engine(jdbc_url)
    df.to_sql(table_name, engine, schema=schema, if_exists="replace")

def datamart_pandas(table_name: str, schema: str = "datamart", conn_id: str = None) -> None:
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
    jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
               f"{conn_object.host}:{conn_object.port}/{conn_object.schema}"
    engine = create_engine(jdbc_url)

    query = open(f"{AIRFLOW_HOME}/sql/amount_datamart.sql", 'r')
    df = pd.read_sql_query(query.read(), engine)
    df.to_sql(table_name, engine, schema=schema, if_exists="append")


with DAG(dag_id=DAG_ID,
         description='Dag to transfer data from csv to postgres [version 1.0]',
         schedule_interval=schedule,
         default_args=DAG_DEFAULT_ARGS,
         is_paused_upon_creation=True,
         max_active_runs=1,
         catchup=False
         ) as dag:
    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)

    payments_table_name = "payments"
    datamart_table = "payments_totals"


    load_payments_raw_task = PythonOperator(dag=dag,
                                            task_id=f"{DAG_ID}.RAW.{payments_table_name}",
                                            python_callable=load_csv_pandas,
                                            op_kwargs={
                                                "table_name": payments_table_name,
                                                "schema": "raw",
                                                "conn_id": "raw_postgres"
                                            }
                                            )


    payments_totals_datamart_task = PythonOperator(dag=dag,
                                                   task_id=f"{DAG_ID}.DATAMART.{datamart_table}",
                                                   python_callable=datamart_pandas,
                                                   op_kwargs={
                                                       "table_name": datamart_table,
                                                       "schema": "datamart",
                                                       "conn_id": "datamart_postgres"
                                                   }
                                                   )

    create_schema_raw = PythonOperator(dag=dag,
                                       task_id=f"{DAG_ID}.RAW.CREATE_SCHEMA",
                                       python_callable=create_schema,
                                       op_kwargs={
                                           "conn_id": "raw_postgres",
                                           "schemaName": "raw"
                                       }
                                       )

    create_schema_datamart = PythonOperator(dag=dag,
                                            task_id=f"{DAG_ID}.DATAMART.CREATE_SCHEMA",
                                            python_callable=create_schema,
                                            op_kwargs={
                                                "conn_id": "datamart_postgres",
                                                "schemaName": "datamart"
                                            }
                                            )

    start_task >> create_schema_raw >> create_schema_datamart >> [load_payments_raw_task] >> payments_totals_datamart_task >> end_task
