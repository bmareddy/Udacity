#Instructions
#In this exercise, we’ll consolidate repeated code into Operator Plugins
#1 - Move the data quality check logic into a custom operator
#2 - Replace the data quality check PythonOperators with our new custom operator
#3 - Consolidate both the S3 to RedShift functions into a custom operator
#4 - Replace the S3 to RedShift PythonOperators with our new custom operator
#5 - Execute the DAG

import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (
    HasRowsOperator,
    PostgresOperator,
    S3ToRedshiftOperator
)

import sql_statements


#
# TODO: Replace the data quality checks with the HasRowsOperator
#
# def check_greater_than_zero(*args, **kwargs):
#    table = kwargs["params"]["table"]
#    redshift_hook = PostgresHook("redshift")
#    records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
#    if len(records) < 1 or len(records[0]) < 1:
#        raise ValueError(f"Data quality check failed. {table} returned no results")
#    num_records = records[0][0]
#    if num_records < 1:
#        raise ValueError(f"Data quality check failed. {table} contained 0 rows")
#    logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")

def push_to_master_table(*args, **kwargs):
    table = kwargs["params"]["table"]
    redshift_hook = PostgresHook("redshift")
    logging.info("Pushing {} data into master table".format(table))
    redshift_hook.run("INSERT INTO {}_master SELECT * FROM {};".format(table, table))

dag = DAG(
    "lesson3.exercise1",
    start_date=datetime.datetime(2018, 1, 1, 0, 0, 0, 0),
    end_date=datetime.datetime(2018, 12, 1, 0, 0, 0, 0),
    schedule_interval="@monthly",
    max_active_runs=1,
)

create_trips_table = PostgresOperator(
    task_id="create_trips_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL,
)

copy_trips_task = S3ToRedshiftOperator(
    task_id="load_trips_from_s3_to_redshift",
    dag=dag,
    table="trips",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="data-pipelines/divvy/partitioned/{execution_date.year}/{execution_date.month}/divvy_trips.csv",
)

check_trips = HasRowsOperator(
    task_id='check_trips_data',
    dag=dag,
    redshift_conn_id='redshift',
    table='trips',
)

trips_to_master = PythonOperator(
    task_id="push_trips_data_to_master",
    dag=dag,
    python_callable=push_to_master_table,
    provide_context=True,
    params={
        "table": "trips"
    },
)

create_stations_table = PostgresOperator(
    task_id="create_stations_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
)

copy_stations_task = S3ToRedshiftOperator(
    task_id="load_stations_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv",
    table="stations",
)

check_stations = HasRowsOperator(
    task_id='check_stations_data',
    dag=dag,
    redshift_conn_id='redshift',
    table='stations',
)

stations_to_master = PythonOperator(
    task_id="push_stations_data_to_master",
    dag=dag,
    python_callable=push_to_master_table,
    provide_context=True,
    params={
        "table": "stations"
    },
)

create_trips_table >> copy_trips_task
copy_trips_task >> check_trips
check_trips >> trips_to_master
create_stations_table >> copy_stations_task
copy_stations_task >> check_stations
check_stations >> stations_to_master
