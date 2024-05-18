import os, json
import sys
import pandas as pd
import pymysql
import pytz

from google.oauth2 import service_account
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from datetime import date

from airflow.models.variable import Variable


DATA_PATH = "~/airflow/data"

@dag(
    default_args={
        'owner': 'admin',
    },
    dag_id='project_etl_pipeline_dag',
    description='Extract, transform and load data from Google Cloud SQL instance to Google Cloud Bigquery with Airflow',
    schedule_interval='0 0 * * 0',
    start_date=days_ago(1),
    tags=['book', 'ETL'],
)
def project_etl_pipeline_dag():
    start_operator = DummyOperator(task_id='Start')

    mysql_connection = pymysql.connect(host='34.70.165.104', user='root', password='data225', db='book_rating_225',
                                       port=3306)
    os.environ["no_proxy"] = "*"

    @task(task_id='extract_transform_users_task')
    def extract_transform_users():
        query_users = 'SELECT * FROM users'
        df_users = pd.read_sql(query_users, mysql_connection)
        df_users[pd.to_numeric(df_users['user_id'], errors='coerce').notnull()]
        df_users['update_date'] = date.today()
        df_users.to_csv(f"{DATA_PATH}/extract_transform_users.csv", index=False, header=True)

    @task(task_id='extract_transform_books_task')
    def extract_transform_books():
        query_books = 'SELECT * FROM books'
        df_books = pd.read_sql(query_books, mysql_connection)
        df_books[pd.to_numeric(df_books['year_of_publish'], errors='coerce').notnull()]
        df_books['update_date'] = date.today()
        df_books.to_csv(f"{DATA_PATH}/extract_transform_books.csv", index=False, header=True)


    @task(task_id='extract_transform_ratings_task')
    def extract_transform_ratings():
        query_ratings = 'SELECT * FROM ratings'
        df_ratings = pd.read_sql(query_ratings, mysql_connection)
        df_ratings[pd.to_numeric(df_ratings['user_id'], errors='coerce').notnull()]
        df_ratings[pd.to_numeric(df_ratings['book_rating'], errors='coerce').notnull()]
        df_ratings['update_date'] = date.today()
        df_ratings.to_csv(f"{DATA_PATH}/extract_transform_ratings.csv", index=False, header=True)

    key_path = "/Users/liuying/Downloads/group-project-369421-e6aad4f28171.json"
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    @task(task_id='load_users_staging')
    def load_users():
        table_id = "staging_dataset.users"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("user_id", bigquery.enums.SqlTypeNames.INT64),
                bigquery.SchemaField("location", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("age", bigquery.enums.SqlTypeNames.INT64),
                bigquery.SchemaField("update_date", bigquery.enums.SqlTypeNames.STRING),
            ]
        )

        df = pd.read_csv(f"{DATA_PATH}/extract_transform_users.csv")

        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )  # Make an API request.
        job.result()

    @task(task_id='load_books_staging')
    def load_books():
        table_id = "staging_dataset.books"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("ISBN", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("book_title", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("book_author", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("year_of_publish", bigquery.enums.SqlTypeNames.INT64),
                bigquery.SchemaField("publisher", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("Image_URL_S", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("Image_URL_M", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("Image_URL_L", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("update_date", bigquery.enums.SqlTypeNames.STRING),
            ]
        )

        df = pd.read_csv(f"{DATA_PATH}/extract_transform_books.csv")

        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )  # Make an API request.
        job.result()

    @task(task_id='load_ratings_staging')
    def load_ratings():
        table_id = "staging_dataset.ratings"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("user_id", bigquery.enums.SqlTypeNames.INT64),
                bigquery.SchemaField("ISBN", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("book_rating", bigquery.enums.SqlTypeNames.INT64),
                bigquery.SchemaField("update_date", bigquery.enums.SqlTypeNames.STRING),
            ]
        )

        df = pd.read_csv(f"{DATA_PATH}/extract_transform_ratings.csv")

        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )  # Make an API request.
        job.result()

    @task(task_id='load_ratings_fact_table')
    def load_ratings_fact_table():
        query = """
            INSERT INTO book_rating_225.ratings
            SELECT DISTINCT user_id, ISBN, book_rating, update_date AS date_id
            FROM staging_dataset.ratings
        """

        query_job = client.query(query)  # Make an API request.
        query_job.result()

    @task(task_id='load_users_dimension_table')
    def load_users_dimension_table():
        query = """
                INSERT INTO book_rating_225.users
                SELECT DISTINCT user_id, location, age
                FROM staging_dataset.users
            """

        query_job = client.query(query)  # Make an API request.
        query_job.result()

    @task(task_id='load_books_dimension_table')
    def load_books_dimension_table():
        query = """
                    INSERT INTO book_rating_225.books
                    SELECT DISTINCT ISBN, book_title, book_author, year_of_publish, publisher, Image_URL_S, Image_URL_M,Image_URL_L
                    FROM staging_dataset.books
                """

        query_job = client.query(query)  # Make an API request.
        query_job.result()

    @task(task_id='load_time_dimension_table')
    def load_time_dimension_table():
        query = """
                    INSERT INTO book_rating_225.time
                    SELECT DISTINCT update_date AS date_id, CAST(update_date AS DATE) AS update_date, EXTRACT(YEAR FROM CAST(update_date AS DATE)), EXTRACT(MONTH FROM CAST(update_date AS DATE)), EXTRACT(DAY FROM CAST(update_date AS DATE))
                    FROM staging_dataset.ratings
                """

        query_job = client.query(query)  # Make an API request.
        query_job.result()

    extract_transform_users_task = extract_transform_users()
    extract_transform_books_task = extract_transform_books()
    extract_transform_ratings_task = extract_transform_ratings()

    load_ratings_staging = load_ratings()
    load_users_staging = load_users()
    load_books_staging = load_books()

    load_ratings_fact_table = load_ratings_fact_table()
    load_users_dimension_table = load_users_dimension_table()
    load_books_dimension_table = load_books_dimension_table()
    load_time_dimension_table = load_time_dimension_table()

    end_operator = DummyOperator(task_id='End')

    start_operator >> extract_transform_users_task >> load_users_staging >> load_ratings_fact_table
    start_operator >> extract_transform_books_task >> load_books_staging >> load_ratings_fact_table
    start_operator >> extract_transform_ratings_task >> load_ratings_staging >> load_ratings_fact_table

    load_ratings_fact_table >> load_users_dimension_table >> end_operator
    load_ratings_fact_table >> load_books_dimension_table >> end_operator
    load_ratings_fact_table >> load_time_dimension_table >> end_operator


project_etl_pipeline_dag = project_etl_pipeline_dag()