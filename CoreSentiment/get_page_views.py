import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from CoreSentiment.include.functions import check_if_already_processed
from CoreSentiment.include.functions import skip_processing_function
from CoreSentiment.include.functions import execute_sql_file
from CoreSentiment.include.functions import load_data_function
from CoreSentiment.include.functions import analyze_top_company
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 20, 14),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

TMP_LOAD_DIR: str = "/tmp/wikimedia_pageviews_data_dumps"

with DAG(
    dag_id="CoreSentiment",
    default_args=default_args,
    schedule='@hourly',
    catchup=False
):
    create_tmp_dir_if_not_exists = BashOperator(
        task_id="create_tmp_dir_if_not_exists",
        bash_command=f"mkdir -p {TMP_LOAD_DIR}"
    )

    download_pageviews_file = BashOperator(
        task_id='download_pageviews_file',
        bash_command="""
            mkdir -p /opt/airflow/data && \
            echo 'Trying URL with execution date...' && \
            curl -f -L -o /opt/airflow/data/pageviews.gz \
            "https://dumps.wikimedia.org/other/pageviews/{{ data_interval_start.year }}/{{ data_interval_start.strftime('%Y-%m') }}/pageviews-{{ data_interval_start.strftime('%Y%m%d-%H') }}0000.gz" || \
            (echo 'Fallback to known working file...' && \
             curl -f -L -o /opt/airflow/data/pageviews.gz \
             "https://dumps.wikimedia.org/other/pageviews/2024/2024-10/pageviews-20241023-150000.gz") && \
            echo 'Download completed. File info:' && \
            ls -la /opt/airflow/data/pageviews.gz
        """,
    )

    create_table = PythonOperator(
        task_id="create_table",
        python_callable=execute_sql_file
    )

    # Idempotency check task
    check_processing_status = PythonOperator(
        task_id="check_processing_status",
        python_callable=check_if_already_processed
    )

    # Skip processing task
    skip_processing = PythonOperator(
        task_id="skip_processing",
        python_callable=skip_processing_function
        
    )

    # Main processing task
    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data_function,
        retries=2,
        retry_delay=timedelta(minutes=5)
    )

    analyze_companies = PythonOperator(
        task_id="analyze_top_company",
        python_callable=analyze_top_company
    )

    # Set up dependencies with idempotent branching
    create_tmp_dir_if_not_exists >> download_pageviews_file >> create_table >> check_processing_status
    
    # Branch based on idempotency check
    check_processing_status >> [load_data, skip_processing]
    
    # Analysis still runs in both cases
    load_data >> analyze_companies
    skip_processing >> analyze_companies