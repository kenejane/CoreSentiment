import os
import pandas as pd
from io import StringIO
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import gzip

def check_if_already_processed(**context):
    """To check if data for this data interval already exists"""
    data_interval_start = context['data_interval_start']
    hook = PostgresHook(postgres_conn_id="postgres")
    
    # Create processed tracking table if not exists
    hook.run("""
        CREATE TABLE IF NOT EXISTS processed_pageviews_runs (
            data_interval_start TIMESTAMP PRIMARY KEY,
            processed_at TIMESTAMP,
            rows_loaded INTEGER
        )
    """)
    
    # Check if we already processed this data interval
    result = hook.get_first(
        "SELECT data_interval_start FROM processed_pageviews_runs WHERE data_interval_start = %s",
        parameters=(data_interval_start,)
    )
    
    if result:
        print(f"Data for {data_interval_start} already processed. Skipping.")
        return 'skip_processing'
    else:
        print(f"Data for {data_interval_start} not found. Proceeding with processing.")
        return 'process_data'


def skip_processing_function():
    print("Skipping processing - data already exists")
    return "Skipped - already processed"


def execute_sql_file():
    hook = PostgresHook(postgres_conn_id="postgres")
    
    with open('/opt/airflow/dags/CoreSentiment/include/pageviews.sql', 'r') as file:
        sql_commands = file.read()
    
    hook.run(sql_commands)
    print("Successfully executed SQL query from pageviews.sql")


def load_data_function(**context):
    hook = PostgresHook(postgres_conn_id="postgres")
    data_interval_start = context['data_interval_start']
    
    # Double-check idempotency
    existing = hook.get_first(
        "SELECT data_interval_start FROM processed_pageviews_runs WHERE data_interval_start = %s",
        parameters=(data_interval_start,)
    )
    if existing:
        print(f"Data for {data_interval_start} already exists in tracking table. Skipping.")
        return "Already processed - skipped"
    
    # Process the gzipped file and load data
    columns = ['domain_code', 'page_title', 'count_views', 'total_response_size']
    
    # Use the correct file path that matches download location
    file_path = '/opt/airflow/data/pageviews.gz'
   
    try:
        ##with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        df = pd.read_csv(
            #f,
            file_path,
            sep=' ',
            header=None,
            names=columns,
            usecols=[0, 1, 2, 3],
            #compression=None
        )
        
        # Clean data
        df = df[df['count_views'] > 0]
        df = df[df['page_title'].notna()]
        
        # Add timestamp - use data_interval_start
        df['date_hour'] = data_interval_start
        
        # Load to PostgreSQL using COPY
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)
        
        hook.copy_expert(
            sql="""
            COPY pageviews (domain_code, page_title, count_views, total_response_size, date_hour)
            FROM STDIN WITH CSV
            """,
            filename=csv_buffer
        )
        
        # Record successful processing
        hook.run("""
            INSERT INTO processed_pageviews_runs (data_interval_start, processed_at, rows_loaded)
            VALUES (%s, %s, %s)
        """, parameters=(data_interval_start, datetime.now(), len(df)))
        
        print(f"Loaded {len(df)} rows into table for {data_interval_start}")
        return f"Successfully processed {len(df)} rows"
        
    except Exception as e:
        print(f"Error processing file: {e}")
        # Record failure for debugging
        hook.run("""
            INSERT INTO processed_pageviews_runs (data_interval_start, processed_at, rows_loaded)
            VALUES (%s, %s, %s)
            ON CONFLICT (data_interval_start) DO UPDATE SET
                processed_at = EXCLUDED.processed_at,
                rows_loaded = EXCLUDED.rows_loaded
        """, parameters=(data_interval_start, datetime.now(), -1))
        raise


def analyze_top_company(**context):
    hook = PostgresHook(postgres_conn_id="postgres")
    data_interval_start = context['data_interval_start']
    
    # Read SQL from file
    with open('/opt/airflow/dags/CoreSentiment/include/company_views.sql', 'r') as file:
        analysis_query = file.read()
        result = hook.get_first(analysis_query)
    
    if result:
        company, total_views = result
        print(f"ANALYSIS RESULT for {data_interval_start}: {company} has the highest views with {total_views:,} total page views")
        return f"{company} tops with {total_views:,} views"
    else:
        print(f"No data found for the specified companies for {data_interval_start}")
        return "No data found"