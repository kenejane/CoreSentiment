# CoreSentiment Data Pipeline - README

## Overview

The **CoreSentiment** Airflow DAG is an automated data pipeline that processes Wikipedia pageview data to analyze which major tech company (Amazon, Apple, Facebook, Google, or Microsoft) receives the highest page views. The pipeline extracts, transforms, and loads data into PostgreSQL, then performs analytical queries to identify trends.

## Business Objective

To build a stock market prediction tool that applies sentiment analysis using the CoreSentiment leveragingd data from wikipedia

## Architecture


### Data Flow

```
Wikimedia Pageviews → Download → PostgreSQL Storage → Analysis → Results
```

### Technologies

- **Apache Airflow**: Workflow orchestration
- **PostgreSQL**: Data storage and analysis
- **Pandas**: Data processing and transformation
- **cURL**: Data extraction from Wikimedia

## Project Structure

```
/opt/airflow/dags/CoreSentiment/
├── CoreSentiment_dag.py          # Main DAG file
└── include/
    ├── pageviews.sql             # Database table schema
    └── company_views.sql         # Analytical query for company comparison
```

## Workflow Tasks


### 1. **create_tmp_dir_if_not_exists**

- **Type**: BashOperator
- **Purpose**: Creates a temporary directory for data processing
- **Command**: `mkdir -p /tmp/wikimedia_pageviews_data_dumps`

### 2. **download_pageviews_file**

- **Type**: BashOperator
- **Purpose**: Downloads Wikipedia pageviews data
- **Features**:
  - Primary: Downloads hourly pageviews using execution date
  - Fallback: Uses known working file if primary fails
  - Stores in persistent location: `/opt/airflow/data/pageviews.gz`

### 3. **create_table**

- **Type**: PythonOperator
- **Purpose**: Creates PostgreSQL table schema
- **Implementation**: Executes SQL from `pageviews.sql`

### 4. **load_data**

- **Type**: PythonOperator
- **Purpose**: Processes and loads data into PostgreSQL
- **Data Processing**:
  - Reads gzipped CSV with space-separated values
  - Cleans data (removes zero views, null pages)
  - Adds execution timestamp
  - Uses efficient PostgreSQL COPY for bulk loading

### 5. **analyze_top_company**

- **Type**: PythonOperator
- **Purpose**: Analytical query to find the company with the highest views
- **Analysis**: Executes SQL from `company_views.sql` to compare page views across five tech companies

## Database Schema

### pageviews Table

```sql
domain_code         -- Wikipedia domain code (e.g., 'en', 'fr')
page_title          -- Page title/name
count_views         -- Number of views for the period
total_response_size -- Total bytes served
date_hour           -- Timestamp of the data collection
```

## Analysis Logic

The pipeline analyses page views for five target companies using pattern matching:

- **Amazon**: Pages containing 'amazon' in title or domain
- **Apple**: Pages containing 'apple' in title or domain  
- **Facebook**: Pages containing 'facebook' in title or domain
- **Google**: Pages containing 'google' in title or domain
- **Microsoft**: Pages containing 'microsoft' in title or domain

## ⚙Configuration
The postgres config is defined in the docker compose file

### DAG Settings

- **Schedule**: Hourly (`@hourly`)
- **Start Date**: October 20, 2025, 14:00
- **Catchup**: Disabled
- **Retries**: 2 attempts with a 5-minute delay

### Connection Requirements

- **PostgreSQL**: Connection ID `postgres` must be configured in Airflow

## Execution

### Manual Trigger

1. Access Airflow UI
2. Navigate to DAGs → CoreSentiment
3. Click "Trigger DAG"
4. Monitor progress in Tree/Graph view

### Automated Execution

- Runs automatically every hour
- Processes the most recent available Wikipedia pageviews data

## Output

### Logs

- Download status and file information
- Data processing statistics (rows loaded)
- Analysis results with company rankings

### Results

- **Primary**: Company with the highest page views
- **Secondary**: Complete ranking of all five companies
- **Format**: Printed to logs and returned as task output

## Error Handling

- **Download Fallback**: Automatic fallback to known working file
- **Data Validation**: Filters invalid records during processing
- **Retry Logic**: Automatic retries with exponential backoff
- **Idempotency**: Safe for re-execution without data duplication

## SQL Files

### pageviews.sql

Creates the main storage table with appropriate indexes for performance.

### company_views.sql

Contains the analytical query that:

1. Maps pages to companies using pattern matching
2. Aggregates total views per company
3. Returns the company with the highest views

## Key Features

- **Robust Data Extraction**: Dual-source download strategy
- **Efficient Processing**: Pandas for transformation, COPY for loading
- **Automated Analysis**: Built-in analytical capabilities
- **Production Ready**: Error handling, logging, and monitoring
- **Scalable Design**: Modular tasks for easy maintenance

## Use Cases

- **Sentiment Analysis**: Find our company perception by what people are saying about the companies
- **Trend Analysis**: Monitor page view patterns over time
- **Data Pipeline Template**: Reusable pattern for similar ETL workflows
