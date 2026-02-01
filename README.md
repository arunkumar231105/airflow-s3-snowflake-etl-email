# Airflow ETL Pipeline: S3 to Snowflake with Email Notification

This project demonstrates a complete end to end data engineering pipeline using:

- AWS S3 as data source
- Apache Airflow for orchestration
- Snowflake as data warehouse
- Gmail SMTP for email notification

## Pipeline Flow

S3 File Available → Create Table in Snowflake → Load CSV into Snowflake → Send Email Notification

## Tech Stack

- Apache Airflow (Local Executor on EC2)
- AWS S3
- Snowflake
- Python
- Gmail SMTP

## DAG Tasks

1. S3KeySensor waits for file in S3
2. SnowflakeOperator creates table
3. SnowflakeOperator loads data using COPY INTO
4. EmailOperator sends notification email

## Airflow Configuration

SMTP configured using airflow.cfg instead of Airflow connections.

```
[smtp]
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_user = your_email
smtp_password = app_password
smtp_port = 587
smtp_mail_from = your_email
```

## How to Run

1. Start Airflow scheduler and webserver
2. Place CSV file in S3 bucket
3. Trigger DAG
4. Data loads into Snowflake
5. Email notification sent

