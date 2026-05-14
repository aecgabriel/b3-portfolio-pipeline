FROM quay.io/astronomer/astro-runtime:3.2-4

RUN mkdir -p /usr/local/airflow/dbt_data /usr/local/airflow/reports
