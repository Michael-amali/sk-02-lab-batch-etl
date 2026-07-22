FROM apache/airflow:2.9.3-python3.11

# Java is required by PySpark (Spark runs on the JVM)
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Python packages must be installed as the airflow user, never as root
USER airflow

# Must match the Airflow version in the FROM line above so constraints align
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    pyspark==3.5.1 \
    dbt-postgres==1.8.2 \
    apache-airflow-providers-amazon==8.25.0