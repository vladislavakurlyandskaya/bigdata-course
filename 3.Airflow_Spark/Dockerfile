FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

# default credentials: login - airflow, password - airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR $AIRFLOW_HOME
#USER $AIRFLOW_UID

COPY dags dags
COPY example example
COPY spark_apps spark_apps
COPY spark spark

COPY scripts scripts
RUN chmod +x scripts
