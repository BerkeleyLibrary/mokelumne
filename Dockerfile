FROM apache/airflow:3.1.6 AS reqs
USER root

# This Dockerfile relies on ARGs and ENVs defined in the upstream
# Airflow Dockerfile: https://github.com/apache/airflow/blob/main/Dockerfile

# this matches the uid/gid in lap/workflow
ENV AIRFLOW_UID=49003
RUN umask 0002; \
    mkdir -p "${AIRFLOW_USER_HOME_DIR}/artifacts/unittest"

RUN apt-get update -y && apt-get upgrade -y \
    && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/

USER airflow
WORKDIR /home/airflow
COPY requirements.txt .


FROM reqs AS airflow

USER airflow
WORKDIR $AIRFLOW_HOME
COPY dags dags
COPY plugins plugins

# we want to isolate anything that airflow might not run directly
WORKDIR $AIRFLOW_USER_HOME_DIR
COPY test .

RUN pip install --no-cache-dir "apache-airflow==3.1.6" \
    -r requirements.txt

ENV AIRFLOW__CORE__LOAD_EXAMPLES=True
