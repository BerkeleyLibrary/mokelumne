FROM apache/airflow:3.1.6 AS reqs
USER root

# this matches the uid/gid in lap/workflow
ENV AIRFLOW_UID=49003

WORKDIR /opt/airflow

RUN apt-get update -y && apt-get upgrade -y \
    && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/

RUN umask 0002; \
    mkdir -p artifacts

USER airflow

COPY requirements.txt .
COPY dags dags
COPY plugins plugins
COPY test test
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
    -r /opt/airflow/requirements.txt

ENV AIRFLOW__CORE__LOAD_EXAMPLES=True
