# This Dockerfile relies on ARGs and ENVs defined in the upstream
# Airflow Dockerfile: https://github.com/apache/airflow/blob/main/Dockerfile

ARG AIRFLOW_VERSION="3.1.7"
ARG AIRFLOW_IMAGE_NAME="apache/airflow:${AIRFLOW_VERSION}"

FROM ${AIRFLOW_IMAGE_NAME} AS reqs
ENV AIRFLOW_VERSION="${AIRFLOW_VERSION}"

USER root

RUN umask 0002; \
    mkdir -p "${AIRFLOW_USER_HOME_DIR}/artifacts"; \
    chown -R airflow:0 "${AIRFLOW_USER_HOME_DIR}/artifacts"

RUN apt-get update -y && apt-get upgrade -y \
    && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/

USER airflow
WORKDIR /home/airflow
COPY --chown=airflow:0 requirements.txt .


FROM reqs AS airflow
ENV AIRFLOW_VERSION="${AIRFLOW_VERSION}"

USER airflow
WORKDIR $AIRFLOW_HOME
COPY --chown=airflow:0 dags dags
COPY --chown=airflow:0 plugins plugins

# we want to isolate anything that airflow might not run directly
WORKDIR $AIRFLOW_USER_HOME_DIR
COPY --chown=airflow:0 test test

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
    -r requirements.txt

ENV AIRFLOW__CORE__LOAD_EXAMPLES=True
