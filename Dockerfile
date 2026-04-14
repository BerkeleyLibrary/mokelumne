# This Dockerfile relies on ARGs and ENVs defined in the upstream
# Airflow Dockerfile: https://github.com/apache/airflow/blob/main/Dockerfile

ARG AIRFLOW_VERSION="3.2.0"
ARG AIRFLOW_IMAGE_NAME="apache/airflow:${AIRFLOW_VERSION}"

FROM ${AIRFLOW_IMAGE_NAME}
ENV AIRFLOW_VERSION="${AIRFLOW_VERSION}"
ENV AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW_HOME}/mokelumne/dags"

USER root

RUN umask 0002; \
    mkdir -p "${AIRFLOW_USER_HOME_DIR}/artifacts"; \
    chown -R airflow:0 "${AIRFLOW_USER_HOME_DIR}/artifacts"

RUN apt-get update -y && apt-get upgrade -y \
    && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/

USER airflow
WORKDIR $AIRFLOW_HOME

# Install pinned PyPI dependencies first to maximize layer cache hits.
# python-tind-client is installed separately because pip cannot verify
# hashes on git-sourced packages.
COPY --chown=airflow:0 requirements.txt ./
RUN pip install --no-cache-dir --require-hashes -r requirements.txt
RUN pip install --no-cache-dir git+https://github.com/BerkeleyLibrary/python-tind-client.git@0.2.1

# Install the project itself without pulling dependencies.
COPY --chown=airflow:0 pyproject.toml ./
COPY --chown=airflow:0 mokelumne mokelumne
COPY --chown=airflow:0 plugins plugins
COPY --chown=airflow:0 webserver_config.py .
COPY --chown=airflow:0 test test
RUN pip install --no-cache-dir --no-deps .

# Fail the build if any installed package has unsatisfied dependencies.
# This catches conflicts between our pins and the base image's packages.
RUN pip check
