# mokelumne

UC Berkeley Library's Airflow installation, libraries, and Dags.

This is a proof of concept repo for Airflow under Docker Compose/Swarm. As is, it should be deployed with caution.

Consult [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) for more information. The compose file in the initial commit reflects the composed file linked from those docs.

## Configuration

Airflow's configuration is propagated by environment variables [defined upstream by Airflow](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html) and that are defined and handled in the base image. You can use a `.env` file or pass them.

Important environment variables for our build/environment:

| Variable | Purpose | Usage |
| -------- | ------- | ----- |
| `AIRFLOW_UID` | Set the `uid` the container runs as. | `AIRFLOW_UID="49003"` |
| `AIRFLOW_VERSION` | Sets the Airflow release version. Used to identify the base Airflow image and to define it as a Python constraint | `AIRFLOW_VERSION="3.1.7"` |
| `AIRFLOW_IMAGE_NAME` | Sets an alternate base image for Airflow, e.g. for `slim` images | `AIRFLOW_IMAGE_NAME="apache/airflow:slim-latest"` |
| `FERNET_KEY` | [Fernet](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html) encryption key used to encrypt Airflow secrets | `FERNET_KEY="somebase64value=` |

Note: The `AIRFLOW_UID` example in `example.env` maps to the reserved `uid` for the `airflow` user in [lap/workflow](https://git.lib.berkeley.edu/lap/workflow/-/wikis/UIDs).
