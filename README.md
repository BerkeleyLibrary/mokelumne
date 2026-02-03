# mokelumne

UC Berkeley Library's Airflow installation, libraries, and Dags.

This is a proof of concept repo for Airflow under Docker Compose/Swarm. As is, it should be deployed with caution.

Consult [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) for more information. The compose file in the initial commit reflects the composed file linked from those docs.

The UID set in `.env` maps to the reserved UID for the `airflow` user in [lap/workflow](https://git.lib.berkeley.edu/lap/workflow/-/wikis/UIDs).
