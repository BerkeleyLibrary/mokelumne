from airflow.sdk import dag, task


@dag(...)
def pdf_creation():

    @task
    def validate_inputs():
        ...

    validate_inputs()

pdf_creation()