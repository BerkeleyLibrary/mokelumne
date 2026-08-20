from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task

from mokelumne.util import pdf_utils


@dag(
    params={
        "source": Param(type="string"),
        "destination": Param(type="string"),
        "language": Param(
            default="",
            type=["null", "string"],
            description="Optional Tesseract language override.",
        ),
    }
)
def pdf_creation():

    @task
    def validate_inputs():
        context = get_current_context()

        source_path = Path(context["params"]["source"])
        destination_path = Path(context["params"]["destination"])

        pdf_utils.validate_source_path(source_path)
        pdf_utils.validate_destination_path(destination_path)
        pdf_utils.validate_source_structure(source_path)

    validate_inputs()

pdf_creation()