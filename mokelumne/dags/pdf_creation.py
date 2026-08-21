"""DAG for creating searchable PDFs from directories of source images."""

from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task

from mokelumne.util import pdf_utils


@dag(
    description="Creates searchable PDFs from directories of source images",
    schedule=None,
    catchup=False,
    params={
        "source": Param(
            type="string",
            title="Source directory",
            description="Directory containing the document subdirectories to process.",
        ),
        "destination": Param(
            type="string",
            title="Destination directory",
            description="Directory where the generated PDFs will be saved.",
        ),
        "language": Param(
            default="",
            type=["null", "string"],
            title="OCR Language",
            description="Optional Tesseract language code to use instead of automatic language selection.",
        ),
    },
)
def pdf_creation():
    """Create searchable PDFs from document directories."""

    @task
    def validate_inputs():
        """Validate source, destination, and source directory structure."""
        context = get_current_context()

        source_path = Path(context["params"]["source"])
        destination_path = Path(context["params"]["destination"])

        pdf_utils.validate_source_path(source_path)
        pdf_utils.validate_destination_path(destination_path)
        pdf_utils.validate_source_structure(source_path)

    validate_inputs()


pdf_creation()
