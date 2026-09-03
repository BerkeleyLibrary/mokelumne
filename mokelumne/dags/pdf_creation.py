"""DAG for creating searchable PDFs from directories of source images."""

import logging
from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from mokelumne.util import pdf_utils, storage


logger = logging.getLogger(__name__)


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

    @task
    def discover_documents():
        """Discover document directories and build work items."""
        context = get_current_context()
        source_path = Path(context["params"]["source"])

        return pdf_utils.discover_documents(source_path)

    @task
    def process_document(document: pdf_utils.DocumentWorkItem):
        """Process a document directory into a searchable PDF."""

        context = get_current_context()
        destination_path = Path(context["params"]["destination"])
        run_id = context["run_id"]

        # 1 - Check if output PDF already exists (skip if it does)
        if pdf_utils.output_exists(destination_path, document["output"]):
            raise AirflowSkipException(
                f"Output PDF already exists: {destination_path / document['output']}"
            )

        # 2 - Prepare workspace!
        run_path = storage.run_dir(run_id)
        workspace_path = pdf_utils.prepare_workspace(
            run_path,
            Path(document["source"]).name,
        )

        # Log the workspace path for now; later stages will use it directly.
        logger.info("Prepared document workspace: %s", workspace_path)

        # 3 - Determine language (coming soon to a theater near you!)
        # 4 - Prepare images (size/convert as necessary)

    validation = validate_inputs()
    documents = discover_documents()
    processed_documents = process_document.expand(document=documents)

    validation >> documents >> processed_documents


pdf_creation()
