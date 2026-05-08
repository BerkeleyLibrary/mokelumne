# pyright: reportTypedDictNotRequiredAccess=false

"""Generate descriptions for images matching a given TIND query using an LLM."""

import csv
import json
import logging
from collections import namedtuple
from datetime import datetime, UTC
from html import escape
from itertools import filterfalse
from os import environ as ENV
from pathlib import Path
from shutil import copyfile
from typing import List
from uuid import uuid4

from airflow.exceptions import AirflowFailException
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.sdk import dag, task, task_group, Param, get_current_context
from pymarc.marcxml import map_xml

from langchain_aws import ChatBedrock
from mokelumne.dags.fetch_tind_records import write_query_results_to_xml
from mokelumne.providers.tind.hooks.tind import TindHook
from mokelumne.util import langfuse
from mokelumne.util.image_describer import ImageDescriber
from mokelumne.util.image_fetcher import ImageFetcher, base64_size
from mokelumne.util.storage import run_dir, public_dir, public_path_to_url
from mokelumne.util.tind_csv_writer import TindCsvWriter, is_single_image_record


logger = logging.getLogger(__name__)


RunStatus = namedtuple('RunStatus', ('tind_id', 'status', 'path'))

SUPPORTED_IMAGE_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp"}
"""The supported image MIME types we will fetch."""

SIZE_LIMIT: int = 3750000
"""The upper bound for a size of image in bytes that we will fetch."""

FALLBACK_HEADERS = [
    "035__a",
    "5880_a",
    "Status",
    "520__a-1",
    "983__a-1",
    "983__d-1",
    "983__t-1",
    "Image Name",
    "Link to record",
    "Collection name",
]
"""Fallback headers for when `processed` has no result."""

FILENAME_TEMPLATE: str = "imagedesc_%s_{timestamp}.csv"
"""The template for the generated filenames."""


@dag(
    schedule=None,
    catchup=False,
    params={
        "tind_query": Param(
            title="Tind query",
            type="string",
            description_md="""[Search query](https://digicoll.lib.berkeley.edu/docs/search-guide/)
for the Tind [Search API](https://docs.tind.io/article/cmi2ci71w7-overview-of-the-search-api).
This is equivalent to the _p_ (pattern) parameter in the Tind query syntax.""",
            examples=[
                "collection:[Ladies Relief Society]",
                "\"ice cream\" AND 336__a:Image"
            ]
        ),
        "langfuse_prompt_name": Param(
            "image-description",
            title="Prompt name",
            type="string",
            section="Prompt configuration",
            description_md="""The name of the
[Langfuse prompt](https://langfuse.com/docs/prompt-management/overview) used
to generate image descriptions."""
        ),
        "langfuse_prompt_version_or_label": Param(
            "production",
            title="Version or label",
            type=["string", "integer"],
            section="Prompt configuration",
            examples=["production", "staging", "latest", 1, 2, 3],
            description_md="""
The [version or label](https://langfuse.com/docs/prompt-management/features/prompt-version-control)
for the Langfuse prompt used to generate image descriptions. You likely want to
keep this as **production** unless you are testing prompts."""
        ),
        "max_width": Param(
            8000,
            maximum=8000,
            title="Max image width",
            type="integer",
            section="Image fetcher configuration",
            description_md="The maximum width for the fetched image.  Must be less than 8000px."
        ),
        "max_height": Param(
            8000,
            maximum=8000,
            title="Max image height",
            type="integer",
            section="Image fetcher configuration",
            description_md="The maximum height for the fetched image.  Must be less than 8000px."
        ),
    },
    tags=["batch-image", "csv", "generate-descriptions", "llm", "process",],
)
def gen_llm_image_descriptions():
    """Generate descriptions for images matching a given TIND query using an LLM."""

    @task_group()
    def fetch_tind_records():
        """Fetch TIND records matching a query and write them to an XML file."""

        @task
        def validate_params() -> str:
            """Validate that the tind_query parameter is not empty and serialise params to disk.

            :raises AirflowFailException: If the tind_query parameter is empty.
            """

            context = get_current_context()
            val = context["params"].get("tind_query", "")
            if not val.strip():
                raise AirflowFailException("Parameter tind_query cannot be empty")

            with (run_dir(context["run_id"]) / "params.json").open("w", encoding="utf-8") as fp:
                json.dump(context["params"], fp)

            return val.strip()

        query = validate_params()
        return write_query_results_to_xml(query)

    @task_group()
    def tind_filter():
        """Filter TIND records on conditions matching the batch image processing workflow."""

        @task
        def filter_records() -> None:
            """Filter TIND records from query XML, and write to_process and skipped CSV files."""

            context = get_current_context()
            batch_dir = run_dir(context["run_id"])
            xml_file = batch_dir / "tind_bulk.xml"

            with TindCsvWriter(batch_dir, is_single_image_record) as csv_writer:
                map_xml(csv_writer.process_tind_record, xml_file)

            logger.info(
                "Tind filter complete. to_process=%s (%s records), skipped=%s (%s records)",
                csv_writer.csv_p,
                csv_writer.count_p,
                csv_writer.csv_s,
                csv_writer.count_s
            )

        filter_records()

    @task_group()
    def fetch_images():
        """Fetches images from a filtered list of TIND records."""
        @task(multiple_outputs=True)
        def read_csv_to_process() -> dict[str, List[str] | str]:
            """Read the to_process.csv file to determine our target TIND records."""
            context = get_current_context()
            batch_dir = run_dir(context["run_id"])

            csv_path = batch_dir / "to_process.csv"
            record_ids: list[str] = []
            records: dict[str, list[str]] = {}

            with csv_path.open("r", encoding="utf-8") as csv_file:
                reader = csv.reader(csv_file)
                for row in reader:
                    record_ids.append(row[0])
                    records[row[0]] = row
            return {
                "record_ids": record_ids[1:],
                "records": records,
            }

        @task
        def load_record_ids(processed: dict[str, List[str] | str]) -> List[str]:
            """Load the record IDs from the to_process job."""
            return processed["record_ids"]

        @task
        def load_records(
                processed: dict[str, List[str] | str | dict[str, list[str]]],
        ) -> dict[str, list[str]]:
            """Load the records from the to_process job."""
            return processed["records"]

        @task
        def fetch_image_to_record_directory(run_id: str, fetcher: ImageFetcher,
                                            tind_id: str) -> RunStatus:
            """Fetch an image from TIND to the target record's storage directory."""
            context = get_current_context()
            fetcher.max_width = context["params"]["max_width"]
            fetcher.max_height = context["params"]["max_height"]
            try:
                file_md = fetcher.get_metadata_for_record(tind_id)[0]
                if not file_md.get("mime") in SUPPORTED_IMAGE_TYPES:
                    return RunStatus(
                        tind_id=tind_id,
                        path="",
                        status=f"skipped: Unsupported file type {file_md.get('mime')}",
                    )

                path = str(fetcher.fetch_one_image_for_record(tind_id, run_id))
            except Exception as ex:  # pylint: disable=broad-exception-caught
                logger.warning("Fetcher encountered exception", exc_info=ex)
                return RunStatus(tind_id=tind_id, status=f'failed: {str(ex)}', path='')

            return RunStatus(tind_id=tind_id, status="fetched", path=path)

        @task
        def write_status_to_fetched_csv(
                records: dict[str, list[str]], statuses: List[RunStatus]
        ) -> None:
            """Write the status of processed records to a CSV file."""
            context = get_current_context()

            fetched_path = run_dir(context["run_id"]) / "fetched.csv"
            with fetched_path.open("w", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow((*records["Record ID"], "Image Path"))

                status_col = records["Record ID"].index("Status")

                for status in statuses:
                    record = [*records[status[0]], *status[2:]]
                    record[status_col] = status[1]
                    writer.writerow(record)

        processed = read_csv_to_process()
        client = TindHook()
        fetcher = ImageFetcher(client, 8000, 8000, SIZE_LIMIT, size_transform=base64_size)
        fetch_partial = fetch_image_to_record_directory.partial(fetcher=fetcher)
        results = fetch_partial.expand(tind_id=load_record_ids(processed))
        write_status_to_fetched_csv(load_records(processed), results)

    @task_group()
    def generate_descriptions():
        """Generates descriptions for images listed in a CSV file, writing results to a new CSV."""

        @task
        def get_prompt() -> dict[str, str]:
            """Fetch a prompt from Langfuse to use for generating image descriptions."""
            context = get_current_context()
            prompt = langfuse.get_prompt(
                name=context["params"]["langfuse_prompt_name"],
                version_or_label=context["params"]["langfuse_prompt_version_or_label"],
            )

            return {"prompt": prompt.prompt, "version": prompt.version}

        @task
        def read_and_batch_csv() -> list[list[dict[str, str]]]:
            """Fetch the CSV and chunk it for processing."""
            context = get_current_context()
            fetched_path = run_dir(context["run_id"]) / "fetched.csv"

            with open(fetched_path, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(filter(lambda x: x["Status"] == "fetched", reader))

            # we could make the chunking a parameter or an env variable
            # we want to pass an empty batch to the downstream tasks to continue the pipeline
            return [rows[i: i + 10] for i in range(0, len(rows), 10)] or [[]]

        @task(max_active_tis_per_dagrun=10)
        def invoke_llm_on_batch_with_prompt(
                batch: list[dict[str, str]], prompt: dict[str, str]
        ) -> list[dict[str, str]]:
            """For each image in the batch, encode it and send to LLM for description generation."""
            results = []

            model = ChatBedrock(model=ENV["AWS_MODEL_ID"], provider=ENV["AWS_MODEL_PROVIDER"])
            describer = ImageDescriber(model, prompt["prompt"])

            for record in batch:
                results.append(describer.describe(record))

            return results

        @task()
        def transform_results(
                batch_results: list[dict[str, str]], prompt: dict[str, str]
        ) -> list[dict[str, str]]:
            """Apply any necessary mapping or transformations to the batch results
            before writing to CSV."""

            processed_dicts = []
            for record in batch_results:
                processed_dict = {
                    "035__a": record["035__a"],
                    "Link to record": f"https://berkeley.tind.io/r/sys-num/{record['035__a']}",
                    "Image Name": Path(record["Image Path"]).name,
                    "Collection name": record["Collection name"],
                    "Status": record["Status"],
                    "520__a-1": record.get("Description", ""),
                    "5880_a": f"Image description generated by AI ({ENV.get('AWS_MODEL_LABEL')})"
                              " and reviewed on [MM/YYYY].",
                    "983__a-1": f"mokelumne-image-description|{ENV.get('AWS_MODEL_ID')}"
                                f"|mokelumne/{prompt['version']}",
                    "983__d-1": datetime.now(UTC).strftime("%Y-%m-%d"),
                    "983__t-1": "520",
                }
                processed_dicts.append(processed_dict)

            return processed_dicts

        @task
        def write_output_csv(processed_dicts: list[list[dict[str, str]]]) -> None:
            """Write all batch results to a new CSV."""
            context = get_current_context()
            processed_csv_path = run_dir(context["run_id"]) / "processed.csv"

            all_results = [record for batch in processed_dicts for record in batch]

            # if no results, we want to write a CSV with headers to continue downstream processing
            fieldnames = all_results[0].keys() if all_results else FALLBACK_HEADERS

            with open(processed_csv_path, mode="w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_results)

        prompt = get_prompt()
        batches = read_and_batch_csv()
        batch_results = invoke_llm_on_batch_with_prompt.partial(prompt=prompt).expand(
            batch=batches
        )
        processed_dicts = transform_results.partial(prompt=prompt).expand(
            batch_results=batch_results
        )
        write_output_csv(processed_dicts)  # pyright: ignore[reportArgumentType]

    @task_group()
    def summarise_job():
        """Summarise the DAG runs for the job."""

        @task
        def generate_id() -> str:
            """Generate a URL-safe directory for collated output."""
            output_path = public_dir() / str(uuid4())
            output_path.mkdir()  # We don't exist_ok=True because it should be unique.
            return str(output_path)

        @task
        def collate_csvs(output_dir_str: str) -> str:
            """Gather the CSVs from the DAG runs and place them in the given directory."""
            context = get_current_context()
            asset_path = run_dir(context["run_id"])
            output_path = Path(output_dir_str)

            timestamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
            template_name = FILENAME_TEMPLATE.format(timestamp=timestamp)

            for csv_file in ('processed', 'fetched', 'skipped'):
                copyfile(asset_path / f'{csv_file}.csv', output_path / (template_name % csv_file))
            copyfile(asset_path / 'params.json', output_path / 'params.json')

            return timestamp

        @task
        def generate_summary(output_dir_str: str, timestamp: str) -> str:
            """Generate a summary of the files in the collated path."""
            def count_success_fail_of_csv(csv_file: Path, success: str) -> tuple[int, int, int]:
                """Count the success and failure rows for the given CSV."""
                with csv_file.open(encoding="utf-8") as fp:
                    reader = csv.reader(fp)
                    header = next(reader)
                    all_rows = list(reader)
                    total_count = len(all_rows)
                    status_col = header.index('Status')
                    failures = len(list(filterfalse(lambda x: x == success,
                                                    (row[status_col] for row in all_rows))))
                    successes = total_count - failures
                return total_count, successes, failures

            output_path = Path(output_dir_str)

            template_name = FILENAME_TEMPLATE.format(timestamp=timestamp)
            proc_path = output_path / (template_name % 'processed')
            fetched_path = output_path / (template_name % 'fetched')
            skipped_path = output_path / (template_name % 'skipped')
            with (output_path / 'params.json').open(encoding='utf-8') as fp:
                params = json.load(fp)

            with skipped_path.open(encoding='utf-8') as skipped:
                skip_count = len(skipped.readlines()) - 1

            fetch_count, fetch_success, fetch_failures = (
                count_success_fail_of_csv(fetched_path, "fetched")
            )
            proc_count, proc_success, proc_failures = (
                count_success_fail_of_csv(proc_path, "success")
            )

            template_html = f"""
<html><head><title>Batch image description results</title></head><body>
<h1>Batch image description results</h1>
<p>Query: {escape(params['tind_query'])}</p>
<dl>
    <dt><a href="{public_path_to_url(proc_path)}">{proc_count} images processed</a></dt>
    <dd>{proc_success} succeeded, {proc_failures} failed</dd>
    <dt><a href="{public_path_to_url(fetched_path)}">{fetch_count} images fetched</a></dt>
    <dd>{fetch_success} succeeded, {fetch_failures} failed</dd>
    <dt><a href="{public_path_to_url(skipped_path)}">{skip_count} records skipped</a></dt>
    <dd>These records did not match filter criteria</dd>
</dl>
</body></html>"""

            with (output_path / 'index.html').open('w') as html:
                html.write(template_html)

            return output_dir_str

        output_directory = generate_id()
        csv_timestamp = collate_csvs(output_directory)
        return [csv_timestamp, generate_summary(output_directory, csv_timestamp)]

    @task_group()
    def notify_user(pub_directory):
        """Notify the SPA list that a batch image job has been completed."""

        @task
        def render_email_template(asset_directory: str) -> str:
            """Create the HTML template for the email message that will be sent."""
            asset_path = Path(str(asset_directory))

            with (asset_path / "index.html").open(encoding='utf-8') as html:
                return html.read()

        EmailOperator(
            task_id='send_email',
            to=ENV.get("MOKELUMNE_MAIL_RCPT",
                       "group-spa-lib-mokelumne-alerts@calgroups.Berkeley.EDU"),
            subject="Batch Image Description Results for Query",
            from_email="lib-noreply@berkeley.edu",
            html_content=render_email_template(pub_directory),
        )

    collate, directory = summarise_job()
    filtered = fetch_tind_records() >> tind_filter()
    filtered >> collate  # pyright: ignore[reportUnusedExpression]
    fetched = filtered >> fetch_images()
    fetched >> collate  # pyright: ignore[reportUnusedExpression]
    fetched >> generate_descriptions() >> collate >> directory >> notify_user(directory)


gen_llm_image_descriptions()
