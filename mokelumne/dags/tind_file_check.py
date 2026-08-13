import logging
from pathlib import Path
from airflow.sdk import Param, dag, task, get_current_context
from airflow.sdk.exceptions import AirflowFailException

from mokelumne.dags.fetch_tind_records import write_query_results_to_xml
from mokelumne.util.file_transfer import list_files, build_volume_path
from mokelumne.util.parse_marc_xml import list_values_from_marc_xml, extract_url_names
from mokelumne.util.tind_reports import format_da_tind_report

from mokelumne.util.storage import run_dir

logger = logging.getLogger(__name__)
BASE_VOLUME_PATH = "/srv/da"

@dag(
    dag_id="tind_file_check",
    description="Check files in directory against Tind collection search",
    schedule=None,
    catchup=False,
    params={
        "da_directory": Param(
            type="string", 
            description="The da directory containing derivatives. e.g. sugoroku/ucb/images"
        ),
        "file_extension": Param(
            type=["null", "string"],
            description="File extension. Default is any extension"
        ),
        "tind_query": Param(
            type="string",
            minLength=5,
            description_md="""[Search query](https://digicoll.lib.berkeley.edu/docs/search-guide/)
for the Tind [Search API](https://docs.tind.io/article/cmi2ci71w7-overview-of-the-search-api).
This is equivalent to the _p_ (pattern) parameter in the Tind query syntax.""",
            examples=["982__a:[Sugoroku]", "collection:[Sugoroku]"]
        ),
        "search_options": Param(
            default="In Tind",
            type="string",
            enum=["In Tind","Not in Tind","Both"],
            description="Whether to show files in the TIND collection, files not in the TIND collection, or both."
        ),
    },
)
def tind_file_check():
    """Compare a directory of files against Tind to see if they are present in the Tind 856"""

    @task
    def validated_source_dir(params: dict) -> str:
        """Checks that the source path exists."""
        da_path = build_volume_path(BASE_VOLUME_PATH, params["da_directory"].lstrip('/'))
        if not da_path.exists():
            raise AirflowFailException(f"Source directory does not exist: {da_path}")

        return str(da_path)

    @task
    def retrieve_file_list(da_path: str, params: dict) -> list[str]:
        """Retrieves a list of filenames from the path.  Extension can be used to filter results."""

        extension = params["file_extension"]
        if not extension:
            extension = "*"

        file_list = list_files(Path(da_path), extension)
        return file_list

    @task.short_circuit
    def has_files(file_list: list[str], params: dict) -> bool:
        """Short-circuit the DAG when no files are available for comparison."""

        if not file_list:
            logger.info(
                "File directory does not have any files with the extension: %s. Skipping subsequent tasks",
                params["file_extension"] or "*",
            )
            return False

        return True

    @task
    def retrieve_856_filenames(context=None) -> list:
        """Retrieve a list of 856 filenames from Tind"""
        context = get_current_context()
        batch_dir = run_dir(context["run_id"])
        xml_file = batch_dir / "tind_bulk.xml"
        
        field_list = list_values_from_marc_xml(
            xml_file_path=xml_file,
            field_num='856',
            ind1='4',
            ind2=' ',
            sub='u'
        )

        if field_list is None:
            logger.info("856 not found in Tind records")
        else:
            filenames = extract_url_names(field_list) 

            return filenames

        return []

    @task
    def compare_to_tind(file_list: list, filenames: list, params: dict) -> dict:
        """
        Compares local files against TIND records.
        returns a dictionary containing categorized lists.
        """

        search_criteria = params["search_options"]
    
        unique_local_files = list(dict.fromkeys(file_list or []))
        tind_set = set(filenames or [])

        results = {
            "in_tind": [],
            "not_in_tind": []
        }

        if search_criteria in ("In Tind", "Both"):
            results["in_tind"] = [filename for filename in unique_local_files if filename in tind_set]
        if search_criteria in ("Not in Tind", "Both"):
            results["not_in_tind"] = [filename for filename in unique_local_files if filename not in tind_set]

        return results

    @task
    def write_results(results: dict, params: dict, context=None) -> str:
        """
        Write Tind comparison report to disk
        """
        context = get_current_context()
        report_dir = run_dir(context["run_id"])
        report_file = report_dir / "tind_report.txt"

        search_criteria = params["search_options"]

        full_report_text = format_da_tind_report(results, search_criteria)
        report_file.write_text(full_report_text, encoding="utf-8")
		    
        logger.info("Successfully wrote tind comparison report %s", report_file)
        return str(report_file)

    @task
    def view_report_log(context=None):
        """View the Tind report in the Airflow"""
        context = get_current_context()
        report_dir = run_dir(context["run_id"])
        report_file = report_dir / "tind_report.txt"

        output_file = Path(report_file)
    
        if output_file.exists():
            logger.info("========================================")
            logger.info("Results are also written to %s", report_file)
            logger.info("========================================")
            logger.info(output_file.read_text(encoding="utf-8"))
        else:
            raise AirflowFailExecption("Output report file does not exist.")
    

    file_list = retrieve_file_list(da_path=validated_source_dir())
    file_list_ready = has_files(file_list=file_list)

    tind_metadata = write_query_results_to_xml(tind_query="{{ params.tind_query }}")
    tind_filename_list = retrieve_856_filenames()    
    comparison_results = compare_to_tind(file_list, tind_filename_list)
    write_results_to_file = write_results(results=comparison_results)
    view_results = view_report_log()

    file_list >> file_list_ready >> tind_metadata >> tind_filename_list >> comparison_results >> write_results_to_file >> view_results

tind_file_check()
