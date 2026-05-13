"""Template renderers for Batch Image emails, web pages, etc.

Jinja2 templates live alongside this module as ``*.html`` files. Helpers in
this package load them via a FileSystemLoader anchored to ``__file__``.
"""

from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from mokelumne.plugins.static_files.helpers import static_path_to_url


_env = Environment(
    loader=FileSystemLoader(Path(__file__).parent),
    autoescape=True,
)


def render_results_html(
    query: str,
    processed_path: Path, processed_count: int, processed_success: int, processed_failures: int,
    fetched_path: Path, fetched_count: int, fetched_success: int, fetched_failures: int,
    skipped_path: Path, skipped_count: int,
    embed: bool,
) -> str:
    """Render the per-run results summary.

    When ``embed`` is True, asset links resolve inside Airflow's sandboxed
    "Files" iframe (text/plain downgrade). When False, links are suitable for
    standalone browser use, e.g. an emailed summary.
    """
    return _env.get_template("results.html").render(
        query=query,
        processed={
            "url": static_path_to_url(processed_path, embed=embed),
            "count": processed_count,
            "success": processed_success,
            "failures": processed_failures,
        },
        fetched={
            "url": static_path_to_url(fetched_path, embed=embed),
            "count": fetched_count,
            "success": fetched_success,
            "failures": fetched_failures,
        },
        skipped={
            "url": static_path_to_url(skipped_path, embed=embed),
            "count": skipped_count,
        },
    )
