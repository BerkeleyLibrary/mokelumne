"""Notify the user of job completion."""

from os import environ as ENV
from pathlib import Path

from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.sdk import dag, task, get_current_context

from mokelumne.batch_image.assets import public_dir


@dag(schedule=[public_dir], catchup=False, tags=["batch-image", "notification"])
def notify_user():
    """Notify the SPA list that a batch image job has been completed."""

    @task(inlets=[public_dir])
    def render_email_template() -> str:
        """Create the HTML template for the email message that will be sent."""
        context = get_current_context()
        asset = context["triggering_asset_events"][public_dir][0].asset
        directory = Path(asset.uri.replace("file://", ""))

        with (directory / "index.html").open(encoding='utf-8') as html:
            return html.read()

    EmailOperator(
        task_id='send_email',
        to=ENV.get("MOKELUMNE_MAIL_RCPT", "group-spa-lib-mokelumne-alerts@calgroups.Berkeley.EDU"),
        subject="Batch Image Description Results for Query",
        from_email="lib-noreply@berkeley.edu",
        html_content=render_email_template(),
    )


notify_user()
