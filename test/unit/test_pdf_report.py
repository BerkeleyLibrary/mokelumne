"""PyTest cases for the mokelumne.templates and pdf_report modules."""

import pytest
from jinja2 import TemplateNotFound, UndefinedError

from mokelumne import templates
from mokelumne.util import pdf_report


def published(job_id: str = "job-1", output: str = "/dest/a.pdf") -> dict:
    """Build a published result."""
    return {
        "job_id": job_id,
        "status": "PUBLISHED",
        "output_path": output,
        "date_done": "2026-11-04T00:00:00+00:00",
        "error": None,
    }


class TestRender:
    """Tests for the shared renderer."""

    def test_missing_template_raises(self):
        """Raise when the template does not exist."""
        with pytest.raises(TemplateNotFound):
            templates.render("nope/missing.html")

    def test_missing_variable_raises(self):
        """Raise when a template variable is not supplied."""
        with pytest.raises(UndefinedError):
            templates.render("pdf_creation/results.html")


class TestFailedOcrResults:
    """Tests for failed_ocr_results."""

    def test_skips_success_and_none(self):
        """Ignore successful and empty jobs."""
        jobs = [{"id": "a", "status": "SUCCESS"}, None]
        assert not pdf_report.failed_ocr_results(jobs)

    def test_failure_includes_error(self):
        """Report non-successful jobs with their result as the error."""
        jobs = [{"id": "a", "status": "FAILURE", "result": "boom", "date_done": "d"}]
        assert pdf_report.failed_ocr_results(jobs) == [
            {
                "job_id": "a",
                "status": "FAILURE",
                "output_path": None,
                "date_done": "d",
                "error": "boom",
            }
        ]


class TestRenderResultsHtml:
    """Tests for render_results_html."""

    def render(self, results):
        """Render with fixed directories."""
        return pdf_report.render_results_html("/src", "/dest", "eng", results)

    def test_mixed_counts(self):
        """Show counts and both sections for mixed results."""
        failed = {**published("job-2"), "status": "PUBLISH_FAILED", "error": "disk full"}
        html = self.render([published(), failed, None])
        assert "2 total: 1 published, 1 failed" in html
        assert "Failed (1)" in html
        assert "Published (1)" in html
        assert "disk full" in html

    def test_empty(self):
        """Say so when no jobs ran."""
        html = self.render([])
        assert "No jobs were run." in html
        assert "Failed (" not in html

    def test_escapes_html(self):
        """Escape markup in paths and errors."""
        bad = {**published(), "status": "PUBLISH_FAILED", "error": "<script>x</script>"}
        html = self.render([bad])
        assert "<script>" not in html
        assert "&lt;script&gt;" in html

    def test_blank_language_is_automatic(self):
        """Show automatic when no language is set."""
        html = pdf_report.render_results_html("/src", "/dest", "", [])
        assert "automatic" in html
