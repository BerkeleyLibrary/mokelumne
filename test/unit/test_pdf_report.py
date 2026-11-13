"""PyTest cases for the mokelumne.templates and pdf_report modules."""

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


class TestRenderResultsHtml:
    """Tests for render_results_html."""

    def render(self, results):
        """Render with fixed directories."""
        return pdf_report.render_results_html("/src", "/dest", "eng", results)

    def test_mixed_counts(self):
        """Count published, skipped, and failed results separately, ignoring missing entries."""
        failed = {**published("job-2"), "status": "PUBLISH_FAILED", "error": "disk full"}
        skipped = {
            **published("job-3", "/dest/skipped.pdf"),
            "document": {"source": "/src/skipped", "output": "skipped.pdf"},
            "status": "PDF_ALREADY_EXISTS",
        }
        html = self.render([published(), failed, skipped, None])
        assert "3 total: 1 published, 1 skipped, 1 failed" in html
        assert "Skipped (1)" in html
        assert "Failed (1)" in html
        assert "Published (1)" in html
        assert "disk full" in html

    def test_skipped_document_is_not_reported_as_an_error(self):
        """Show an existing PDF only in the skipped section with its document path."""
        skipped = {
            **published(output="/dest/skipped.pdf"),
            "document": {"source": "/src/skipped", "output": "skipped.pdf"},
            "status": "PDF_ALREADY_EXISTS",
            "job_id": None,
        }
        html = self.render([skipped])

        assert "1 total: 0 published, 1 skipped, 0 failed" in html
        assert "Skipped (1)" in html
        assert "/src/skipped" in html
        assert "/dest/skipped.pdf" in html
        assert "Failed (" not in html
        assert "Published (" not in html

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
