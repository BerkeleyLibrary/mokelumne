"""PyTest cases for the TIND report formatting helpers."""

from mokelumne.util.tind_reports import format_da_tind_report


class TestFormatDaTindReport:
    """Tests for the TIND report formatter."""

    def test_formats_in_tind_only_report(self):
        results = {"in_tind": ["a.jpg", "b.jpg"], "not_in_tind": []}

        report = format_da_tind_report(results, "In Tind")

        assert "TIND FILE COMPARISON REPORT" in report
        assert "Total files found in Tind: 2" in report
        assert "a.jpg" in report
        assert "b.jpg" in report
        assert "FILES NOT FOUND IN TIND" not in report

    def test_formats_not_in_tind_only_report(self):
        results = {"in_tind": [], "not_in_tind": ["c.jpg"]}

        report = format_da_tind_report(results, "Not in Tind")

        assert "Total files not found in Tind: 1" in report
        assert "c.jpg" in report
        assert "FILES FOUND IN TIND" not in report

    def test_formats_both_sections(self):
        results = {"in_tind": ["a.jpg"], "not_in_tind": ["b.jpg"]}

        report = format_da_tind_report(results, "Both")

        assert "Total files found in Tind: 1" in report
        assert "Total files not found in Tind: 1" in report
        assert "--- FILES FOUND IN TIND ---" in report
        assert "--- FILES NOT FOUND IN TIND ---" in report
