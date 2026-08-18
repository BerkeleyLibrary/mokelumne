"""Helpers for creating and formatting TIND reports."""

from collections.abc import Mapping, Sequence


def format_da_tind_report(results: Mapping[str, Sequence[str]], search_options: str) -> str:
    """Create and format Tind file search report"""
    in_tind = list(results.get("in_tind", []))
    not_in_tind = list(results.get("not_in_tind", []))

    report_lines = [
        "========================================",
        "       TIND FILE COMPARISON REPORT      ",
        "========================================",
    ]

    if search_options in ("In Tind", "Both"):
        report_lines.append(f"Total files found in Tind: {len(in_tind)}")

    if search_options in ("Not in Tind", "Both"):
        report_lines.append(f"Total files not found in Tind: {len(not_in_tind)}")

    report_lines.append("========================================\n")

    if in_tind and search_options in ("In Tind", "Both"):
        report_lines.append("--- FILES FOUND IN TIND ---")
        report_lines.extend(in_tind)
        report_lines.append("========================================\n")

    if not_in_tind and search_options in ("Not in Tind", "Both"):
        report_lines.append("--- FILES NOT FOUND IN TIND ---")
        report_lines.extend(not_in_tind)
        report_lines.append("========================================\n")

    return "\n".join(report_lines)
