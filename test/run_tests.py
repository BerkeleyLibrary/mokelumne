#!/usr/bin/env python3

# -*- coding: utf-8 -*-
"""run_tests.py

To be used as a Docker command when tests in a standalone container.
This script runs the tests and generates JUnit-compatible xml reports.
"""

import os
import pathlib
import sys

from tests import test_dags

if os.environ.get("ARTIFACTS_DIR") is not None:
    ARTIFACTS_ROOT = pathlib.Path(os.environ["ARTIFACTS_DIR"])
else:
    ARTIFACTS_ROOT = pathlib.Path(__file__).parent.parent / "artifacts"
REPORTS_DIR = ARTIFACTS_ROOT / 'unittest'
TIMEOUT_SECONDS = 60


def log(msg):
    print(msg, file=sys.stderr)


def ensure_reports_dir():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    return REPORTS_DIR


def main():
    reports_dir = ensure_reports_dir()
    report_file = reports_dir / 'airflow_test.xml'

    log(f"Writing test report to {report_file}")
    result = test_dags.DagLoadingTest.runTestsWithXMLReport(report_file)
    if not result.wasSuccessful():
        exit(1)


if __name__ == "__main__":
    main()
