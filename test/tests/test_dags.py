import unittest

from pathlib import Path

import xmlrunner

from airflow.models import DagBag

class DagLoadingTest(unittest.TestCase):
    """integration test to ensure Dags load"""

    @classmethod
    def runTests(cls):
        return unittest.TextTestRunner().run(cls.all_tests())


    @classmethod
    def runTestsWithXMLReport(cls, report_file):
        with open(report_file, 'wb') as report:
            test_runner = xmlrunner.XMLTestRunner(output=report, failfast=False)
            result = test_runner.run(cls.all_tests())
        return result


    @classmethod
    def all_tests(cls):
        return unittest.defaultTestLoader.loadTestsFromTestCase(cls)


    def setUp(self) -> None:
        self.dag_path = Path('/opt/airflow/dags').resolve()
        self.dags = DagBag(dag_folder=self.dag_path, include_examples=False)


    def test_dags_load_with_no_errors(self) -> None:
        assert len(self.dags.import_errors) == 0, "Error during Dag import"
