"""Unit tests for observability functions."""

import pytest

from mokelumne.util.observability import task_instance_correlation_id
from test.conftest import mock_ti

class TestObservability:

    @pytest.mark.parametrize(
        "environment,dag_id,run_id,task_id,map_index,expected_correlation_id",
        [
            pytest.param(
                "production", "make_rocket_go", "1234", "fire_boosters", 1, "production/make_rocket_go/1234/fire_boosters/1",
                id="with_mapped_task"
            ),
            pytest.param(
                "staging", "bake_cookies", "abcd", "add_eggs", -1,
                "staging/bake_cookies/abcd/add_eggs",
                id="without_mapped_task"),
            pytest.param(
                "dev", "make_pasta", "5678", "boil_water", None,
                "dev/make_pasta/5678/boil_water",
                id="without_mapped_task_but_map_index_is_none"),
            pytest.param(
                None, "rotate_widget", "rstlne", "grab_wrench", -1, 
                "mokelumne/rotate_widget/rstlne/grab_wrench",
                id="with_default_instance_name"
            )
        ]
    )
    def test_task_instance_correlation_id(
        self,
        environment: str | None,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int | None,
        expected_correlation_id: str
    ):
        ti = mock_ti(dag_id=dag_id, run_id=run_id, task_id=task_id, map_index=map_index)

        assert task_instance_correlation_id(
            ti=ti, environment=environment
        ) == expected_correlation_id
