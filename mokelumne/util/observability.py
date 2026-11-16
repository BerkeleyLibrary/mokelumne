"""Provides utility functions for observability."""

from typing import List

from airflow.configuration import conf
from airflow.sdk.types import TaskInstance

def task_instance_correlation_id(
        ti: TaskInstance,
        environment: str | None = "mokelumne"
    ) -> str:
    """
    Given an Airflow task instance, return a string that can be used as a
    correlation ID that can be passed to another service to identify the
    originating task instance for a request, etc.

    .. todo:: Decide whether we need to include the task instance's try number
    
    :param ti: An Airflow task instance for which to generate a correlation ID.
    :type ti: TaskInstance
    :param environment: The environment name for the Mokelumne deployment.
    :type environment: str | None
    :returns: A slash-delimited string containing the correlation ID.
    :rtype: str
    """

    if not environment:
        environment = "mokelumne"

    id_parts : List[str] = [environment, ti.dag_id, ti.run_id, ti.task_id]

    if isinstance(ti.map_index, int) and ti.map_index >= 0:
        id_parts.append(str(ti.map_index))

    return "/".join(id_parts)
