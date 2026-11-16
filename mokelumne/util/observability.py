"""Provides utility functions for observability."""

from typing import List

def task_instance_correlation_id(
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int | None = -1,
        environment: str | None = "mokelumne",
    ) -> str:
    """
    Given metadata for an Airflow task instance, return a string that can be 
    used as a  correlation ID that can be passed to another service to identify
    the originating task instance for a request, etc.

    .. todo:: Decide whether we need to include the task instance's try number
    
    :param dag_id: The Dag ID for the task instance's Dag.
    :type dag_id: str
    :param run_id: The run ID for the DagRun the task instance occured withinn.
    :type run_id: str
    :param task_id: The task ID for the given task instance.
    :param map_index: The map index for a mapped task's specific task instance.
        Note: ``airflow.sdk.types.TaskInstance`` returns ``-1`` for unmapped
        tasks, but other ``TaskInstance`` classes may return ``None`` instead.
    :type map_index: int | None
    :param environment: The environment name for the Mokelumne deployment.
    :type environment: str |none
    :returns: A slash-delimited string containing the correlation ID.
    :rtype: str
    """

    if not environment:
        environment = "mokelumne"

    id_parts : List[str] = [environment, dag_id, run_id, task_id]

    if isinstance(map_index, int) and map_index >= 0:
        id_parts.append(str(map_index))

    return "/".join(id_parts)
