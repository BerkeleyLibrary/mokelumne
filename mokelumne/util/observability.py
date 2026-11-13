"""Provides utility functions for observability."""

from typing import List

from airflow.configuration import conf
from airflow.sdk.types import TaskInstance 

def task_instance_correlation_id(ti: TaskInstance) -> str:
    """
    Given an Airflow task instance, return a string that can be used as a 
    correlation ID that can be passed to another service to identify the
    originating task instance for a request, etc.
    
    :param ti: An Airflow task instance.
    :type ti: TaskInstance
    :returns: A slash-delimited string containing the correlation ID.
    :rtype: str
    """
    environment = conf.get("api", "instance_name", fallback="mokelumne")
    id_parts : List[str] = [environment, ti.dag_id, ti.run_id, ti.task_id]

    if isinstance(ti.map_index, int) and ti.map_index >= 0:
        id_parts.append(str(ti.map_index))

    return "/".join(id_parts)
