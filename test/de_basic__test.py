import pytest
from .downstream import assert_dag_dict_equal

from airflow.models import DagBag


@pytest.fixture()
def dagbag():
    return DagBag()


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="basic")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 5

    assert_dag_dict_equal(
        {
            "task_1": ["task_3"],
            "task_2": ["task_3"],
            "task_3": ["task_4"],
            "task_4": [],
            "task_5": [],
        },
        dag,
    )
