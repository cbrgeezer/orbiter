import pytest

from orbiter import DAG
from orbiter.core.exceptions import DAGValidationError


def test_dag_validates_ok():
    dag = DAG("x")

    @dag.task()
    async def a():
        return 1

    @dag.task(depends_on=["a"])
    async def b():
        return 2

    dag.finalize()
    assert dag.topological_order() == ["a", "b"]


def test_dag_detects_cycle():
    dag = DAG("x")

    @dag.task(depends_on=["b"])
    async def a():
        return 1

    @dag.task(depends_on=["a"])
    async def b():
        return 2

    with pytest.raises(DAGValidationError):
        dag.finalize()


def test_dag_missing_dep():
    dag = DAG("x")

    @dag.task(depends_on=["ghost"])
    async def a():
        return 1

    with pytest.raises(DAGValidationError):
        dag.finalize()


def test_dag_duplicate_task():
    dag = DAG("x")

    @dag.task(id="dup")
    async def a():
        return 1

    with pytest.raises(DAGValidationError):

        @dag.task(id="dup")
        async def b():
            return 2


def test_ready_tasks_respects_deps():
    dag = DAG("x")

    @dag.task()
    async def a():
        return 1

    @dag.task()
    async def b():
        return 1

    @dag.task(depends_on=["a", "b"])
    async def c():
        return 1

    dag.finalize()
    assert dag.ready_tasks(completed=set()) == ["a", "b"]
    assert dag.ready_tasks(completed={"a"}) == ["b"]
    assert dag.ready_tasks(completed={"a", "b"}) == ["c"]


def test_fingerprint_stable():
    def _build():
        d = DAG("x")

        @d.task()
        async def a():
            return 1

        return d

    assert _build().fingerprint() == _build().fingerprint()
