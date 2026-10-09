from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import FunctionType

import pytest

from lightweight_dag import node_registry
from lightweight_dag.node_registry import (
    dag_node,
    get_node,
    get_node_ids,
    get_registered_nodes,
)


def sample_operation():
    return "done"


async def async_sample_operation():
    return "done"


def global_transform_operation():
    return "global"


def report_transform_operation():
    return "report"


def test_dag_node_registers_function_and_returns_registry_copy(monkeypatch):
    monkeypatch.setattr(node_registry, "_nodes", {})

    registered_function = dag_node("sample_operation")(sample_operation)
    registry_copy = get_registered_nodes()
    registry_copy.clear()

    assert registered_function is sample_operation
    assert get_node("sample_operation") is sample_operation
    assert get_node_ids() == ["sample_operation"]


def test_dag_node_infers_operation_id_from_function_name(monkeypatch):
    monkeypatch.setattr(node_registry, "_nodes", {})

    dag_node()(sample_operation)

    assert get_node("sample_operation") is sample_operation


def test_dag_node_infers_operation_id_for_named_dag(monkeypatch):
    monkeypatch.setattr(node_registry, "_nodes", {})

    dag_node(dag_name="report")(report_transform_operation)

    assert get_node("report_transform_operation", "report") is report_transform_operation


def test_dag_node_can_be_used_without_parentheses(monkeypatch):
    monkeypatch.setattr(node_registry, "_nodes", {})

    registered_function = dag_node(sample_operation)

    assert registered_function is sample_operation
    assert get_node("sample_operation") is sample_operation


def test_dag_node_registers_global_and_dag_scoped_functions(monkeypatch):
    monkeypatch.setattr(node_registry, "_nodes", {})

    dag_node("transform")(global_transform_operation)
    dag_node("report", "transform")(report_transform_operation)

    assert get_registered_nodes() == {"transform": global_transform_operation}
    assert get_registered_nodes("report") == {"transform": report_transform_operation}
    assert get_registered_nodes("other") == {}
    assert get_node("transform", "report") is report_transform_operation
    assert get_node("transform", "other") is None
    assert get_node_ids("report") == ["transform"]

    assert dag_node("report", "transform")(report_transform_operation) is report_transform_operation


@pytest.mark.parametrize("args", [("", "transform"), ("report", ""), ("report", None)])
def test_dag_node_rejects_invalid_scope_or_operation(args):
    with pytest.raises(ValueError):
        dag_node(*args)


def test_dag_node_rejects_duplicate_identifier(monkeypatch):
    monkeypatch.setattr(node_registry, "_nodes", {})
    dag_node("sample_operation")(sample_operation)

    with pytest.raises(ValueError, match="already registered"):
        dag_node("sample_operation")(global_transform_operation)


def test_dag_node_ignores_reregistration_of_same_function(monkeypatch):
    monkeypatch.setattr(node_registry, "_nodes", {})
    dag_node()(sample_operation)
    redefined_function = FunctionType(
        sample_operation.__code__,
        sample_operation.__globals__,
        name=sample_operation.__name__,
    )
    redefined_function.__qualname__ = sample_operation.__qualname__
    redefined_function.__module__ = sample_operation.__module__

    assert dag_node()(redefined_function) is redefined_function
    assert get_node("sample_operation") is sample_operation


def test_concurrent_duplicate_registration_is_atomic(monkeypatch):
    monkeypatch.setattr(node_registry, "_nodes", {})
    workers = 8
    start = Barrier(workers)

    def register_operation():
        start.wait()
        return dag_node("shared_operation")(sample_operation) is sample_operation

    with ThreadPoolExecutor(max_workers=workers) as executor:
        registered = list(executor.map(lambda _: register_operation(), range(workers)))

    assert all(registered)
    assert get_node_ids() == ["shared_operation"]
    assert get_node("shared_operation") is sample_operation


@pytest.mark.parametrize("node_id", ["", None])
def test_dag_node_rejects_empty_or_non_string_identifier(node_id):
    with pytest.raises(ValueError, match="non-empty string"):
        dag_node(node_id)


def test_dag_node_rejects_nested_functions():
    def nested_operation():
        return None

    with pytest.raises(TypeError, match="module-level functions"):
        dag_node("nested_operation")(nested_operation)


def test_dag_node_rejects_callable_instances():
    class CallableOperation:
        def __call__(self):
            return None

    with pytest.raises(TypeError, match="can only decorate functions"):
        dag_node("callable_operation")(CallableOperation())


def test_dag_node_rejects_async_functions(monkeypatch):
    monkeypatch.setattr(node_registry, "_nodes", {})

    with pytest.raises(TypeError, match="does not support async functions"):
        dag_node("async_operation")(async_sample_operation)
