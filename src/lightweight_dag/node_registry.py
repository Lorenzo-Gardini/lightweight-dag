"""Registry for synchronous operations used by DAGs."""

import inspect
from collections.abc import Callable
from threading import Lock
from typing import Any, TypeVar, overload

F = TypeVar("F", bound=Callable[..., Any])

_nodes: dict[tuple[str | None, str], Callable[..., Any]] = {}
_nodes_lock = Lock()
_MISSING = object()


@overload
def dag_node(node_id: str) -> Callable[[F], F]: ...


@overload
def dag_node(dag_name: str, node_id: str) -> Callable[[F], F]: ...


def dag_node(
    dag_name_or_node_id: str,
    node_id: str | None | object = _MISSING,
) -> Callable[[F], F]:
    """Register a global operation or one scoped to a named DAG.

    Use ``@dag_node("operation")`` for a global operation or
    ``@dag_node("dag_name", "operation")`` for a DAG-specific version.
    Named DAG scopes do not fall back to global registrations.
    """
    if node_id is _MISSING:
        dag_name = None
        operation_id = dag_name_or_node_id
    else:
        dag_name = dag_name_or_node_id
        operation_id = node_id

    if not isinstance(operation_id, str) or not operation_id:
        raise ValueError("node_id must be a non-empty string")
    if dag_name is not None and (not isinstance(dag_name, str) or not dag_name):
        raise ValueError("dag_name must be a non-empty string")

    def register(func: F) -> F:
        if inspect.iscoroutinefunction(func):
            raise TypeError("@dag_node does not support async functions")
        if not inspect.isfunction(func):
            raise TypeError("@dag_node can only decorate functions")
        if func.__qualname__ != func.__name__:
            raise TypeError("@dag_node can only decorate module-level functions")
        key = (dag_name, operation_id)
        with _nodes_lock:
            if key in _nodes:
                raise ValueError(f"Node ID '{operation_id}' is already registered")
            _nodes[key] = func
        return func

    return register


def get_registered_nodes(
    dag_name: str | None = None,
) -> dict[str, Callable[..., Any]]:
    """Return a copy of the global registry or exactly one named-DAG scope."""
    with _nodes_lock:
        return {
            node_id: func
            for (scope, node_id), func in _nodes.items()
            if scope == dag_name
        }


def get_node_ids(dag_name: str | None = None) -> list[str]:
    """Return operation IDs in the global registry or named-DAG scope."""
    with _nodes_lock:
        return [node_id for scope, node_id in _nodes if scope == dag_name]


def get_node(node_id: str, dag_name: str | None = None) -> Callable[..., Any] | None:
    """Look up an operation by ID in the requested registry scope."""
    with _nodes_lock:
        return _nodes.get((dag_name, node_id))
