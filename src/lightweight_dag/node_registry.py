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
def dag_node() -> Callable[[F], F]: ...


@overload
def dag_node(func: F, /) -> F: ...


@overload
def dag_node(*, dag_name: str) -> Callable[[F], F]: ...


@overload
def dag_node(node_id: str) -> Callable[[F], F]: ...


@overload
def dag_node(dag_name: str, node_id: str) -> Callable[[F], F]: ...


def dag_node(
    dag_name_or_node_id: str | F | None | object = _MISSING,
    node_id: str | None | object = _MISSING,
    *,
    dag_name: str | None = None,
) -> Callable[[F], F] | F:
    """Register a global operation or one scoped to a named DAG.

    The operation ID defaults to the function name. Use ``@dag_node("operation")``
    or ``@dag_node("dag_name", "operation")`` to provide it explicitly, or
    ``@dag_node(dag_name="dag_name")`` to infer it in a DAG-specific scope.
    Named DAG scopes do not fall back to global registrations.
    """
    direct_func = None
    if inspect.isfunction(dag_name_or_node_id):
        if node_id is not _MISSING or dag_name is not None:
            raise TypeError("A function cannot be combined with decorator arguments")
        direct_func = dag_name_or_node_id
        scope = None
        operation_id = _MISSING
    elif dag_name is not None:
        if dag_name_or_node_id is not _MISSING:
            raise TypeError("Use either positional arguments or dag_name, not both")
        scope = dag_name
        operation_id = node_id
    elif dag_name_or_node_id is _MISSING:
        scope = None
        operation_id = node_id
    elif dag_name_or_node_id is None:
        if node_id is _MISSING:
            raise ValueError("node_id must be a non-empty string")
        scope = None
        operation_id = node_id
    elif node_id is _MISSING:
        scope = None
        operation_id = dag_name_or_node_id
    else:
        scope = dag_name_or_node_id
        operation_id = node_id

    if operation_id is not _MISSING and (
        not isinstance(operation_id, str) or not operation_id
    ):
        raise ValueError("node_id must be a non-empty string")
    if scope is not None and (not isinstance(scope, str) or not scope):
        raise ValueError("dag_name must be a non-empty string")

    def register(func: F) -> F:
        if inspect.iscoroutinefunction(func):
            raise TypeError("@dag_node does not support async functions")
        if not inspect.isfunction(func):
            raise TypeError("@dag_node can only decorate functions")
        if func.__qualname__ != func.__name__:
            raise TypeError("@dag_node can only decorate module-level functions")
        resolved_operation_id = (
            func.__name__ if operation_id is _MISSING else operation_id
        )
        key = (scope, resolved_operation_id)
        with _nodes_lock:
            if key in _nodes:
                raise ValueError(
                    f"Node ID '{resolved_operation_id}' is already registered"
                )
            _nodes[key] = func
        return func

    if direct_func is not None:
        return register(direct_func)
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
