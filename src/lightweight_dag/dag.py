"""Typed DAG models, graph validation, execution, and named DAG suites."""

from collections import deque
from collections.abc import Callable
from functools import cached_property
from inspect import Signature, signature
from types import UnionType
from typing import Any, Union, get_args, get_origin, get_type_hints

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lightweight_dag.node_registry import get_registered_nodes


class _FrozenList(list):
    """A list-compatible container that rejects in-place mutation."""

    @staticmethod
    def _reject_mutation(*args, **kwargs):
        raise TypeError("DAG model collections are immutable")

    __delitem__ = __iadd__ = __imul__ = __setitem__ = _reject_mutation
    append = clear = extend = insert = pop = remove = reverse = sort = _reject_mutation


class _FrozenDict(dict):
    """A dict-compatible container that rejects in-place mutation."""

    @staticmethod
    def _reject_mutation(*args, **kwargs):
        raise TypeError("DAG model collections are immutable")

    __delitem__ = __ior__ = __setitem__ = _reject_mutation
    clear = pop = popitem = setdefault = update = _reject_mutation


def _function_annotations(function: Callable[..., Any]) -> dict[str, Any]:
    try:
        return get_type_hints(function)
    except (NameError, TypeError):
        return dict(getattr(function, "__annotations__", {}))


def _type_is_compatible(produced: Any, expected: Any) -> bool:
    """Conservatively compare common Python annotations without checking values."""
    if produced in (Any, Signature.empty) or expected in (Any, Signature.empty):
        return True
    if produced is None:
        produced = type(None)
    if expected is None:
        expected = type(None)

    produced_origin = get_origin(produced)
    expected_origin = get_origin(expected)
    union_types = (Union, UnionType)
    produced_args = get_args(produced)
    expected_args = get_args(expected)

    if produced_origin in union_types:
        produced_options = produced_args
    else:
        produced_options = (produced,)
    if expected_origin in union_types:
        expected_options = expected_args
    else:
        expected_options = (expected,)

    if len(produced_options) > 1 or len(expected_options) > 1:
        return all(
            any(_type_is_compatible(option, accepted) for accepted in expected_options)
            for option in produced_options
        )

    actual = produced_options[0]
    wanted = expected_options[0]
    actual_origin = get_origin(actual) or actual
    wanted_origin = get_origin(wanted) or wanted
    if not isinstance(actual_origin, type) or not isinstance(wanted_origin, type):
        return True
    try:
        if not issubclass(actual_origin, wanted_origin):
            return False
    except TypeError:
        return True

    actual_args = get_args(actual)
    wanted_args = get_args(wanted)
    if not actual_args or not wanted_args or len(actual_args) != len(wanted_args):
        return True
    return all(
        _type_is_compatible(actual_arg, wanted_arg)
        and _type_is_compatible(wanted_arg, actual_arg)
        for actual_arg, wanted_arg in zip(actual_args, wanted_args)
    )


def _load_yaml(yaml_str: str) -> Any:
    """Load safe YAML while rejecting duplicate mapping keys."""
    import yaml
    from yaml.constructor import ConstructorError

    class UniqueKeyLoader(yaml.SafeLoader):
        pass

    def construct_unique_mapping(loader, node, deep=False):
        loader.flatten_mapping(node)
        mapping = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node, deep=deep)
            if key in mapping:
                raise ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"found duplicate key {key!r}",
                    key_node.start_mark,
                )
            mapping[key] = loader.construct_object(value_node, deep=deep)
        return mapping

    UniqueKeyLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_unique_mapping,
    )
    return yaml.load(yaml_str, Loader=UniqueKeyLoader)


class Node(BaseModel):
    """One operation in a DAG and the IDs of its predecessor nodes."""

    node_id: str = Field(..., description="The unique identifier for this node")
    operation: str = Field(..., description="The operation performed by this node")
    inputs: list[str] = Field(
        default_factory=_FrozenList, description="List of input node IDs"
    )
    params: dict[str, int | float | str] = Field(
        default_factory=_FrozenDict,
        description="Dictionary of parameters for this node",
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
    )

    @field_validator("inputs")
    @classmethod
    def freeze_inputs(cls, value: list[str]) -> list[str]:
        return _FrozenList(value)

    @field_validator("params")
    @classmethod
    def freeze_params(
        cls, value: dict[str, int | float | str]
    ) -> dict[str, int | float | str]:
        return _FrozenDict(value)


class NodeExecutionResult(BaseModel):
    """Common identity for one node's execution outcome."""

    node_id: str
    dag_name: str | None = None

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        arbitrary_types_allowed=True,
    )

    @property
    def result(self) -> bool:
        """Legacy success flag, derived from the concrete outcome type."""
        return isinstance(self, SuccessfulNodeExecutionResult)


class SuccessfulNodeExecutionResult(NodeExecutionResult):
    """Successful node outcome containing its produced value."""

    value: Any


class ErrorNodeExecutionResult(NodeExecutionResult):
    """Failed node outcome containing the original exception."""

    error: Exception


class SkippedNodeExecutionResult(NodeExecutionResult):
    """Skipped node outcome with no produced value or exception."""


class DagExecutionResult(BaseModel):
    """Run status and per-node outcomes."""

    status: bool
    results: dict[str, NodeExecutionResult] = Field(
        default_factory=_FrozenDict,
        description="Execution outcome for each selected node",
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        arbitrary_types_allowed=True,
    )

    @field_validator("results")
    @classmethod
    def freeze_results(
        cls, value: dict[str, NodeExecutionResult]
    ) -> dict[str, NodeExecutionResult]:
        return _FrozenDict(value)

    @model_validator(mode="after")
    def check_result_node_ids(self) -> "DagExecutionResult":
        mismatched_ids = [
            node_id
            for node_id, outcome in self.results.items()
            if node_id != outcome.node_id
        ]
        if mismatched_ids:
            raise ValueError(
                f"Result keys must match outcome node IDs: {sorted(mismatched_ids)}"
            )
        return self


class DagExecutionPlan(BaseModel):
    """Immutable description of the nodes and external inputs for a DAG run."""

    execution_order: list[str] = Field(
        default_factory=_FrozenList,
        description="Node IDs in the order they would execute",
    )
    required_external_inputs: list[str] = Field(
        default_factory=_FrozenList,
        description="External input names required by the selected execution",
    )
    missing_external_inputs: list[str] = Field(
        default_factory=_FrozenList,
        description="Required external input names not supplied to explain",
    )
    seeded_nodes: list[str] = Field(
        default_factory=_FrozenList,
        description="Selected node IDs whose values were supplied directly",
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
    )

    @field_validator(
        "execution_order",
        "required_external_inputs",
        "missing_external_inputs",
        "seeded_nodes",
    )
    @classmethod
    def freeze_plan_lists(cls, value: list[str]) -> list[str]:
        return _FrozenList(value)


class Dag(BaseModel):
    """A validated directed acyclic graph of registered operations.

    Unnamed DAGs resolve global operations. Named DAGs resolve operations from
    their matching registry scope only.
    """

    nodes: list[Node]
    name: str | None = Field(
        default=None, description="Optional registry scope for this DAG"
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
    )

    @field_validator("name")
    @classmethod
    def check_name(cls, value: str | None) -> str | None:
        """Reject blank names while allowing unnamed DAGs."""
        if value is not None and not value.strip():
            raise ValueError("DAG name must be non-empty")
        return value

    @field_validator("nodes")
    @classmethod
    def freeze_collections(cls, value: list) -> list:
        return _FrozenList(value)

    @cached_property
    def _nodes_by_id(self) -> dict[str, Node]:
        return _FrozenDict({node.node_id: node for node in self.nodes})

    @model_validator(mode="after")
    def check_duplicate_node_ids(self) -> "Dag":
        """Reject repeated node IDs."""
        node_ids = [node.node_id for node in self.nodes]
        if len(node_ids) != len(set(node_ids)):
            raise ValueError("Node IDs must be unique")
        return self

    @model_validator(mode="after")
    def check_acyclic(self) -> "Dag":
        """Reject cycles among node dependencies while allowing external inputs."""
        node_ids = {node.node_id for node in self.nodes}
        internal_nodes = [
            node.model_copy(
                update={
                    "inputs": [
                        input_id for input_id in node.inputs if input_id in node_ids
                    ]
                }
            )
            for node in self.nodes
        ]
        self.topological_sort(internal_nodes)
        return self

    @model_validator(mode="after")
    def check_operations_are_registered(self) -> "Dag":
        """Ensure every operation exists in this DAG's registry scope."""
        registered_functions = (
            get_registered_nodes()
            if self.name is None
            else get_registered_nodes(self.name)
        )
        function_not_found = [
            node.node_id
            for node in self.nodes
            if node.operation not in registered_functions
        ]
        if function_not_found:
            raise ValueError(
                f"Operations reference non-existent functions: {sorted(function_not_found)}"
            )
        return self

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "Dag":
        """Parse and validate one DAG from a YAML document."""
        data = _load_yaml(yaml_str)
        return cls.model_validate(data)

    @classmethod
    def from_dict(cls, data: dict) -> "Dag":
        """Validate one DAG from a dictionary."""
        return cls.model_validate(data)

    def to_dict(self) -> dict[str, Any]:
        """Return this DAG's configuration as a plain dictionary."""
        return self.model_dump(mode="python", exclude_unset=True)

    @classmethod
    def topological_sort(cls, nodes: list[Node]) -> list[str]:
        """Return node IDs in dependency order using Kahn's linear-time algorithm."""
        node_ids = [node.node_id for node in nodes]
        if len(node_ids) != len(set(node_ids)):
            raise ValueError("Node IDs must be unique")

        known_ids = set(node_ids)
        missing_inputs = {
            input_id
            for node in nodes
            for input_id in node.inputs
            if input_id not in known_ids
        }
        if missing_inputs:
            raise ValueError(
                f"Inputs reference non-existent nodes: {sorted(missing_inputs)}"
            )

        in_degree = {}
        dependents = {node_id: [] for node_id in node_ids}
        for node in nodes:
            dependencies = set(node.inputs) & known_ids
            in_degree[node.node_id] = len(dependencies)
            for dependency in dict.fromkeys(node.inputs):
                if dependency not in dependencies:
                    continue
                dependents[dependency].append(node.node_id)

        ready = deque(node.node_id for node in nodes if in_degree[node.node_id] == 0)
        result = []
        while ready:
            node_id = ready.popleft()
            result.append(node_id)
            for dependent in dependents[node_id]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    ready.append(dependent)

        if len(result) != len(nodes):
            raise ValueError("DAG contains a cycle")

        return result

    def dependencies(self, node_id: str) -> tuple[str, ...]:
        """Return the declared direct inputs for a node, including external ones."""
        try:
            return tuple(self._nodes_by_id[node_id].inputs)
        except KeyError as error:
            raise ValueError(f"Unknown node ID: {node_id!r}") from error

    def topological_order(self) -> tuple[str, ...]:
        """Return all node IDs in deterministic dependency order."""
        node_ids = self._nodes_by_id.keys()
        internal_nodes = [
            node.model_copy(
                update={
                    "inputs": [
                        input_id for input_id in node.inputs if input_id in node_ids
                    ]
                }
            )
            for node in self.nodes
        ]
        return tuple(self.topological_sort(internal_nodes))

    def descendants(self, node_id: str) -> tuple[str, ...]:
        """Return all transitive dependents in topological order."""
        if node_id not in self._nodes_by_id:
            raise ValueError(f"Unknown node ID: {node_id!r}")
        dependents = {candidate: [] for candidate in self._nodes_by_id}
        for node in self.nodes:
            for dependency in dict.fromkeys(node.inputs):
                if dependency in dependents:
                    dependents[dependency].append(node.node_id)

        found = set()
        pending = list(dependents[node_id])
        while pending:
            dependent = pending.pop()
            if dependent not in found:
                found.add(dependent)
                pending.extend(dependents[dependent])
        return tuple(
            candidate for candidate in self.topological_order() if candidate in found
        )

    def explain(
        self,
        outputs: list[str] | None = None,
        **inputs: Any,
    ) -> DagExecutionPlan:
        """Describe a selected execution without running any operations."""
        nodes_by_id = self._nodes_by_id
        node_ids = nodes_by_id.keys()
        external_input_ids = {
            input_id
            for node in self.nodes
            for input_id in node.inputs
            if input_id not in node_ids
        }
        selected_outputs = list(nodes_by_id) if outputs is None else outputs
        unknown_outputs = set(selected_outputs) - node_ids
        if unknown_outputs:
            raise ValueError(
                f"Outputs reference non-existent nodes: {sorted(unknown_outputs)}"
            )
        unknown_inputs = inputs.keys() - node_ids - external_input_ids
        if unknown_inputs:
            raise ValueError(
                f"Inputs are not declared by this DAG: {sorted(unknown_inputs)}"
            )

        required_node_ids = set()
        required_external_input_ids = set()
        required_nodes = []
        pending_node_ids = list(selected_outputs)
        while pending_node_ids:
            node_id = pending_node_ids.pop()
            if node_id not in required_node_ids:
                required_node_ids.add(node_id)
                node = nodes_by_id[node_id]
                required_nodes.append(node)
                if node_id not in inputs:
                    for input_id in node.inputs:
                        if input_id in nodes_by_id:
                            pending_node_ids.append(input_id)
                        else:
                            required_external_input_ids.add(input_id)

        nodes_for_sort = [
            node.model_copy(
                update={
                    "inputs": [
                        input_id
                        for input_id in node.inputs
                        if node.node_id not in inputs and input_id in required_node_ids
                    ]
                }
            )
            for node in required_nodes
        ]
        execution_order = self.topological_sort(nodes_for_sort)
        required_external_inputs = sorted(required_external_input_ids)
        missing_external_inputs = [
            input_id for input_id in required_external_inputs if input_id not in inputs
        ]
        seeded_nodes = [node_id for node_id in execution_order if node_id in inputs]
        return DagExecutionPlan(
            execution_order=execution_order,
            required_external_inputs=required_external_inputs,
            missing_external_inputs=missing_external_inputs,
            seeded_nodes=seeded_nodes,
        )

    def run(
        self,
        outputs: list[str] | None = None,
        listeners: list[Callable[[NodeExecutionResult], None]] | None = None,
        *,
        fail_fast: bool = False,
        validate_types: bool = False,
        **inputs: Any,
    ) -> DagExecutionResult:
        """Execute requested outputs and their transitive dependencies.

        Each operation receives predecessor results and node params as keyword
        arguments. Failed nodes are skipped along with their descendants, while
        independent requested branches continue to run unless ``fail_fast`` is
        true, in which case the original exception is re-raised. Set
        ``validate_types`` to check compatible declared annotations before
        invoking operations; this is a conservative static check, not runtime
        type enforcement. Values passed as keyword arguments seed matching
        nodes and skip their operations. Listener exceptions propagate. If
        outputs is omitted, every node is selected; an empty list selects none.
        """
        plan = self.explain(outputs=outputs, **inputs)
        if plan.missing_external_inputs:
            raise ValueError(
                "Missing values for external inputs: " f"{plan.missing_external_inputs}"
            )
        nodes_by_id = self._nodes_by_id
        sorted_node_ids = plan.execution_order
        outcomes = {}
        registered_functions = (
            get_registered_nodes()
            if self.name is None
            else get_registered_nodes(self.name)
        )
        for node_id in sorted_node_ids:
            if node_id in inputs:
                continue
            node = nodes_by_id[node_id]
            function = registered_functions[node.operation]
            argument_names = dict.fromkeys(node.params)
            argument_names.update(dict.fromkeys(node.inputs))
            try:
                operation_signature = signature(function)
                operation_signature.bind(**argument_names)
            except TypeError as error:
                raise ValueError(
                    f"Invalid arguments for node '{node_id}' "
                    f"(operation '{node.operation}'): {error}"
                ) from error

            if validate_types:
                annotations = _function_annotations(function)
                for input_id in node.inputs:
                    expected_type = annotations.get(input_id, Signature.empty)
                    if expected_type is Signature.empty:
                        continue
                    if input_id in nodes_by_id:
                        if input_id in inputs:
                            produced_type = type(inputs[input_id])
                        else:
                            producer = registered_functions[
                                nodes_by_id[input_id].operation
                            ]
                            produced_type = _function_annotations(producer).get(
                                "return", Signature.empty
                            )
                    elif input_id in inputs:
                        produced_type = type(inputs[input_id])
                    else:
                        continue
                    if not _type_is_compatible(produced_type, expected_type):
                        raise ValueError(
                            f"Incompatible type for input '{input_id}' of node "
                            f"'{node_id}': producer declares {produced_type!r}, "
                            f"consumer expects {expected_type!r}"
                        )

        def notify_listeners(outcome: NodeExecutionResult) -> NodeExecutionResult:
            if listeners is not None:
                for listener in listeners:
                    listener(outcome)
            return outcome

        for node_id in sorted_node_ids:
            node = nodes_by_id[node_id]
            if node_id in inputs:
                outcomes[node_id] = notify_listeners(
                    SuccessfulNodeExecutionResult(
                        node_id=node_id, dag_name=self.name, value=inputs[node_id]
                    )
                )
                continue
            if any(
                input_id in outcomes
                and isinstance(
                    outcomes[input_id],
                    (ErrorNodeExecutionResult, SkippedNodeExecutionResult),
                )
                for input_id in node.inputs
            ):
                outcomes[node_id] = notify_listeners(
                    SkippedNodeExecutionResult(node_id=node_id, dag_name=self.name)
                )
                continue

            node_inputs = dict(node.params)
            node_inputs.update(
                {
                    input_id: (
                        outcomes[input_id].value
                        if input_id in nodes_by_id
                        else inputs[input_id]
                    )
                    for input_id in node.inputs
                }
            )
            try:
                result = registered_functions[node.operation](**node_inputs)
            except Exception as error:
                outcomes[node_id] = notify_listeners(
                    ErrorNodeExecutionResult(
                        node_id=node_id, dag_name=self.name, error=error
                    )
                )
                if fail_fast:
                    raise
            else:
                outcomes[node_id] = notify_listeners(
                    SuccessfulNodeExecutionResult(
                        node_id=node_id, dag_name=self.name, value=result
                    )
                )

        return DagExecutionResult(
            status=all(outcome.result for outcome in outcomes.values()),
            results=outcomes,
        )
