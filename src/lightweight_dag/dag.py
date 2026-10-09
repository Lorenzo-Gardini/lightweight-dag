"""Typed DAG models, graph validation, execution, and named DAG suites."""

from collections import deque
from functools import cached_property
from typing import Any

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
        default_factory=list, description="List of input node IDs"
    )
    params: dict[str, int | float | str] = Field(
        default_factory=dict, description="Dictionary of parameters for this node"
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


class DagExecutionResult(BaseModel):
    """Execution status and values produced or retained during a DAG run."""

    status: bool
    results: dict[str, Any] = Field(
        default_factory=dict, description="Dictionary of results for this DAG execution"
    )


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
    def check_inputs_exist(self) -> "Dag":
        """Ensure node inputs refer to existing nodes."""
        node_ids = {node.node_id for node in self.nodes}
        missing_inputs = {
            input_id
            for node in self.nodes
            for input_id in node.inputs
            if input_id not in node_ids
        }
        if missing_inputs:
            raise ValueError(
                f"Inputs reference non-existent nodes: {sorted(missing_inputs)}"
            )
        return self

    @model_validator(mode="after")
    def check_input_parameter_conflicts(self) -> "Dag":
        """Reject keyword names that collide between inputs and params."""
        conflicts = {
            node.node_id: sorted(set(node.inputs) & node.params.keys())
            for node in self.nodes
            if set(node.inputs) & node.params.keys()
        }
        if conflicts:
            raise ValueError(f"Node inputs conflict with parameters: {conflicts}")
        return self

    @model_validator(mode="after")
    def check_acyclic(self) -> "Dag":
        """Reject dependency cycles."""
        self.topological_sort(self.nodes)
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
            for dependency in dependencies:
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

    def run(self, outputs: list[str] | None = None) -> DagExecutionResult:
        """Execute requested outputs and their transitive dependencies.

        Each operation receives predecessor results and node params as keyword
        arguments. Failed nodes are skipped along with their descendants, while
        independent requested branches continue to run. If outputs is omitted,
        every node is selected; an empty list selects no nodes.
        """
        nodes_by_id = self._nodes_by_id
        selected_outputs = list(nodes_by_id) if outputs is None else outputs
        unknown_outputs = set(selected_outputs) - nodes_by_id.keys()
        if unknown_outputs:
            raise ValueError(
                f"Outputs reference non-existent nodes: {sorted(unknown_outputs)}"
            )

        required_node_ids = set()
        required_nodes = []
        pending_node_ids = list(selected_outputs)
        while pending_node_ids:
            node_id = pending_node_ids.pop()
            if node_id not in required_node_ids:
                required_node_ids.add(node_id)
                node = nodes_by_id[node_id]
                required_nodes.append(node)
                pending_node_ids.extend(node.inputs)
        sorted_node_ids = [node_id for node_id in self.topological_sort(required_nodes)]
        context = {}
        is_run_successful = True
        failed_node_ids = set()
        registered_functions = (
            get_registered_nodes()
            if self.name is None
            else get_registered_nodes(self.name)
        )

        for node_id in sorted_node_ids:
            node = nodes_by_id[node_id]
            if any(input_id in failed_node_ids for input_id in node.inputs):
                context[node_id] = None
                failed_node_ids.add(node_id)
                is_run_successful = False
                continue

            node_inputs = {input_id: context[input_id] for input_id in node.inputs}
            node_inputs.update(node.params)
            try:
                result = registered_functions[node.operation](**node_inputs)
            except Exception:
                context[node_id] = None
                failed_node_ids.add(node_id)
                is_run_successful = False
            else:
                context[node_id] = result

        return DagExecutionResult(status=is_run_successful, results=context)
