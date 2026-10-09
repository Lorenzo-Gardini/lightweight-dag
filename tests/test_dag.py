from pathlib import Path

import pytest
from pydantic import ValidationError
from yaml.constructor import ConstructorError

import lightweight_dag.dag as dag_module
from lightweight_dag import DagSuite, dag_node, node_registry
from lightweight_dag.dag import Dag, DagExecutionResult, Node, NodeExecutionResult

FIXTURES_DIRECTORY = Path(__file__).parent / "fixtures"


def test_node_execution_result_accepts_custom_value_types():
    class CustomValue:
        pass

    value = CustomValue()

    event = NodeExecutionResult(
        node_id="custom_output",
        result=True,
        value=value,
    )

    assert event.value is value


def registered_scale_operation(value, multiplier):
    return value * multiplier


def registered_load_value():
    return 14


@pytest.mark.usefixtures("registered_operations")
def test_from_yaml_loads_sales_pipeline():
    yaml_config = (FIXTURES_DIRECTORY / "sales_pipeline.yaml").read_text()

    dag = Dag.from_yaml(yaml_config)

    assert [node.node_id for node in dag.nodes] == [
        "daily_sales",
        "valid_orders",
        "orders",
    ]
    assert dag.nodes[0].params == {"group_by": "date"}
    assert dag.nodes[1].inputs == ["orders"]
    assert dag.topological_sort(dag.nodes) == [
        "orders",
        "valid_orders",
        "daily_sales",
    ]


@pytest.mark.parametrize(
    ("fixture_name", "error_message"),
    [
        ("duplicate_node_ids.yaml", "Node IDs must be unique"),
        ("cyclic_pipeline.yaml", "DAG contains a cycle"),
        (
            "unknown_operation.yaml",
            "Operations reference non-existent functions",
        ),
    ],
)
@pytest.mark.usefixtures("registered_operations")
def test_from_yaml_rejects_invalid_pipelines(fixture_name, error_message):
    yaml_config = (FIXTURES_DIRECTORY / fixture_name).read_text()

    with pytest.raises(ValidationError, match=error_message):
        Dag.from_yaml(yaml_config)


@pytest.mark.usefixtures("registered_operations")
def test_from_yaml_defers_external_input_validation_until_run():
    yaml_config = (FIXTURES_DIRECTORY / "missing_input.yaml").read_text()

    dag = Dag.from_yaml(yaml_config)

    with pytest.raises(ValueError, match="Missing values for external inputs"):
        dag.run()


@pytest.mark.usefixtures("registered_operations")
def test_from_dict_loads_a_dag():
    dag = Dag.from_dict(
        {
            "nodes": [
                {
                    "node_id": "orders",
                    "operation": "read_orders",
                    "params": {"source": "memory"},
                }
            ]
        }
    )

    assert dag.name is None
    assert len(dag.nodes) == 1
    assert dag.nodes[0].node_id == "orders"
    assert dag.nodes[0].operation == "read_orders"
    assert dag.nodes[0].inputs == []
    assert dag.nodes[0].params == {"source": "memory"}


def test_dag_rejects_blank_name(monkeypatch):
    monkeypatch.setattr("lightweight_dag.dag.get_registered_nodes", lambda name: {})

    with pytest.raises(ValidationError, match="DAG name must be non-empty"):
        Dag.model_validate({"name": "  ", "nodes": []})


def test_dag_rejects_unknown_fields_instead_of_ignoring_name_typos():
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        Dag.from_dict({"nam": "report", "nodes": []})


def test_dag_rejects_external_inputs_field():
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        Dag.model_validate(
            {
                "inputs": ["source"],
                "nodes": [{"node_id": "result", "operation": "transform"}],
            }
        )


@pytest.mark.parametrize(
    ("loader", "yaml_config"),
    [
        (Dag.from_yaml, "nodes: []\nnodes: []\n"),
        (
            DagSuite.from_yaml,
            "report:\n  nodes: []\nreport:\n  nodes: []\n",
        ),
    ],
)
def test_yaml_loaders_reject_duplicate_mapping_keys(loader, yaml_config):
    with pytest.raises(ConstructorError, match="found duplicate key"):
        loader(yaml_config)


@pytest.mark.usefixtures("registered_operations")
def test_dag_model_collections_cannot_be_mutated():
    node = Node(
        node_id="orders",
        operation="read_orders",
        inputs=[],
        params={"path": "orders.csv"},
    )
    dag = Dag(nodes=[node])
    execution_result = DagExecutionResult(status=True, results={"answer": 42})
    suite = DagSuite.from_dict({})

    with pytest.raises(TypeError, match="immutable"):
        node.inputs.append("other")
    with pytest.raises(TypeError, match="immutable"):
        node.params.update({"path": "other.csv"})
    with pytest.raises(TypeError, match="immutable"):
        dag.nodes.append(node)
    with pytest.raises(TypeError, match="immutable"):
        execution_result.results.update({"answer": 0})
    with pytest.raises(TypeError, match="immutable"):
        suite.dags["other"] = dag


def test_named_dag_uses_its_scoped_operation(monkeypatch):
    def global_operation():
        return "global"

    def report_operation():
        return "report"

    registries = {
        None: {"transform": global_operation},
        "report": {"transform": report_operation},
    }
    monkeypatch.setattr(
        dag_module,
        "get_registered_nodes",
        lambda dag_name=None: registries.get(dag_name, {}),
    )
    dag = Dag.from_dict(
        {"name": "report", "nodes": [{"node_id": "result", "operation": "transform"}]}
    )

    events = []
    result = dag.run(outputs=["result"], listeners=[events.append])

    assert result.results["result"] == "report"
    assert events[0].dag_name == "report"


def test_unnamed_dag_uses_global_operation(monkeypatch):
    def global_operation():
        return "global"

    monkeypatch.setattr(
        dag_module,
        "get_registered_nodes",
        lambda dag_name=None: (
            {"transform": global_operation} if dag_name is None else {}
        ),
    )
    dag = Dag.from_dict({"nodes": [{"node_id": "result", "operation": "transform"}]})

    result = dag.run(outputs=["result"])

    assert result.results["result"] == "global"


def test_named_dag_does_not_fall_back_to_global_operation(monkeypatch):
    def global_operation():
        return "global"

    monkeypatch.setattr(
        dag_module,
        "get_registered_nodes",
        lambda dag_name=None: (
            {"transform": global_operation} if dag_name is None else {}
        ),
    )

    with pytest.raises(ValidationError, match="Operations reference non-existent"):
        Dag.from_dict(
            {
                "name": "report",
                "nodes": [{"node_id": "result", "operation": "transform"}],
            }
        )


def test_registered_decorator_runs_through_real_dag_registry(monkeypatch):
    monkeypatch.setattr(node_registry, "_nodes", {})
    dag_node("scale_test", "scale")(registered_scale_operation)
    dag_node("scale_test", "load_value")(registered_load_value)
    dag = Dag.from_dict(
        {
            "name": "scale_test",
            "nodes": [
                {"node_id": "value", "operation": "load_value"},
                {
                    "node_id": "scaled",
                    "operation": "scale",
                    "inputs": ["value"],
                    "params": {"multiplier": 3},
                },
            ],
        }
    )

    result = dag.run(outputs=["scaled"])

    assert result.status is True
    assert result.results["scaled"] == 42


def test_dag_suite_indexes_named_dags_from_yaml(monkeypatch):
    def report_operation():
        return "report"

    def audit_operation():
        return "audit"

    registries = {
        "report": {"transform": report_operation},
        "audit": {"transform": audit_operation},
    }
    monkeypatch.setattr(
        dag_module,
        "get_registered_nodes",
        lambda dag_name=None: registries.get(dag_name, {}),
    )
    suite = DagSuite.from_yaml("""
report:
  nodes:
    - node_id: result
      operation: transform
audit:
  nodes:
    - node_id: result
      operation: transform
""")

    assert suite["report"].name == "report"
    assert suite["report"].run(outputs=["result"]).results["result"] == ("report")
    assert suite["audit"].run(outputs=["result"]).results["result"] == ("audit")
    with pytest.raises(KeyError):
        suite["missing"]


def test_dag_suite_rejects_non_mapping_input():
    with pytest.raises(TypeError, match="mapping of names to DAG definitions"):
        DagSuite.from_dict(None)


def test_dag_suite_rejects_name_that_differs_from_suite_key():
    with pytest.raises(ValueError, match="does not match suite key"):
        DagSuite.from_dict({"report": {"name": "audit", "nodes": []}})


def test_dag_suite_accepts_already_named_dags():
    dag = Dag(name="report", nodes=[])

    suite = DagSuite.from_dict({"report": dag})

    assert suite["report"] is dag


def test_dag_suite_rejects_dag_instance_with_different_name():
    dag = Dag(name="audit", nodes=[])

    with pytest.raises(ValueError, match="does not match suite key"):
        DagSuite.from_dict({"report": dag})


def test_dag_suite_model_rejects_dag_name_mismatched_with_key():
    dag = Dag(name="audit", nodes=[])

    with pytest.raises(ValidationError, match="Suite keys must match DAG names"):
        DagSuite(dags={"report": dag})


@pytest.mark.parametrize(
    ("data", "error_type"),
    [
        ([], TypeError),
        ({1: {}}, TypeError),
        ({"": {}}, ValueError),
        ({"report": None}, TypeError),
    ],
)
def test_dag_suite_rejects_invalid_names_and_definitions(data, error_type):
    with pytest.raises(error_type):
        DagSuite.from_dict(data)


def test_from_yaml_rejects_empty_document():
    with pytest.raises(ValidationError):
        Dag.from_yaml("")


def test_node_rejects_unexpected_fields():
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        Node(node_id="orders", operation="read_orders", unexpected=True)


def test_node_input_overrides_param_with_the_same_name(monkeypatch):
    received_orders = []

    def load_orders():
        return ["computed"]

    def filter_orders(orders):
        received_orders.append(orders)
        return orders

    monkeypatch.setattr(
        "lightweight_dag.dag.get_registered_nodes",
        lambda: {"load_orders": load_orders, "filter_orders": filter_orders},
    )
    dag = Dag.from_dict(
        {
            "nodes": [
                {"node_id": "orders", "operation": "load_orders"},
                {
                    "node_id": "filtered_orders",
                    "operation": "filter_orders",
                    "inputs": ["orders"],
                    "params": {"orders": "configured"},
                },
            ]
        }
    )

    result = dag.run(outputs=["filtered_orders"])

    assert result.status is True
    assert result.results["filtered_orders"] == ["computed"]
    assert received_orders == [["computed"]]


def test_topological_sort_handles_nodes_declared_out_of_order():
    nodes = [
        Node(node_id="report", operation="summarize_sales", inputs=["filtered"]),
        Node(node_id="filtered", operation="filter_orders", inputs=["orders"]),
        Node(node_id="orders", operation="read_orders"),
    ]

    assert Dag.topological_sort(nodes) == ["orders", "filtered", "report"]


@pytest.mark.parametrize(
    ("nodes", "error_message"),
    [
        (
            [
                Node(node_id="duplicate", operation="operation"),
                Node(node_id="duplicate", operation="operation"),
            ],
            "Node IDs must be unique",
        ),
        (
            [Node(node_id="consumer", operation="operation", inputs=["missing"])],
            "Inputs reference non-existent nodes",
        ),
        (
            [Node(node_id="self", operation="operation", inputs=["self"])],
            "DAG contains a cycle",
        ),
    ],
)
def test_topological_sort_rejects_invalid_node_relationships(nodes, error_message):
    with pytest.raises(ValueError, match=error_message):
        Dag.topological_sort(nodes)


def test_dag_rejects_self_dependency():
    with pytest.raises(ValidationError, match="DAG contains a cycle"):
        Dag.from_dict(
            {
                "nodes": [
                    {
                        "node_id": "orders",
                        "operation": "read_orders",
                        "inputs": ["orders"],
                    }
                ]
            }
        )


@pytest.mark.usefixtures("registered_operations")
def test_run_executes_sales_pipeline():
    yaml_config = (FIXTURES_DIRECTORY / "sales_pipeline.yaml").read_text()
    dag = Dag.from_yaml(yaml_config)

    result = dag.run(outputs=["daily_sales"])

    assert result.status is True
    assert result.results["daily_sales"] == {
        "order_count": 2,
        "total_amount": 200,
        "totals_by_group": {"2026-10-01": 80, "2026-10-02": 120},
    }


@pytest.mark.usefixtures("registered_operations")
def test_run_without_outputs_executes_all_nodes():
    yaml_config = (FIXTURES_DIRECTORY / "sales_pipeline.yaml").read_text()
    dag = Dag.from_yaml(yaml_config)

    result = dag.run()

    assert result.status is True
    assert set(result.results) == {"orders", "valid_orders", "daily_sales"}
    assert result.results["daily_sales"]["order_count"] == 2


def test_run_executes_only_requested_outputs_and_their_dependencies(monkeypatch):
    executed_nodes = []

    def record_execution(**node_inputs):
        executed_nodes.append(tuple(node_inputs))
        return len(executed_nodes)

    monkeypatch.setattr(
        "lightweight_dag.dag.get_registered_nodes",
        lambda: {"operation": record_execution},
    )
    dag = Dag.from_dict(
        {
            "nodes": [
                {"node_id": "E", "operation": "operation", "inputs": ["D"]},
                {"node_id": "D", "operation": "operation", "inputs": ["C"]},
                {"node_id": "C", "operation": "operation", "inputs": ["B"]},
                {"node_id": "B", "operation": "operation", "inputs": ["A"]},
                {"node_id": "side_branch", "operation": "operation", "inputs": ["A"]},
                {"node_id": "A", "operation": "operation"},
            ]
        }
    )

    result = dag.run(outputs=["D", "B"])

    assert result.status is True
    assert set(result.results) == {"A", "B", "C", "D"}
    assert executed_nodes == [(), ("A",), ("B",), ("C",)]


def test_run_selects_only_required_paths_in_branching_pipeline(monkeypatch):
    received_inputs = {}

    def record(label, **node_inputs):
        received_inputs[label] = node_inputs
        return label

    monkeypatch.setattr(
        "lightweight_dag.dag.get_registered_nodes",
        lambda: {"record": record},
    )
    yaml_config = (FIXTURES_DIRECTORY / "branching_pipeline.yaml").read_text()
    dag = Dag.from_yaml(yaml_config)

    result = dag.run(outputs=["G", "J", "A"])

    assert result.status is True
    assert result.results == {
        "A": "A",
        "B": "B",
        "E": "E",
        "F": "F",
        "G": "G",
        "J": "J",
    }
    assert received_inputs == {
        "A": {},
        "B": {"A": "A"},
        "E": {"B": "B"},
        "F": {"B": "B"},
        "G": {"E": "E"},
        "J": {"F": "F"},
    }


def test_run_merges_all_predecessors_and_executes_shared_dependencies_once(
    monkeypatch,
):
    calls_by_node = {}

    def record(label, **node_inputs):
        calls_by_node[label] = node_inputs
        return label

    monkeypatch.setattr(
        "lightweight_dag.dag.get_registered_nodes",
        lambda: {"record": record},
    )
    yaml_config = (FIXTURES_DIRECTORY / "branching_pipeline.yaml").read_text()
    dag = Dag.from_yaml(yaml_config)

    result = dag.run(outputs=["K", "N"])

    assert result.status is True
    assert calls_by_node["K"] == {"G": "G", "H": "H"}
    assert calls_by_node["N"] == {"I": "I", "J": "J", "G": "G"}
    assert set(calls_by_node) == {"A", "B", "E", "F", "G", "H", "I", "J", "K", "N"}


def test_run_deduplicates_requested_outputs(monkeypatch):
    executed = []

    def operation():
        executed.append("called")
        return "result"

    monkeypatch.setattr(
        "lightweight_dag.dag.get_registered_nodes",
        lambda: {"operation": operation},
    )
    dag = Dag.from_dict({"nodes": [{"node_id": "A", "operation": "operation"}]})

    result = dag.run(outputs=["A", "A"])

    assert result.results == {"A": "result"}
    assert executed == ["called"]


def test_run_rejects_unknown_outputs():
    dag = Dag.from_dict({"nodes": []})

    with pytest.raises(ValueError, match="Outputs reference non-existent nodes"):
        dag.run(outputs=["missing"])


def test_run_with_no_outputs_does_not_execute_nodes(monkeypatch):
    executed = []
    monkeypatch.setattr(
        "lightweight_dag.dag.get_registered_nodes",
        lambda: {"operation": lambda: executed.append(True)},
    )
    dag = Dag.from_dict({"nodes": [{"node_id": "A", "operation": "operation"}]})

    result = dag.run(outputs=[])

    assert result.status is True
    assert result.results == {}
    assert executed == []


def test_run_passes_node_params_to_registered_operation(monkeypatch):
    def load_source(source_name):
        return source_name

    def scale_value(raw_value, multiplier):
        return raw_value * multiplier

    monkeypatch.setattr(
        "lightweight_dag.dag.get_registered_nodes",
        lambda: {"load_source": load_source, "scale_value": scale_value},
    )
    dag = Dag.from_dict(
        {
            "nodes": [
                {
                    "node_id": "scaled_value",
                    "operation": "scale_value",
                    "inputs": ["raw_value"],
                    "params": {"multiplier": 3},
                },
                {
                    "node_id": "raw_value",
                    "operation": "load_source",
                    "params": {"source_name": 14},
                },
            ]
        }
    )

    result = dag.run(outputs=["scaled_value"])

    assert result.status is True
    assert result.results["scaled_value"] == 42


def test_run_uses_provided_node_values_instead_of_executing_them(monkeypatch):
    executed = []

    def load_value():
        executed.append("load")
        return 1

    def scale_value(value, multiplier):
        executed.append("scale")
        return value * multiplier

    monkeypatch.setattr(
        "lightweight_dag.dag.get_registered_nodes",
        lambda: {"load_value": load_value, "scale_value": scale_value},
    )
    dag = Dag.from_dict(
        {
            "nodes": [
                {"node_id": "value", "operation": "load_value"},
                {
                    "node_id": "scaled",
                    "operation": "scale_value",
                    "inputs": ["value"],
                    "params": {"multiplier": 3},
                },
            ]
        }
    )

    events = []
    result = dag.run(value=14, outputs=["scaled"], listeners=[events.append])

    assert result.status is True
    assert result.results == {"value": 14, "scaled": 42}
    assert executed == ["scale"]
    assert [(event.node_id, event.result, event.value) for event in events] == [
        ("value", True, 14),
        ("scaled", True, 42),
    ]


def test_run_passes_external_inputs_alongside_node_inputs(monkeypatch):
    executed_nodes = []

    def load_elem1():
        executed_nodes.append("elem1")
        return 2

    def combine(elem1, elem2):
        executed_nodes.append("combined")
        return elem1 + elem2

    monkeypatch.setattr(
        "lightweight_dag.dag.get_registered_nodes",
        lambda: {"load_elem1": load_elem1, "combine": combine},
    )
    dag = Dag.from_dict(
        {
            "nodes": [
                {"node_id": "elem1", "operation": "load_elem1"},
                {
                    "node_id": "combined",
                    "operation": "combine",
                    "inputs": ["elem1", "elem2"],
                },
            ]
        }
    )
    events = []

    result = dag.run(elem2=5, outputs=["combined"], listeners=[events.append])

    assert result.status is True
    assert result.results == {"elem1": 2, "combined": 7}
    assert executed_nodes == ["elem1", "combined"]
    assert [(event.node_id, event.result, event.value) for event in events] == [
        ("elem1", True, 2),
        ("combined", True, 7),
    ]


def test_run_does_not_require_external_inputs_on_unselected_paths(monkeypatch):
    monkeypatch.setattr(
        "lightweight_dag.dag.get_registered_nodes",
        lambda: {
            "combine": lambda raw: raw,
            "independent": lambda: "ready",
        },
    )
    dag = Dag.from_dict(
        {
            "nodes": [
                {
                    "node_id": "external_branch",
                    "operation": "combine",
                    "inputs": ["raw"],
                },
                {"node_id": "independent", "operation": "independent"},
            ]
        }
    )

    result = dag.run(outputs=["independent"])

    assert result.status is True
    assert result.results == {"independent": "ready"}


def test_run_seeded_node_skips_its_ancestors_and_notifies_listeners(monkeypatch):
    executed_nodes = []

    def upstream():
        executed_nodes.append("upstream")
        return 7

    def create_seeded_value(upstream_value):
        executed_nodes.append("seeded")
        return upstream_value * 2

    def scale_value(seeded, multiplier):
        executed_nodes.append("scaled")
        return seeded * multiplier

    monkeypatch.setattr(
        "lightweight_dag.dag.get_registered_nodes",
        lambda: {
            "upstream": upstream,
            "create_seeded_value": create_seeded_value,
            "scale_value": scale_value,
        },
    )
    dag = Dag.from_dict(
        {
            "nodes": [
                {"node_id": "upstream", "operation": "upstream"},
                {
                    "node_id": "seeded",
                    "operation": "create_seeded_value",
                    "inputs": ["upstream"],
                },
                {
                    "node_id": "scaled",
                    "operation": "scale_value",
                    "inputs": ["seeded"],
                    "params": {"multiplier": 3},
                },
            ]
        }
    )
    events = []

    result = dag.run(seeded=14, outputs=["scaled"], listeners=[events.append])

    assert result.status is True
    assert result.results == {"seeded": 14, "scaled": 42}
    assert executed_nodes == ["scaled"]
    assert [(event.node_id, event.result, event.value) for event in events] == [
        ("seeded", True, 14),
        ("scaled", True, 42),
    ]


def test_run_propagates_listener_exceptions_after_node_completion(monkeypatch):
    def operation():
        return "completed"

    monkeypatch.setattr(
        "lightweight_dag.dag.get_registered_nodes",
        lambda: {"operation": operation},
    )
    dag = Dag.from_dict({"nodes": [{"node_id": "output", "operation": "operation"}]})
    events = []

    def failing_listener(event):
        events.append(event)
        raise RuntimeError("listener failed")

    with pytest.raises(RuntimeError, match="listener failed"):
        dag.run(listeners=[failing_listener])

    assert len(events) == 1
    assert events[0].node_id == "output"
    assert events[0].result is True
    assert events[0].value == "completed"


def test_run_rejects_inputs_that_are_not_node_ids():
    dag = Dag.from_dict({"nodes": []})

    with pytest.raises(ValueError, match="Inputs are not declared by this DAG"):
        dag.run(source=14)


def test_run_keeps_independent_branch_results_when_another_branch_fails(
    monkeypatch,
):
    executed = []

    def fail():
        raise RuntimeError("operation failed")

    def dependent(failed_node):
        executed.append("dependent")
        return failed_node

    def independent():
        executed.append("independent")
        return "available"

    monkeypatch.setattr(
        "lightweight_dag.dag.get_registered_nodes",
        lambda: {
            "fail": fail,
            "dependent": dependent,
            "independent": independent,
        },
    )
    dag = Dag.from_dict(
        {
            "nodes": [
                {
                    "node_id": "failed_output",
                    "operation": "dependent",
                    "inputs": ["failed_node"],
                },
                {"node_id": "failed_node", "operation": "fail"},
                {"node_id": "independent_output", "operation": "independent"},
            ]
        }
    )

    events = []
    result = dag.run(
        outputs=["failed_output", "independent_output"], listeners=[events.append]
    )

    assert result.status is False
    assert result.results["failed_node"] is None
    assert result.results["failed_output"] is None
    assert result.results["independent_output"] == "available"
    assert executed == ["independent"]
    assert {event.node_id: (event.result, event.value) for event in events} == {
        "failed_node": (False, None),
        "failed_output": (False, None),
        "independent_output": (True, "available"),
    }


def test_run_reports_failure_when_an_operation_raises(registered_operations):
    def fail_filter_orders(orders, minimum_amount):
        raise RuntimeError("Could not read order records")

    registered_operations["filter_orders"] = fail_filter_orders
    yaml_config = (FIXTURES_DIRECTORY / "sales_pipeline.yaml").read_text()
    dag = Dag.from_yaml(yaml_config)

    result = dag.run(outputs=["daily_sales"])

    assert result.status is False
    assert result.results["valid_orders"] is None
