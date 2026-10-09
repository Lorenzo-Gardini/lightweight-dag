"""Named DAG suite loading and validation."""

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from lightweight_dag.dag import Dag, _FrozenDict, _load_yaml


class DagSuite(BaseModel):
    """A name-indexed collection of DAGs, accessible with ``suite[name]``."""

    dags: dict[str, Dag]

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
    )

    @field_validator("dags")
    @classmethod
    def freeze_dags(cls, value: dict[str, Dag]) -> dict[str, Dag]:
        return _FrozenDict(value)

    @model_validator(mode="after")
    def check_dag_names_match_keys(self) -> "DagSuite":
        """Ensure every suite key matches the corresponding DAG name."""
        mismatched_names = {
            suite_name: dag.name
            for suite_name, dag in self.dags.items()
            if suite_name != dag.name
        }
        if mismatched_names:
            raise ValueError(f"Suite keys must match DAG names: {mismatched_names}")
        return self

    def __getitem__(self, name: str) -> Dag:
        """Return the DAG registered under ``name``."""
        return self.dags[name]

    @classmethod
    def from_dict(cls, data: Mapping[str, Mapping[str, Any] | Dag]) -> "DagSuite":
        """Build a suite from a mapping of names to DAG definitions or models."""
        if not isinstance(data, Mapping):
            raise TypeError("DAG suite must be a mapping of names to DAG definitions")

        dags = {}
        for name, dag_data in data.items():
            if not isinstance(name, str):
                raise TypeError("DAG suite names must be strings")
            if not name:
                raise ValueError("DAG suite names must be non-empty strings")

            if isinstance(dag_data, Dag):
                if dag_data.name != name:
                    raise ValueError(
                        f"DAG name '{dag_data.name}' does not match suite key '{name}'"
                    )
                dags[name] = dag_data
                continue

            if not isinstance(dag_data, Mapping):
                raise TypeError(
                    f"DAG definition for '{name}' must be a mapping or a Dag"
                )
            configured_name = dag_data.get("name", name)
            if configured_name != name:
                raise ValueError(
                    f"DAG name '{configured_name}' does not match suite key '{name}'"
                )
            dags[name] = Dag.model_validate({**dag_data, "name": name})

        return cls(dags=dags)

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "DagSuite":
        """Parse a name-to-DAG mapping from YAML and validate each DAG."""
        data = _load_yaml(yaml_str)
        return cls.from_dict(data)
