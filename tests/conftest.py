import pytest

import lightweight_dag.dag as dag_module


@pytest.fixture
def registered_operations(monkeypatch):
    def read_orders(source):
        assert source == "local"
        return [
            {"date": "2026-10-01", "amount": 80},
            {"date": "2026-10-01", "amount": 25},
            {"date": "2026-10-02", "amount": 120},
        ]

    def filter_orders(orders, minimum_amount):
        return [order for order in orders if order["amount"] >= minimum_amount]

    def summarize_sales(valid_orders, group_by):
        totals_by_group = {}
        for order in valid_orders:
            key = order[group_by]
            totals_by_group[key] = totals_by_group.get(key, 0) + order["amount"]

        return {
            "order_count": len(valid_orders),
            "total_amount": sum(order["amount"] for order in valid_orders),
            "totals_by_group": totals_by_group,
        }

    operations = {
        "read_orders": read_orders,
        "filter_orders": filter_orders,
        "summarize_sales": summarize_sales,
    }
    monkeypatch.setattr(dag_module, "get_registered_nodes", lambda: operations)
    return operations
