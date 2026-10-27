import json
from pathlib import Path

import duckdb
import jinja2

ROOT = Path(__file__).parents[1]
MODELS = ROOT / "src/kalshi_events/dbt/models"


def render_sql(name, *, incremental):
    template = jinja2.Environment().from_string((MODELS / f"{name}.sql").read_text())
    variables = {
        "catalogue_version": 2,
        "incremental_update": incremental,
        "current_capture_id": "capture",
    }
    return template.render(
        config=lambda **kwargs: "",
        ref=lambda relation: f"main.{relation}",
        source=lambda schema, relation: f"{schema}.{relation}",
        var=lambda key, default=None: variables.get(key, default),
    )


def render_model(connection, name, *, incremental):
    return connection.execute(render_sql(name, incremental=incremental))


def rows(result):
    return {tuple(row) for row in result.fetchall()}


def explain_json(connection, sql):
    result = connection.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}").fetchone()
    return json.loads(result[1])


def operators(plan, operator_type):
    if isinstance(plan, list):
        return [operator for node in plan for operator in operators(node, operator_type)]
    if not isinstance(plan, dict):
        return []
    found = []
    if plan.get("operator_type", plan.get("type")) == operator_type:
        found.append(plan)
    children = plan.get("children", [])
    nested_operators = plan.get("operator", [])
    for child in [*children, *nested_operators]:
        found.extend(operators(child, operator_type))
    return found


def operator_cardinality(operator):
    if "operator_cardinality" in operator:
        return operator["operator_cardinality"]
    if "intermediate_rows" in operator:
        return operator["intermediate_rows"]
    raise AssertionError("EXPLAIN operator is missing its cardinality metric")


def test_explain_operators_support_duckdb_15_json_shape():
    plan = {
        "operator_type": "QUERY",
        "children": [
            {
                "operator_type": "WINDOW",
                "children": [{"operator_type": "TABLE_SCAN", "operator_cardinality": 4}],
            }
        ],
    }

    window = operators(plan, "WINDOW")[0]

    assert operator_cardinality(window["children"][0]) == 4


def test_explain_operators_support_duckdb_20_json_shape_and_zero_rows():
    plan = {
        "operator": [
            {
                "type": "QUERY",
                "children": [
                    {
                        "type": "WINDOW",
                        "children": [{"type": "TABLE_SCAN", "intermediate_rows": 0}],
                    }
                ],
            }
        ]
    }

    window = operators(plan, "WINDOW")[0]

    assert operator_cardinality(window["children"][0]) == 0


def test_operator_cardinality_fails_when_plan_has_no_cardinality_metric():
    try:
        operator_cardinality({"type": "TABLE_SCAN"})
    except AssertionError as error:
        assert "missing its cardinality metric" in str(error)
    else:
        raise AssertionError("missing EXPLAIN cardinality should fail closed")


def test_incremental_current_projections_match_full_results_and_prune_history_windows():
    connection = duckdb.connect()
    connection.execute("create schema landing")
    connection.execute(
        """create table observation_history (
            entity_kind varchar, entity_ticker varchar, event_ticker varchar,
            series_ticker varchar, observation_id bigint, capture_id varchar,
            received_at timestamp, provider_updated_at varchar, source_endpoint varchar,
            payload_json varchar, source_kind varchar
        )"""
    )
    connection.execute(
        """create table landing.membership (
            entity_kind varchar, entity_ticker varchar, scope varchar
        )"""
    )
    connection.execute(
        """create table landing.parent_eligibility (
            capture_id varchar, entity_kind varchar, entity_ticker varchar, status integer
        )"""
    )

    count = 240
    history = []
    membership = []
    parent_eligibility = []
    for number in range(count):
        market, event, series = f"M{number}", f"E{number}", f"S{number}"
        membership.append(("markets", market, "markets_live"))
        if number != 1:
            membership.extend([("events", event, "events_live"), ("series", series, "series")])
        elif number == 1:
            # E1 and S1 are required parents of an unaffected active market.
            parent_eligibility.extend(
                [("capture", "events", event, 200), ("capture", "series", series, 200)]
            )
        for version in range(4):
            history.append(
                (
                    "markets",
                    market,
                    event,
                    None,
                    number * 100 + version,
                    "capture",
                    f"2026-01-01 00:00:0{version}",
                    None,
                    "/markets",
                    json.dumps({"status": "active", "last_price_dollars": str(version)}),
                    "direct",
                )
            )
            history.append(
                (
                    "events",
                    event,
                    None,
                    series,
                    number * 1000 + version,
                    "capture",
                    f"2026-01-01 00:00:0{version}",
                    None,
                    "/events",
                    "{}",
                    "direct",
                )
            )
            history.append(
                (
                    "series",
                    series,
                    None,
                    None,
                    number * 10000 + version,
                    "capture",
                    f"2026-01-01 00:00:0{version}",
                    None,
                    "/series",
                    "{}",
                    "direct",
                )
            )
    connection.executemany(
        "insert into observation_history values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", history
    )
    connection.executemany("insert into landing.membership values (?, ?, ?)", membership)
    connection.executemany(
        "insert into landing.parent_eligibility values (?, ?, ?, ?)", parent_eligibility
    )
    for table, key, affected in (
        ("affected_markets", "market_ticker", "M0"),
        ("affected_events", "event_ticker", "E1"),
        ("affected_series", "series_ticker", "S1"),
    ):
        connection.execute(f"create table landing.{table} ({key} varchar)")
        connection.execute(f"insert into landing.{table} values (?)", [affected])

    # The complete event relation is an input to current_series, as in the project DAG.
    full_events = rows(render_model(connection, "current_events", incremental=False))
    connection.execute(
        f"create table current_events as {render_sql('current_events', incremental=False)}"
    )
    connection.execute(
        f"create table current_series as {render_sql('current_series', incremental=False)}"
    )

    expected = {
        "current_events": {row for row in full_events if row[0] == "E1"},
        "current_series": rows(render_model(connection, "current_series", incremental=False)),
    }
    expected["current_series"] = {row for row in expected["current_series"] if row[0] == "S1"}
    full_markets = rows(render_model(connection, "current_markets", incremental=False))
    expected["current_markets"] = {row for row in full_markets if row[0] == "M0"}

    for model in ("current_events", "current_series", "current_markets"):
        actual = rows(render_model(connection, model, incremental=True))
        assert actual == expected[model]

        rendered = render_sql(model, incremental=True)
        window_children = [
            child
            for window in operators(explain_json(connection, rendered), "WINDOW")
            for child in window.get("children", [])
        ]
        assert window_children
        history_input = min(operator_cardinality(child) for child in window_children)
        assert history_input <= 4

    assert next(iter(expected["current_events"]))[0] == "E1"
    assert next(iter(expected["current_series"]))[0] == "S1"
