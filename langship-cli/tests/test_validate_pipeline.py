"""Tests for the pipeline validation logic.

Covers both the happy path (valid pipelines) and the error cases
(trigger, nodes, connections) raised by `_validate_definition`.
"""

from __future__ import annotations

import pytest

from langship.commands.pipelines import _is_trigger_type, _validate_definition


# A valid hello-world definition used as the baseline.
VALID_PIPELINE = {
    "name": "hello",
    "nodes": [
        {
            "id": "1",
            "name": "Trigger",
            "type": "n8n-nodes-base.manualTrigger",
            "typeVersion": 1,
            "parameters": {},
            "position": [0, 0],
        },
        {
            "id": "2",
            "name": "Set Greeting",
            "type": "n8n-nodes-base.set",
            "typeVersion": 3,
            "parameters": {},
            "position": [200, 0],
        },
        {
            "id": "3",
            "name": "End",
            "type": "n8n-nodes-base.noop",
            "typeVersion": 1,
            "parameters": {},
            "position": [400, 0],
        },
    ],
    "connections": {
        "Trigger": {"main": [[{"node": "Set Greeting", "type": "main", "index": 0}]]},
        "Set Greeting": {"main": [[{"node": "End", "type": "main", "index": 0}]]},
    },
}


# --- happy path -------------------------------------------------------------


def test_valid_pipeline_has_no_errors():
    assert _validate_definition(VALID_PIPELINE) == []


def test_valid_pipeline_in_definition_wrapper():
    # The CLI accepts both bare n8n-format and {definition: {...}} shapes.
    wrapped = {"name": "hello", "definition": VALID_PIPELINE}
    assert _validate_definition(wrapped) == []


def test_valid_pipeline_without_connections():
    doc = {
        "name": "t",
        "nodes": [
            {"id": "1", "name": "Trigger", "type": "flow-nodes-base.trigger", "position": [0, 0]},
        ],
    }
    assert _validate_definition(doc) == []


# --- trigger rules ----------------------------------------------------------


@pytest.mark.parametrize(
    "node_type",
    [
        "n8n-nodes-base.manualTrigger",
        "n8n-nodes-base.webhook",
        "n8n-nodes-base.scheduleTrigger",
        "n8n-nodes-base.cron",
        "n8n-nodes-langchain.chatTrigger",
        "flow-nodes-base.trigger",
    ],
)
def test_is_trigger_type_recognizes_known_triggers(node_type):
    assert _is_trigger_type(node_type) is True


def test_non_trigger_type_is_not_trigger():
    assert _is_trigger_type("n8n-nodes-base.set") is False
    assert _is_trigger_type("flow-nodes-base.code") is False


def test_no_trigger_node_is_an_error():
    doc = {
        "nodes": [
            {"id": "1", "name": "A", "type": "n8n-nodes-base.set", "position": [0, 0]},
            {"id": "2", "name": "B", "type": "n8n-nodes-base.noop", "position": [200, 0]},
        ],
        "connections": {"A": {"main": [[{"node": "B", "type": "main", "index": 0}]]}},
    }
    errors = _validate_definition(doc)
    assert any("trigger node (found 0)" in e for e in errors)


def test_multiple_triggers_is_an_error():
    doc = {
        "nodes": [
            {"id": "1", "name": "A", "type": "n8n-nodes-base.manualTrigger", "position": [0, 0]},
            {"id": "2", "name": "B", "type": "n8n-nodes-base.webhook", "position": [200, 0]},
        ],
    }
    errors = _validate_definition(doc)
    assert any("trigger node (found 2)" in e for e in errors)


# --- nodes shape ------------------------------------------------------------


def test_missing_nodes_array_is_an_error():
    errors = _validate_definition({"name": "x"})
    assert errors and "'nodes' must be a non-empty array" in errors[0]


def test_empty_nodes_array_is_an_error():
    errors = _validate_definition({"nodes": []})
    assert errors and "'nodes' array is empty" in errors[0]


def test_node_missing_id_is_an_error():
    doc = {
        "nodes": [
            {"name": "Trigger", "type": "n8n-nodes-base.manualTrigger", "position": [0, 0]},
        ],
    }
    errors = _validate_definition(doc)
    assert any("missing or invalid 'id'" in e for e in errors)


def test_duplicate_node_ids_is_an_error():
    doc = {
        "nodes": [
            {"id": "1", "name": "A", "type": "n8n-nodes-base.manualTrigger", "position": [0, 0]},
            {"id": "1", "name": "B", "type": "n8n-nodes-base.set", "position": [200, 0]},
        ],
    }
    errors = _validate_definition(doc)
    assert any("duplicate node id '1'" in e for e in errors)


def test_duplicate_node_names_is_an_error():
    doc = {
        "nodes": [
            {"id": "1", "name": "X", "type": "n8n-nodes-base.manualTrigger", "position": [0, 0]},
            {"id": "2", "name": "X", "type": "n8n-nodes-base.set", "position": [200, 0]},
        ],
    }
    errors = _validate_definition(doc)
    assert any("duplicate node name 'X'" in e for e in errors)


def test_node_missing_type_is_an_error():
    doc = {
        "nodes": [
            {"id": "1", "name": "X", "position": [0, 0]},
        ],
    }
    errors = _validate_definition(doc)
    assert any("missing or invalid 'type'" in e for e in errors)


def test_position_must_be_pair():
    doc = {
        "nodes": [
            {
                "id": "1",
                "name": "Trigger",
                "type": "n8n-nodes-base.manualTrigger",
                "position": [0],
            },
        ],
    }
    errors = _validate_definition(doc)
    assert any("'position' must be an array of 2 numbers" in e for e in errors)


# --- connections ------------------------------------------------------------


def test_connection_to_unknown_node_is_an_error():
    doc = {
        "nodes": [
            {"id": "1", "name": "Trigger", "type": "n8n-nodes-base.manualTrigger", "position": [0, 0]},
        ],
        "connections": {
            "Trigger": {"main": [[{"node": "Missing", "type": "main", "index": 0}]]},
        },
    }
    errors = _validate_definition(doc)
    assert any("references unknown node 'Missing'" in e for e in errors)


def test_connection_with_unknown_source_is_an_error():
    doc = {
        "nodes": [
            {"id": "1", "name": "Trigger", "type": "n8n-nodes-base.manualTrigger", "position": [0, 0]},
        ],
        "connections": {
            "Ghost": {"main": [[{"node": "Trigger", "type": "main", "index": 0}]]},
        },
    }
    errors = _validate_definition(doc)
    assert any("source node 'Ghost' not found in nodes" in e for e in errors)


def test_connections_must_be_object():
    doc = {
        "nodes": [
            {"id": "1", "name": "Trigger", "type": "n8n-nodes-base.manualTrigger", "position": [0, 0]},
        ],
        "connections": "not a dict",
    }
    errors = _validate_definition(doc)
    assert any("'connections' must be an object" in e for e in errors)


def test_connection_target_must_have_node_field():
    doc = {
        "nodes": [
            {"id": "1", "name": "Trigger", "type": "n8n-nodes-base.manualTrigger", "position": [0, 0]},
            {"id": "2", "name": "End", "type": "n8n-nodes-base.noop", "position": [200, 0]},
        ],
        "connections": {
            "Trigger": {"main": [[{"type": "main", "index": 0}]]},  # no 'node' field
        },
    }
    errors = _validate_definition(doc)
    assert any("missing or invalid 'node'" in e for e in errors)
