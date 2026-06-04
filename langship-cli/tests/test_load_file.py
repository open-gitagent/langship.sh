"""Tests for the pipeline file loader (JSON and YAML)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from langship.commands.pipelines import _load_file


def test_load_json_file(tmp_path: Path):
    p = tmp_path / "pipeline.json"
    p.write_text(json.dumps({"name": "x", "nodes": []}))
    assert _load_file(p) == {"name": "x", "nodes": []}


def test_load_invalid_json_raises(tmp_path: Path):
    p = tmp_path / "bad.json"
    p.write_text("{not valid json")
    with pytest.raises(json.JSONDecodeError):
        _load_file(p)


def test_load_yaml_file_requires_pyyaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("yaml")
    import yaml as _yaml  # noqa: F401  (importorskip guarantees availability)

    p = tmp_path / "pipeline.yaml"
    p.write_text("name: x\nnodes: []\n")
    assert _load_file(p) == {"name": "x", "nodes": []}
