"""pipelines list / get / push / delete / validate.

`push` takes a local file containing the pipeline definition (n8n-format
JSON, or YAML if PyYAML is installed). If the file (or the top-level
object) carries an `id`, the pipeline is updated; otherwise a new one is
created and its id printed. The file is the source of truth — GitOps.

`validate` checks the definition locally before hitting the server.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer

from .. import client
from ..utils import confirm, console, die, emit, make_table, print_kv, relative_time, truncate

app = typer.Typer(no_args_is_help=True)

# n8n trigger types that map to flow-nodes-base.trigger — kept in sync with
# pkg/engine/parser.go so local validation matches server behaviour.
_TRIGGER_TYPES = frozenset({
    "n8n-nodes-base.webhook",
    "n8n-nodes-base.scheduleTrigger",
    "n8n-nodes-base.manualTrigger",
    "n8n-nodes-base.formTrigger",
    "n8n-nodes-base.activationTrigger",
    "n8n-nodes-base.errorTrigger",
    "n8n-nodes-base.executeWorkflowTrigger",
    "n8n-nodes-base.n8nTrigger",
    "n8n-nodes-base.sseTrigger",
    "n8n-nodes-base.workflowTrigger",
    "n8n-nodes-base.localFileTrigger",
    "n8n-nodes-base.emailReadImap",
    "n8n-nodes-base.rssFeedReadTrigger",
    "n8n-nodes-base.evaluationTrigger",
    "n8n-nodes-base.start",
    "n8n-nodes-base.cron",
    "n8n-nodes-base.interval",
    "n8n-nodes-langchain.chatTrigger",
    "n8n-nodes-langchain.mcpTrigger",
    "flow-nodes-base.trigger",
})


def _is_trigger_type(node_type: str) -> bool:
    """Return True if the node type is treated as a trigger."""
    return node_type in _TRIGGER_TYPES


def _load_file(path: Path) -> dict:
    text = path.read_text()
    if path.suffix in (".yaml", ".yml"):
        try:
            import yaml  # type: ignore

            return yaml.safe_load(text)
        except ImportError:
            die("PyYAML not installed — install it or pass a .json file")
    return json.loads(text)


def _validate_definition(doc: dict) -> list[str]:
    """Validate a parsed pipeline definition, returning a list of errors.

    Checks match the server-side validations in pkg/engine/parser.go plus
    common-sense structural rules.
    """
    errors: list[str] = []

    definition = doc.get("definition", doc)

    nodes = definition.get("nodes")
    if not isinstance(nodes, list):
        errors.append("'nodes' must be a non-empty array")
        return errors
    if len(nodes) == 0:
        errors.append("'nodes' array is empty")
        return errors

    seen_ids: set[str] = set()
    seen_names: set[str] = set()
    trigger_count = 0

    for i, node in enumerate(nodes):
        prefix = f"nodes[{i}]"
        if not isinstance(node, dict):
            errors.append(f"{prefix}: expected an object, got {type(node).__name__}")
            continue

        node_id = node.get("id")
        if not node_id or not isinstance(node_id, str):
            errors.append(f"{prefix}: missing or invalid 'id' (must be a non-empty string)")
        elif node_id in seen_ids:
            errors.append(f"{prefix}: duplicate node id '{node_id}'")
        else:
            seen_ids.add(node_id)

        node_name = node.get("name")
        if not node_name or not isinstance(node_name, str):
            errors.append(f"{prefix}: missing or invalid 'name' (must be a non-empty string)")
        elif node_name in seen_names:
            errors.append(f"{prefix}: duplicate node name '{node_name}'")
        else:
            seen_names.add(node_name)

        node_type = node.get("type")
        if not node_type or not isinstance(node_type, str):
            errors.append(f"{prefix}: missing or invalid 'type' (must be a non-empty string)")
        elif _is_trigger_type(node_type):
            trigger_count += 1

        pos = node.get("position")
        if pos is not None and (not isinstance(pos, (list, tuple)) or len(pos) != 2):
            errors.append(f"{prefix}: 'position' must be an array of 2 numbers")

    if trigger_count == 0:
        errors.append("workflow must have exactly one trigger node (found 0)")
    elif trigger_count > 1:
        errors.append(f"workflow must have exactly one trigger node (found {trigger_count})")

    connections = definition.get("connections")
    if connections is not None:
        if not isinstance(connections, dict):
            errors.append("'connections' must be an object")
        else:
            for source_name, conn_targets in connections.items():
                if source_name not in seen_names:
                    errors.append(f"connections: source node '{source_name}' not found in nodes")
                    continue
                if not isinstance(conn_targets, dict):
                    errors.append(f"connections['{source_name}']: expected an object with 'main' key")
                    continue
                main = conn_targets.get("main")
                if not isinstance(main, list):
                    errors.append(f"connections['{source_name}']: 'main' must be an array")
                    continue
                for output_idx, output_group in enumerate(main):
                    if not isinstance(output_group, list):
                        errors.append(f"connections['{source_name}'].main[{output_idx}]: expected an array of targets")
                        continue
                    for target_idx, target in enumerate(output_group):
                        if not isinstance(target, dict):
                            errors.append(f"connections['{source_name}'].main[{output_idx}][{target_idx}]: expected an object with 'node' field")
                            continue
                        target_node = target.get("node")
                        if not target_node or not isinstance(target_node, str):
                            errors.append(f"connections['{source_name}'].main[{output_idx}][{target_idx}]: missing or invalid 'node'")
                        elif target_node not in seen_names:
                            errors.append(f"connections['{source_name}'].main[{output_idx}][{target_idx}]: references unknown node '{target_node}'")

    return errors


@app.command("validate")
def validate_pipeline(
    file: Path = typer.Argument(..., exists=True, readable=True, help="JSON/YAML file with the pipeline definition."),
) -> None:
    """Validate a pipeline definition locally without contacting the server.

    Checks structure, required fields, duplicate names/ids, exactly one
    trigger node, and that connections reference valid nodes.
    """
    from ..utils import err_console

    try:
        doc = _load_file(file)
    except (json.JSONDecodeError, ValueError) as e:
        die(f"invalid {file.suffix} file: {e}")

    if not isinstance(doc, dict):
        die("file must contain a JSON/YAML object")

    errors = _validate_definition(doc)
    if not errors:
        console.print(f"[green]✓[/green] [bold]{file}[/bold] is valid")
        return

    err_console.print(f"[red]✗[/red] [bold]{file}[/bold] has {len(errors)} error{'s' if len(errors) > 1 else ''}:")
    for err in errors:
        err_console.print(f"  [red]·[/red] {err}")
    raise typer.Exit(code=1)


@app.command("list")
def list_pipelines(output: str = typer.Option("table", "--output", "-o", help="table | json | yaml")) -> None:
    """List pipeline definitions."""
    flows = client.get("/api/workflows") or []
    if emit(flows, output):
        return
    if not flows:
        console.print("[dim]no pipelines yet.[/dim]")
        return
    t = make_table("PIPELINE ID", "NAME", "NODES", "STATUS", "UPDATED")
    for f in flows:
        t.add_row(f["id"], f.get("name") or "—", str(f.get("nodeCount", 0)), f.get("status") or "draft", relative_time(f.get("updatedAt")))
    console.print(t)


@app.command("get")
def get_pipeline(
    pipeline_id: str = typer.Argument(...),
    output: str = typer.Option("json", "--output", "-o", help="json | yaml | table"),
) -> None:
    """Dump a pipeline. Default output is JSON (the definition is the point)."""
    p = client.get(f"/api/workflows/{pipeline_id}")
    if output.lower() in ("json", "yaml", "yml"):
        emit(p, output)
        return
    # table summary
    defn = p.get("definition") or {}
    nodes = defn.get("nodes") or []
    print_kv(
        {"id": p.get("id"), "name": p.get("name"), "nodes": len(nodes), "status": p.get("status"), "updated": p.get("updatedAt")},
        title=f"pipeline · {p.get('name')}",
    )
    if nodes:
        t = make_table("NODE", "TYPE", title="nodes")
        for n in nodes:
            t.add_row(n.get("name") or "—", n.get("type") or "—")
        console.print(t)


@app.command("push")
def push_pipeline(
    file: Path = typer.Argument(..., exists=True, readable=True, help="JSON/YAML file with the pipeline definition."),
    pipeline_id: str = typer.Option("", "--id", help="Update this pipeline id (overrides any id in the file)."),
    name: str = typer.Option("", "--name", "-n", help="Override the pipeline name."),
) -> None:
    """Create or update a pipeline from a local definition file."""
    doc = _load_file(file)
    if not isinstance(doc, dict):
        die("file must contain a JSON/YAML object")

    # Two accepted shapes: {id?, name?, definition:{...}} or a bare
    # n8n-format definition {name?, nodes:[...], connections:{...}}.
    if "definition" in doc:
        definition = doc["definition"]
        file_id = doc.get("id")
        file_name = doc.get("name")
    else:
        definition = doc
        file_id = doc.get("id")
        file_name = doc.get("name")

    pid = pipeline_id or (file_id or "")
    body: dict = {"definition": definition}
    if name:
        body["name"] = name
    elif file_name:
        body["name"] = file_name

    if pid:
        client.put(f"/api/workflows/{pid}", json=body)
        console.print(f"[green]✓[/green] updated pipeline [cyan]{pid}[/cyan]")
    else:
        res = client.post("/api/workflows", json=body)
        new_id = res.get("id") if isinstance(res, dict) else None
        console.print(f"[green]✓[/green] created pipeline [cyan]{new_id}[/cyan]")
        console.print(f"  tip: add \"id\": \"{new_id}\" to {file} so the next push updates it in place")


@app.command("delete")
def delete_pipeline(
    pipeline_id: str = typer.Argument(...),
    yes: bool = typer.Option(False, "--yes", "-y"),
) -> None:
    """Delete a pipeline definition."""
    if not yes and not confirm(f"delete pipeline {pipeline_id}? (envs referencing it will lose it)", default=False):
        die("aborted", code=2)
    client.delete(f"/api/workflows/{pipeline_id}")
    console.print(f"[green]✓[/green] deleted {pipeline_id}")
