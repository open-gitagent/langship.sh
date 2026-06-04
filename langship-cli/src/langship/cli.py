"""langship — CLI for the Langship control plane."""

from __future__ import annotations

import typer
from rich.console import Console

from . import __version__
from . import config as cfg
from .client import APIError
from .utils import handle_api_error, die
from .commands import (
    agents as agents_cmd,
    environments as envs_cmd,
    pipelines as pipelines_cmd,
    credentials as creds_cmd,
    runs as runs_cmd,
)

console = Console()

app = typer.Typer(
    name="langship",
    help="langship — drive the Langship control plane from your terminal.",
    no_args_is_help=True,
    add_completion=False,
    rich_markup_mode="rich",
    pretty_exceptions_enable=False,
)

app.add_typer(agents_cmd.app, name="agents", help="Manage agents (repos + env subscriptions).")
app.add_typer(envs_cmd.app, name="envs", help="Manage environments (named, ordered pipeline lists).")
app.add_typer(pipelines_cmd.app, name="pipelines", help="Manage pipeline definitions (push, validate, list, get, delete).")
app.add_typer(creds_cmd.app, name="creds", help="Manage the global credential pool (aws / gcp / kv).")
app.add_typer(runs_cmd.app, name="runs", help="Inspect executions and stream logs.")


@app.command()
def login(
    api_url: str = typer.Option(None, "--api-url", help="Langship API URL, e.g. http://localhost:8090.", show_default=False),
    token: str = typer.Option(None, "--token", help="API auth token (optional).", show_default=False),
) -> None:
    """Save API URL (and optional token) to ~/.langship/config.toml.

    Fully non-interactive when --api-url is passed. With no flags it
    prompts for the URL (and optionally a token).
    """
    current = cfg.load()
    interactive = api_url is None
    if api_url is None:
        api_url = typer.prompt("API URL", default=cfg.api_url_or_none() or "http://localhost:8090")
    if token is None and interactive:
        token = typer.prompt("Token (optional, blank for none)", default=current.get("token", ""), show_default=False)
    current["api_url"] = api_url.rstrip("/")
    if token:
        current["token"] = token
    elif token == "" and "token" in current:
        # explicit empty input clears it
        del current["token"]
    cfg.save(current)
    console.print(f"[green]✓[/green] saved {cfg.CONFIG_PATH}")
    console.print(f"  api_url = {current['api_url']}")
    if "token" in current:
        console.print("  token   = ****")


@app.command(name="config-show")
def config_show() -> None:
    """Print the current local config."""
    console.print(f"[dim]path:[/dim] {cfg.CONFIG_PATH}")
    url = cfg.api_url_or_none()
    console.print(f"api_url = {url or '(unset — run `langship login`)'}")
    console.print("token   = ****" if cfg.token() else "token   = (unset)")


@app.command(name="version")
def version_cmd() -> None:
    """Print version."""
    console.print(f"langship {__version__}")


def _version_callback(value: bool) -> None:
    if value:
        console.print(f"langship {__version__}")
        raise typer.Exit()


@app.callback()
def _root(
    version: bool = typer.Option(
        False, "--version", callback=_version_callback, is_eager=True, help="Show version and exit."
    ),
) -> None:
    pass


def main() -> None:
    import sys

    import click

    try:
        app()
    except SystemExit:
        raise
    except click.exceptions.Exit as e:
        sys.exit(getattr(e, "exit_code", 0))
    except click.exceptions.Abort:
        from .utils import err_console

        err_console.print("[red]error:[/red] aborted")
        sys.exit(2)
    except APIError as e:
        handle_api_error(e)
    except RuntimeError as e:
        from .utils import err_console

        err_console.print(f"[red]error:[/red] {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        die("interrupted", code=130)


if __name__ == "__main__":
    main()
