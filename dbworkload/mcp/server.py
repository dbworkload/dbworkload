"""MCP server entry point for dbworkload authoring helpers."""

from __future__ import annotations

import subprocess
import sys
from importlib import metadata, resources
from pathlib import Path


SERVER_NAME = "dbworkload-helper"
SKILLS_RESOURCE_URI = "dbworkload://docs/skills"
INFO_RESOURCE_URI = "dbworkload://server/info"


def read_skills() -> str:
    """Return the bundled dbworkload authoring guide."""
    return (
        resources.files("dbworkload.mcp")
        .joinpath("skills.md")
        .read_text(encoding="utf-8")
    )


def server_info_text() -> str:
    """Return installation and capability details for this MCP server."""
    try:
        version = metadata.version("dbworkload")
    except metadata.PackageNotFoundError:
        version = "#N/A"

    return "\n".join(
        [
            "dbworkload MCP server is installed and ready.",
            "",
            f"Server name: {SERVER_NAME}",
            f"dbworkload version: {version}",
            "",
            "Resources:",
            f"- {INFO_RESOURCE_URI}",
            f"- {SKILLS_RESOURCE_URI}",
            "",
            "Tools:",
            "- get_server_info",
            "- get_authoring_rules",
            "- dry_run_workload",
        ]
    )


def _format_completed_process(result: subprocess.CompletedProcess[str]) -> str:
    output = result.stdout.strip()
    error = result.stderr.strip()

    parts = [f"Exit code: {result.returncode}"]
    if output:
        parts.extend(["", "stdout:", output])
    if error:
        parts.extend(["", "stderr:", error])
    return "\n".join(parts)


def _run_command(cmd: list[str], timeout_seconds: int) -> str:
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return f"Command timed out after {timeout_seconds} seconds.\n{exc}"

    return _format_completed_process(result)


def create_app():
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:
        msg = "The MCP server requires the optional dependency: install dbworkload[mcp]."
        raise RuntimeError(msg) from exc

    app = FastMCP(SERVER_NAME)

    @app.resource(INFO_RESOURCE_URI)
    def get_dbworkload_mcp_info() -> str:
        """Return installation and capability details for this MCP server."""
        return server_info_text()

    @app.resource(SKILLS_RESOURCE_URI)
    def get_dbworkload_skills() -> str:
        """Return rules and examples for writing valid dbworkload classes."""
        return read_skills()

    @app.tool()
    def get_server_info() -> str:
        """Return installation and capability details for this MCP server."""
        return server_info_text()

    @app.tool()
    def get_authoring_rules() -> str:
        """Return rules and examples for writing valid dbworkload classes."""
        return read_skills()

    @app.tool()
    def dry_run_workload(
        workload_path: str,
        db_uri: str,
        driver: str | None = None,
        args: str | None = None,
        timeout_seconds: int = 60,
    ) -> str:
        """Run a single dbworkload iteration against a target database URI."""
        path = Path(workload_path).expanduser()
        if not path.exists():
            return f"Workload file not found: {workload_path}"
        if not path.is_file():
            return f"Workload path is not a file: {workload_path}"

        cmd = [
            "dbworkload",
            "run",
            "--workload",
            str(path),
            "--uri",
            db_uri,
            "--iterations",
            "1",
            "--quiet",
        ]
        if driver:
            cmd.extend(["--driver", driver])
        if args:
            cmd.extend(["--args", args])

        return _run_command(cmd, timeout_seconds)

    return app


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] in {"--info", "info"}:
        print(server_info_text())
        return

    create_app().run()


if __name__ == "__main__":
    main()
