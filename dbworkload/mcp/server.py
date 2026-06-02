"""MCP server entry point for dbworkload authoring helpers."""

from __future__ import annotations

import subprocess
import sys
from importlib import metadata, resources
from pathlib import Path

import yaml

from dbworkload.utils import common

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
            "- run_workload",
            "- generate_data_seed_blueprint",
        ]
    )


def _validate_file_path(file_path: str, label: str) -> str | None:
    path = Path(file_path).expanduser()
    if not path.exists():
        return f"{label} not found: {file_path}"
    if not path.is_file():
        return f"{label} is not a file: {file_path}"
    return None


def _validate_workload_path(workload_path: str) -> str | None:
    return _validate_file_path(workload_path, "Workload file")


def _append_optional(cmd: list[str], flag: str, value: object | None) -> None:
    if value is not None:
        cmd.extend([flag, str(value)])


def _build_run_command(
    workload_path: str,
    db_uri: str,
    driver: str | None = None,
    procs: int | None = None,
    args: str | None = None,
    concurrency: int | None = None,
    ramp: int | None = None,
    iterations: int | None = None,
    duration: int | None = None,
    max_rate: int | None = None,
    conn_duration: int | None = None,
    app_name: str | None = None,
    autocommit: bool = True,
    prom_port: int | None = None,
    quiet: bool = False,
    save: bool = False,
    schedule: str | None = None,
    histogram_bins: str | None = None,
    delay_stats: int | None = None,
    log_level: str | None = None,
) -> list[str]:
    cmd = [
        "dbworkload",
        "run",
        "--workload",
        str(Path(workload_path).expanduser()),
        "--uri",
        db_uri,
    ]

    _append_optional(cmd, "--driver", driver)
    _append_optional(cmd, "--procs", procs)
    _append_optional(cmd, "--args", args)
    _append_optional(cmd, "--concurrency", concurrency)
    _append_optional(cmd, "--ramp", ramp)
    _append_optional(cmd, "--iterations", iterations)
    _append_optional(cmd, "--duration", duration)
    _append_optional(cmd, "--max-rate", max_rate)
    _append_optional(cmd, "--conn-duration", conn_duration)
    _append_optional(cmd, "--app-name", app_name)
    _append_optional(cmd, "--port", prom_port)
    _append_optional(cmd, "--schedule", schedule)
    _append_optional(cmd, "--bins", histogram_bins)
    _append_optional(cmd, "--delay-stats", delay_stats)
    _append_optional(cmd, "--log-level", log_level)

    if not autocommit:
        cmd.append("--no-autocommit")
    if quiet:
        cmd.append("--quiet")
    if save:
        cmd.append("--save")

    return cmd


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
        msg = (
            "The MCP server requires the optional dependency: install dbworkload[mcp]."
        )
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
        error = _validate_workload_path(workload_path)
        if error:
            return error

        cmd = _build_run_command(
            workload_path=workload_path,
            db_uri=db_uri,
            driver=driver,
            args=args,
            iterations=1,
            quiet=True,
        )
        return _run_command(cmd, timeout_seconds)

    @app.tool()
    def run_workload(
        workload_path: str,
        db_uri: str,
        driver: str | None = None,
        procs: int | None = None,
        args: str | None = None,
        concurrency: int = 1,
        ramp: int = 0,
        iterations: int | None = None,
        duration: int | None = None,
        max_rate: int | None = None,
        conn_duration: int | None = None,
        app_name: str | None = None,
        autocommit: bool = True,
        prom_port: int = 26260,
        quiet: bool = False,
        save: bool = False,
        schedule: str | None = None,
        histogram_bins: str = "5,10,25,50,75,100,125,250,500,750,1000",
        delay_stats: int = 0,
        log_level: str = "info",
        timeout_seconds: int = 3600,
    ) -> str:
        """Run dbworkload with the same options as the dbworkload run CLI."""
        error = _validate_workload_path(workload_path)
        if error:
            return error
        if iterations is None and duration is None and schedule is None:
            return (
                "Refusing to start an unbounded MCP run. Provide iterations, "
                "duration, or schedule."
            )

        cmd = _build_run_command(
            workload_path=workload_path,
            db_uri=db_uri,
            driver=driver,
            procs=procs,
            args=args,
            concurrency=concurrency,
            ramp=ramp,
            iterations=iterations,
            duration=duration,
            max_rate=max_rate,
            conn_duration=conn_duration,
            app_name=app_name,
            autocommit=autocommit,
            prom_port=prom_port,
            quiet=quiet,
            save=save,
            schedule=schedule,
            histogram_bins=histogram_bins,
            delay_stats=delay_stats,
            log_level=log_level,
        )
        return _run_command(cmd, timeout_seconds)

    @app.tool()
    def generate_data_seed_blueprint(
        ddl: str,
    ) -> dict:
        """Generate a JSON-compatible data seeding blueprint from raw DDL text."""
        if not ddl.strip():
            return {
                "ok": False,
                "error": "DDL input is empty.",
            }

        blueprint = yaml.safe_load(common.ddl_to_yaml(ddl)) or {}
        return {
            "ok": True,
            "blueprint": blueprint,
        }

    return app


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] in {"--info", "info"}:
        print(server_info_text())
        return

    create_app().run()


if __name__ == "__main__":
    main()
