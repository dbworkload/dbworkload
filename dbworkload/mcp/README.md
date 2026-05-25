# dbworkload MCP Server

`dbworkload` includes an optional Model Context Protocol (MCP) server that helps
AI coding agents write and validate workload classes.

The server runs over `stdio` and exposes:

- `dbworkload://server/info`: a resource with readiness and capability details.
- `dbworkload://docs/skills`: a resource with workload authoring rules.
- `get_server_info`: a tool that reports server readiness and exposed features.
- `get_authoring_rules`: a tool that returns the same authoring guidance.
- `dry_run_workload`: a tool that runs one `dbworkload` iteration against a
  target database URI.

## Install

Install `pipx` if it is not already available.

On macOS with Homebrew:

```bash
brew install pipx
pipx ensurepath
```

Then install `dbworkload` with the optional MCP dependency:

```bash
pipx install "dbworkload[mcp]"
```

## Test The Install

Confirm that the command is available:

```bash
dbworkload-mcp-server --info
```

Expected output starts with:

```text
dbworkload MCP server is installed and ready.
```

The output also lists the resources and tools exposed by the server.

## MCP Client Configuration

Add the server to an MCP-compatible client such as Claude Desktop, Cursor,
Windsurf, or VS Code with MCP support.

```json
{
  "mcpServers": {
    "dbworkload": {
      "command": "dbworkload-mcp-server"
    }
  }
}
```

After adding the configuration, restart the client and ask it to call
`get_server_info`. A working setup should report that the server is installed
and ready.

## Expected Agent Flow

Once registered, an AI agent can:

1. Call `get_server_info` to confirm the server is available.
2. Read `dbworkload://docs/skills` to learn the workload class contract.
3. Generate or edit a workload Python file in the user's workspace.
4. Call `dry_run_workload` with the workload path and database URI.
5. Use the returned stdout, stderr, and exit code to fix the workload and retry.

## Notes

`dry_run_workload` wraps the existing CLI command:

```bash
dbworkload run --workload <file> --uri <uri> --iterations 1 --quiet
```

A database smoke-test tool can be added later if `dbworkload` grows a dedicated
connection-check command.
