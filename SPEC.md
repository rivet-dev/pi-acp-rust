# Pi ACP Rust specification

## Purpose

`pi-acp-rust` is a low-overhead ACP adapter around the **native Pi CLI**. It launches `pi --mode rpc` and translates Pi's newline-delimited RPC protocol to ACP over standard input and output. Pi remains the source of truth for agents, extensions, tools, models, settings, credentials, and session files; this package never embeds the Pi SDK.

The current compatibility target is `@earendil-works/pi-coding-agent@0.80.6`. CI also protects the migration boundary at `@mariozechner/pi-coding-agent@0.73.1` and AgentOS's legacy `0.60.0` package.

## Public artifacts

- `@rivet-dev/pi-acp`: cargo-dist npm launcher for macOS arm64/x64, Linux arm64/x64, and Windows x64.

AgentOS pins a source commit and owns any VM-specific patch and cross-compilation. This repository publishes no AgentOS- or WASM-specific artifact.

## Compatibility contract

| Area | Required behavior | Status |
| --- | --- | --- |
| Transport | Strict LF-delimited JSON-RPC 2.0 over stdio | Implemented |
| Pi process | Spawn the installed `pi --mode rpc`; preserve host `HOME`, environment credentials, Pi configuration, extensions, skills, prompt templates, and custom tools | Implemented |
| Sessions | New, list with pagination/cwd filter, load with transcript replay, resume, fork, and close | Implemented |
| Turns | Multiple serialized turns, bounded queued prompts, cancellation, and lifecycle completion on current `agent_settled` or legacy `agent_end` | Implemented |
| Prompt content | Text, images, and embedded text resources | Implemented |
| Streaming | Assistant text, reasoning, tool calls, tool progress/results, file locations, and edit diffs in source order | Implemented |
| Models | Query Pi's live model registry, group by provider, select by `provider/model`, and refresh session state | Implemented |
| Thinking | Advertise Pi's available thinking levels through ACP modes and config options; apply mode changes | Implemented |
| Commands | Advertise Pi commands and translate `/compact`, `/session`, `/name`, `/steering`, `/follow-up`, and `/export` to native Pi RPC | Implemented |
| Extension UI | Map extension select/confirm requests to ACP permissions; cancel unsupported free-form input without hanging Pi | Implemented |
| System prompt | Repeatable `--append-system-prompt` plus ACP `_meta.systemPrompt` | Implemented |
| Authentication | Use Pi's native host configuration and environment; no adapter-specific token conversion; advertise `--terminal-login` to capable ACP clients | Implemented |
| MCP servers | Do not silently ignore ACP MCP configuration; return an invalid-params error because Pi RPC has no native MCP surface | Explicitly unsupported |
| Additional roots | Do not silently ignore ACP `additionalDirectories`; return an invalid-params error because Pi RPC cannot apply them | Explicitly unsupported |

The feature contract matches or exceeds AgentOS's embedded Pi SDK adapter: model grouping and switching, thinking modes, multi-turn/cancel, system prompts, native extension discovery, text/reasoning/tool streams, and edit diffs are preserved. Native session resume/list/load/fork and slash-command discovery are additional coverage enabled by using the CLI.

## Runtime and limits

The adapter uses Tokio child processes to run the native Pi CLI.

- ACP and Pi RPC records: 16 MiB maximum each.
- Pending Pi RPC commands: every command has a 30-second timeout and process-exit propagation.
- Pi event broadcast: 512 entries.
- Queued prompts: 32 by default, configurable with `--max-queued-prompts`.
- Tracked edit tool calls: 512 by default, configurable with `--max-tracked-tool-calls`, with a warning at 80%.
- Turn timeout: one hour.

Crossing a configurable limit produces an error that names the limit and its corresponding CLI flag. Child crashes and malformed protocol records are surfaced to the ACP client or stderr.

## Validation and release

The deterministic Rust E2E suite exercises multi-turn prompts, ordering, tool updates/diffs, model grouping and switching, thinking modes, commands, cancellation, list/load/resume/fork/close, and transcript replay. The real-Pi suite runs two turns separated by close/resume through LLMock against all three supported Pi versions.

`cargo-dist` builds and attests the native binaries and publishes the npm launcher with OIDC provenance.
