# pi-acp-rust

An [Agent Client Protocol](https://agentclientprotocol.com/) adapter for the native Pi CLI, written in Rust. It is used by [AgentOS](https://github.com/rivet-dev/agent-os) and does not embed Pi.

```sh
npm install --global @earendil-works/pi-coding-agent @rivet-dev/pi-acp
pi-acp
```

On an i7-12700KF Linux host, 30 isolated interleaved runs produced:

| Adapter | Startup median | Startup p95 | Adapter RSS median | Process-tree RSS median |
| --- | ---: | ---: | ---: | ---: |
| Rust native (`7da5cb2`) | 19.0 ms | 20.8 ms | 3.7 MiB | 15.8 MiB |
| JavaScript `pi-acp@0.0.31` | 104.0 ms | 112.0 ms | 91.5 MiB | 103.6 MiB |

Reproduce it on Linux with exactly `python3 bench/run.py --samples 30 --warmups 5`; see the [methodology and raw samples](bench/results-linux-x86_64.json).

| Feature | Rust | `pi-acp@0.0.31` |
| --- | --- | --- |
| Native `pi --mode rpc` | Yes | Yes |
| Host Pi config, credentials, skills, and tools | Inherited unchanged | Inherited unchanged |
| Text, thought, image, tool, and structured-diff streaming | Yes | No separate thought stream |
| Embedded text resources | On by default | Opt-in |
| Model groups, model switching, and thinking modes | Yes | Yes |
| Multi-turn queue and cancellation | Bounded queue; yes | Client queue; yes |
| Session list/load/resume/fork/close | Yes | List/load |
| Native extension, prompt, and skill command discovery | Yes | Prompt/skill; extension commands filtered |
| Built-in headless commands | Compact/session/name/steering/follow-up/export | Yes; broader UI-oriented set |
| Extension select/confirm through ACP permissions | Yes | Yes |
| Extension free-form input/editor UI | Cancelled explicitly | Cancelled explicitly |
| ACP terminal login | Yes | Yes |
| Per-session/system prompt injection | Yes | No adapter option |
| MCP and additional roots | Rejected explicitly; Pi RPC has no native surface | MCP accepted but not wired |

The real-Pi LLMock gate passes two turns separated by close/resume on `@earendil-works/pi-coding-agent@0.80.6`: 14 models, `PONG`/`PONG`, and two LLM requests. Legacy `0.73.1` and `0.60.0` are also covered in CI.

Releases include native binaries for macOS, Linux, and Windows. AgentOS pins this source and owns its VM-specific cross-compilation separately.
