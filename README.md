# pi-acp-rust

An [Agent Client Protocol](https://agentclientprotocol.com/) adapter for the native Pi CLI, written in Rust. It is used by [AgentOS](https://github.com/rivet-dev/agent-os) and does not embed Pi.

```sh
npm install --global @earendil-works/pi-coding-agent @rivet-dev/pi-acp
pi-acp
```

On an i7-12700KF Linux host, the adapter starts a session in **20.0 ms median** and uses **8.6 MiB median RSS**. The JavaScript `pi-acp@0.0.31` reference measured 101.4 ms and 93.3 MiB under the same 30-run fixture. See the [raw benchmark](bench/results-linux-x86_64.json).

Releases include native binaries and `pi-acp-agentos.wasm`. The npm package exposes the AgentOS build at `@rivet-dev/pi-acp/wasm/pi-acp.wasm`.
