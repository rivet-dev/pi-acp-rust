use std::{path::PathBuf, process::Stdio};

use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout},
};

struct Harness {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl Harness {
    async fn start(state: &TempDir) -> Self {
        let fake = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/fake-pi.py");
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_pi-acp"))
            .arg("--pi-command")
            .arg(fake)
            .arg("--state-dir")
            .arg(state.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            stdin,
            stdout,
            next_id: 1,
        }
    }

    async fn notify(&mut self, method: &str, params: Value) {
        self.write(json!({"jsonrpc": "2.0", "method": method, "params": params}))
            .await;
    }

    async fn request(&mut self, method: &str, params: Value) -> (Value, Vec<Value>) {
        let id = self.next_id;
        self.next_id += 1;
        self.write(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await;
        let mut notifications = Vec::new();
        loop {
            let mut line = String::new();
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                self.stdout.read_line(&mut line),
            )
            .await
            .expect("ACP response timeout")
            .unwrap();
            assert!(!line.is_empty(), "ACP process exited");
            let value: Value = serde_json::from_str(&line).unwrap();
            if value.get("id") == Some(&json!(id)) {
                assert!(value.get("error").is_none(), "ACP error: {value}");
                return (value["result"].clone(), notifications);
            }
            if value.get("method").and_then(Value::as_str) == Some("session/request_permission") {
                let request_id = value["id"].clone();
                self.write(json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {"outcome": {"outcome": "selected", "optionId": "choice-0"}}
                }))
                .await;
            }
            notifications.push(value);
        }
    }

    async fn write(&mut self, value: Value) {
        self.stdin
            .write_all(format!("{value}\n").as_bytes())
            .await
            .unwrap();
        self.stdin.flush().await.unwrap();
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

fn updates(notifications: &[Value]) -> Vec<&str> {
    notifications
        .iter()
        .filter_map(|value| {
            value
                .pointer("/params/update/sessionUpdate")
                .and_then(Value::as_str)
        })
        .collect()
}

#[tokio::test]
async fn full_session_lifecycle_and_multi_turn() {
    let state = TempDir::new().unwrap();
    let mut acp = Harness::start(&state).await;

    let (initialize, _) = acp
        .request(
            "initialize",
            json!({"protocolVersion": 1, "clientCapabilities": {"auth": {"terminal": true}}}),
        )
        .await;
    assert_eq!(initialize["protocolVersion"], 1);
    assert_eq!(initialize["agentCapabilities"]["loadSession"], true);
    assert_eq!(initialize["authMethods"][0]["type"], "terminal");
    assert_eq!(initialize["authMethods"][0]["args"][0], "--terminal-login");
    assert!(initialize["agentCapabilities"]["sessionCapabilities"]["close"].is_object());
    assert!(initialize["agentCapabilities"]["sessionCapabilities"]["resume"].is_object());

    let cwd = state.path().canonicalize().unwrap();
    let (created, _) = acp
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": [], "_meta": {"systemPrompt": "test instructions"}}),
        )
        .await;
    let session_id = created["sessionId"].as_str().unwrap().to_owned();
    assert_eq!(session_id, "fake-session");
    assert_eq!(created["modes"]["currentModeId"], "medium");
    assert!(created["modes"]["availableModes"].is_array());
    let options = created["configOptions"].as_array().unwrap();
    let model = options
        .iter()
        .find(|option| option["id"] == "model")
        .unwrap();
    assert_eq!(model["category"], "model");
    assert_eq!(
        model["options"].as_array().unwrap().len(),
        2,
        "models grouped by provider"
    );

    for number in 1..=2 {
        let (turn, notifications) = acp
            .request(
                "session/prompt",
                json!({"sessionId": session_id, "prompt": [{"type": "text", "text": number.to_string()}]}),
            )
            .await;
        assert_eq!(turn["stopReason"], "end_turn");
        let kinds = updates(&notifications);
        assert!(kinds.contains(&"agent_thought_chunk"));
        assert!(kinds.contains(&"tool_call"));
        assert!(kinds.contains(&"tool_call_update"));
        assert!(notifications.iter().any(|notification| {
            notification
                .pointer("/params/update/content/text")
                .and_then(Value::as_str)
                == Some(format!("reply:{number}").as_str())
        }));
    }

    let (_, edit_updates) = acp
        .request(
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "edit"}]}),
        )
        .await;
    assert!(edit_updates.iter().any(|notification| {
        notification
            .pointer("/params/update/content/0/type")
            .and_then(Value::as_str)
            == Some("diff")
    }));

    let (ui_turn, ui_updates) = acp
        .request(
            "session/prompt",
            json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "ui"}]}),
        )
        .await;
    assert_eq!(ui_turn["stopReason"], "end_turn");
    assert!(ui_updates.iter().any(|message| {
        message.get("method").and_then(Value::as_str) == Some("session/request_permission")
    }));
    assert!(ui_updates.iter().any(|notification| {
        notification
            .pointer("/params/update/content/text")
            .and_then(Value::as_str)
            == Some("selected:first")
    }));

    let (listed, _) = acp.request("session/list", json!({"cwd": cwd})).await;
    assert_eq!(listed["sessions"][0]["sessionId"], session_id);

    let (configured, config_updates) = acp
        .request(
            "session/set_config_option",
            json!({"sessionId": session_id, "configId": "model", "value": "openai/gpt-5"}),
        )
        .await;
    assert!(configured["configOptions"].is_array());
    assert!(updates(&config_updates).contains(&"config_option_update"));

    let (_, mode_updates) = acp
        .request(
            "session/set_mode",
            json!({"sessionId": session_id, "modeId": "high"}),
        )
        .await;
    assert!(updates(&mode_updates).contains(&"current_mode_update"));

    acp.request("session/close", json!({"sessionId": session_id}))
        .await;
    let (_, replay) = acp
        .request(
            "session/load",
            json!({"sessionId": session_id, "cwd": cwd, "mcpServers": []}),
        )
        .await;
    assert_eq!(
        updates(&replay),
        ["user_message_chunk", "agent_message_chunk"]
    );

    acp.request(
        "session/resume",
        json!({"sessionId": session_id, "cwd": cwd, "mcpServers": []}),
    )
    .await;

    let (forked, _) = acp
        .request(
            "session/fork",
            json!({"sessionId": session_id, "cwd": cwd, "mcpServers": []}),
        )
        .await;
    assert!(forked["sessionId"].as_str().unwrap().starts_with("fork-"));
}

#[tokio::test]
async fn slash_commands_and_cancellation() {
    let state = TempDir::new().unwrap();
    let mut acp = Harness::start(&state).await;
    acp.request(
        "initialize",
        json!({"protocolVersion": 1, "clientCapabilities": {}}),
    )
    .await;
    let cwd = state.path().canonicalize().unwrap();
    let (created, _) = acp
        .request("session/new", json!({"cwd": cwd, "mcpServers": []}))
        .await;
    let id = created["sessionId"].as_str().unwrap();

    let (response, updates) = acp
        .request(
            "session/prompt",
            json!({"sessionId": id, "prompt": [{"type": "text", "text": "/compact focus"}]}),
        )
        .await;
    assert_eq!(response["stopReason"], "end_turn");
    assert!(updates.iter().any(|value| {
        value
            .pointer("/params/update/content/text")
            .and_then(Value::as_str)
            == Some("compacted")
    }));

    // The official SDK dispatches notifications while a prompt request is in flight.
    let prompt_id = acp.next_id;
    acp.next_id += 1;
    acp.write(json!({"jsonrpc": "2.0", "id": prompt_id, "method": "session/prompt", "params": {"sessionId": id, "prompt": [{"type": "text", "text": "wait"}]}})).await;
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    acp.notify("session/cancel", json!({"sessionId": id})).await;
    loop {
        let mut line = String::new();
        acp.stdout.read_line(&mut line).await.unwrap();
        let value: Value = serde_json::from_str(&line).unwrap();
        if value.get("id") == Some(&json!(prompt_id)) {
            assert_eq!(value["result"]["stopReason"], "cancelled");
            break;
        }
    }
}
