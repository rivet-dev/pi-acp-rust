use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Mutex, RwLock, broadcast, oneshot},
};

use crate::process::{ChildHandle, ProcessBackend};

const MAX_RPC_LINE_BYTES: usize = 16 * 1024 * 1024;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub enum PiEvent {
    Message(Value),
    Exited(String),
}

pub struct PiProcess {
    stdin: Mutex<crate::process::BoxWriter>,
    child: Mutex<Box<dyn ChildHandle>>,
    pending: Arc<RwLock<HashMap<String, oneshot::Sender<anyhow::Result<Value>>>>>,
    events: broadcast::Sender<PiEvent>,
    next_id: AtomicU64,
    closed: AtomicBool,
    pub cwd: PathBuf,
    pub queued_prompts: AtomicUsize,
}

impl PiProcess {
    pub async fn spawn(
        backend: Arc<dyn ProcessBackend>,
        executable: &str,
        cwd: &Path,
        session_dir: &Path,
        session_file: Option<&Path>,
        append_system_prompts: &[String],
        extra_args: &[String],
    ) -> anyhow::Result<Arc<Self>> {
        let mut args = vec![
            "--mode".into(),
            "rpc".into(),
            "--no-themes".into(),
            "--session-dir".into(),
            session_dir.to_string_lossy().into_owned(),
        ];
        if let Some(path) = session_file {
            args.push("--session".into());
            args.push(path.to_string_lossy().into_owned());
        }
        for prompt in append_system_prompts {
            args.push("--append-system-prompt".into());
            args.push(prompt.clone());
        }
        args.extend(extra_args.iter().cloned());

        let spawned = backend.spawn(executable, &args, cwd).await?;
        let (events, _) = broadcast::channel(512);
        let pending = Arc::new(RwLock::new(HashMap::new()));
        let process = Arc::new(Self {
            stdin: Mutex::new(spawned.stdin),
            child: Mutex::new(spawned.child),
            pending: pending.clone(),
            events: events.clone(),
            next_id: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            cwd: cwd.to_path_buf(),
            queued_prompts: AtomicUsize::new(0),
        });

        let process_weak = Arc::downgrade(&process);
        tokio::spawn(async move {
            let mut reader = spawned.stdout;
            let mut buffer = Vec::with_capacity(8192);
            let mut chunk = [0_u8; 8192];
            loop {
                match reader.read(&mut chunk).await {
                    Ok(0) => break,
                    Ok(n) => {
                        buffer.extend_from_slice(&chunk[..n]);
                        if buffer.len() > MAX_RPC_LINE_BYTES {
                            let _ = events.send(PiEvent::Exited(format!(
                                "Pi RPC record exceeded {MAX_RPC_LINE_BYTES} bytes"
                            )));
                            break;
                        }
                        while let Some(position) = buffer.iter().position(|byte| *byte == b'\n') {
                            let mut line: Vec<_> = buffer.drain(..=position).collect();
                            line.pop();
                            if line.last() == Some(&b'\r') {
                                line.pop();
                            }
                            if line.is_empty() {
                                continue;
                            }
                            match serde_json::from_slice::<Value>(&line) {
                                Ok(value) => {
                                    if value.get("type").and_then(Value::as_str) == Some("response")
                                    {
                                        if let Some(id) = value.get("id").and_then(Value::as_str) {
                                            if let Some(tx) = pending.write().await.remove(id) {
                                                let result = if value
                                                    .get("success")
                                                    .and_then(Value::as_bool)
                                                    .unwrap_or(false)
                                                {
                                                    Ok(value
                                                        .get("data")
                                                        .cloned()
                                                        .unwrap_or(Value::Null))
                                                } else {
                                                    Err(anyhow::anyhow!(
                                                        "Pi RPC command failed: {}",
                                                        value
                                                            .get("error")
                                                            .and_then(Value::as_str)
                                                            .unwrap_or("unknown error")
                                                    ))
                                                };
                                                let _ = tx.send(result);
                                                continue;
                                            }
                                        }
                                    }
                                    let _ = events.send(PiEvent::Message(value));
                                }
                                Err(error) => {
                                    let _ = events.send(PiEvent::Exited(format!(
                                        "invalid Pi RPC JSON: {error}"
                                    )));
                                }
                            }
                        }
                    }
                    Err(error) => {
                        let _ =
                            events.send(PiEvent::Exited(format!("Pi RPC read failed: {error}")));
                        break;
                    }
                }
            }

            if let Some(process) = process_weak.upgrade() {
                process.closed.store(true, Ordering::Release);
            }
            let error = "Pi process exited before completing pending commands";
            for (_, tx) in pending.write().await.drain() {
                let _ = tx.send(Err(anyhow::anyhow!(error)));
            }
            let _ = events.send(PiEvent::Exited(error.into()));
        });

        Ok(process)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PiEvent> {
        self.events.subscribe()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub async fn command(&self, command: Value) -> anyhow::Result<Value> {
        if self.is_closed() {
            anyhow::bail!("Pi process is closed");
        }
        let id = format!("pi-acp-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let mut object = command
            .as_object()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Pi RPC command must be an object"))?;
        object.insert("id".into(), Value::String(id.clone()));
        let bytes = serde_json::to_vec(&Value::Object(object))?;
        let (tx, rx) = oneshot::channel();
        self.pending.write().await.insert(id.clone(), tx);
        let write_result = async {
            let mut stdin = self.stdin.lock().await;
            stdin.write_all(&bytes).await?;
            stdin.write_all(b"\n").await?;
            stdin.flush().await
        }
        .await;
        if let Err(error) = write_result {
            self.pending.write().await.remove(&id);
            return Err(error.into());
        }
        match tokio::time::timeout(COMMAND_TIMEOUT, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => anyhow::bail!("Pi RPC command response channel closed"),
            Err(_) => {
                self.pending.write().await.remove(&id);
                anyhow::bail!("Pi RPC command timed out after {COMMAND_TIMEOUT:?}")
            }
        }
    }

    pub async fn get_state(&self) -> anyhow::Result<Value> {
        self.command(json!({"type": "get_state"})).await
    }

    pub async fn get_models(&self) -> anyhow::Result<Value> {
        self.command(json!({"type": "get_available_models"})).await
    }

    pub async fn get_commands(&self) -> anyhow::Result<Value> {
        self.command(json!({"type": "get_commands"})).await
    }

    pub async fn abort(&self) -> anyhow::Result<()> {
        self.command(json!({"type": "abort"})).await?;
        Ok(())
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.child.lock().await.kill().await
    }
}
