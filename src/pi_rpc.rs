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
const MAX_STDERR_TAIL_BYTES: usize = 64 * 1024;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const STDERR_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

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
    closing: AtomicBool,
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

        let spawned = backend
            .spawn(executable, &args, cwd)
            .await
            .map_err(|error| anyhow::anyhow!("spawn Pi RPC process {executable}: {error:#}"))?;
        let (events, _) = broadcast::channel(512);
        let pending = Arc::new(RwLock::new(HashMap::new()));
        let process = Arc::new(Self {
            stdin: Mutex::new(spawned.stdin),
            child: Mutex::new(spawned.child),
            pending: pending.clone(),
            events: events.clone(),
            next_id: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            closing: AtomicBool::new(false),
            cwd: cwd.to_path_buf(),
            queued_prompts: AtomicUsize::new(0),
        });

        let stderr_task = tokio::spawn(capture_stderr(spawned.stderr));
        let process_weak = Arc::downgrade(&process);
        tokio::spawn(async move {
            let mut reader = spawned.stdout;
            let mut buffer = Vec::with_capacity(8192);
            let mut chunk = [0_u8; 8192];
            let mut primary_failure = None;
            'read: loop {
                match reader.read(&mut chunk).await {
                    Ok(0) => break,
                    Ok(n) => {
                        buffer.extend_from_slice(&chunk[..n]);
                        if buffer.len() > MAX_RPC_LINE_BYTES {
                            primary_failure =
                                Some(format!("Pi RPC record exceeded {MAX_RPC_LINE_BYTES} bytes"));
                            break 'read;
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
                                    primary_failure = Some(format!("invalid Pi RPC JSON: {error}"));
                                    break 'read;
                                }
                            }
                        }
                    }
                    Err(error) => {
                        primary_failure = Some(format!("Pi RPC read failed: {error}"));
                        break 'read;
                    }
                }
            }

            let (status, closing) = if let Some(process) = process_weak.upgrade() {
                let closing = process.closing.load(Ordering::Acquire);
                let mut child = process.child.lock().await;
                let kill_error = if primary_failure.is_some() {
                    child.kill().await.err()
                } else {
                    None
                };
                let status = child.wait().await.map_err(|wait_error| match kill_error {
                    Some(kill_error) => anyhow::anyhow!(
                        "kill after RPC failure failed: {kill_error:#}; wait failed: {wait_error:#}"
                    ),
                    None => wait_error,
                });
                process.closed.store(true, Ordering::Release);
                (status, closing)
            } else {
                (Err(anyhow::anyhow!("Pi process handle was dropped")), false)
            };
            let (stderr, stderr_error) =
                match tokio::time::timeout(STDERR_DRAIN_TIMEOUT, stderr_task).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(error)) => (Vec::new(), Some(format!("stderr task failed: {error}"))),
                    Err(_) => (
                        Vec::new(),
                        Some(format!(
                            "stderr did not close within {STDERR_DRAIN_TIMEOUT:?}"
                        )),
                    ),
                };
            let reason = exit_reason(primary_failure, status, closing, &stderr, stderr_error);
            for (_, tx) in pending.write().await.drain() {
                let _ = tx.send(Err(anyhow::anyhow!(reason.clone())));
            }
            let _ = events.send(PiEvent::Exited(reason));
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

    pub async fn extension_ui_response(&self, response: Value) -> anyhow::Result<()> {
        if self.is_closed() {
            anyhow::bail!("Pi process is closed");
        }
        let mut object = response
            .as_object()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Pi extension UI response must be an object"))?;
        object.insert("type".into(), Value::String("extension_ui_response".into()));
        let bytes = serde_json::to_vec(&Value::Object(object))?;
        if bytes.len() > MAX_RPC_LINE_BYTES {
            anyhow::bail!("Pi RPC record exceeded {MAX_RPC_LINE_BYTES} bytes");
        }
        let mut stdin = self.stdin.lock().await;
        stdin.write_all(&bytes).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;
        Ok(())
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.closing.store(true, Ordering::Release);
        let mut child = self.child.lock().await;
        if let Err(kill_error) = child.kill().await {
            child.wait().await.map(|_| ()).map_err(|wait_error| {
                anyhow::anyhow!(
                    "close Pi process: kill failed: {kill_error:#}; wait failed: {wait_error:#}"
                )
            })?;
        }
        Ok(())
    }
}

async fn capture_stderr(mut reader: crate::process::BoxReader) -> (Vec<u8>, Option<String>) {
    let mut tail = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) => return (tail, None),
            Ok(read) => append_tail(&mut tail, &chunk[..read]),
            Err(error) => return (tail, Some(format!("Pi stderr read failed: {error}"))),
        }
    }
}

fn append_tail(tail: &mut Vec<u8>, chunk: &[u8]) {
    if chunk.len() >= MAX_STDERR_TAIL_BYTES {
        tail.clear();
        tail.extend_from_slice(&chunk[chunk.len() - MAX_STDERR_TAIL_BYTES..]);
        return;
    }
    let overflow = tail
        .len()
        .saturating_add(chunk.len())
        .saturating_sub(MAX_STDERR_TAIL_BYTES);
    if overflow > 0 {
        tail.drain(..overflow);
    }
    tail.extend_from_slice(chunk);
}

fn exit_reason(
    primary_failure: Option<String>,
    status: anyhow::Result<i32>,
    closing: bool,
    stderr: &[u8],
    stderr_error: Option<String>,
) -> String {
    let status = match status {
        Ok(status) => format!("exit status {status}"),
        Err(error) => format!("exit status unavailable: {error:#}"),
    };
    let mut reason = match primary_failure {
        Some(primary) => format!("{primary}; {status}"),
        None if closing => format!("Pi process closed; {status}"),
        None => format!("Pi process exited; {status}"),
    };
    let stderr = String::from_utf8_lossy(stderr);
    let stderr = stderr.trim();
    if !stderr.is_empty() {
        reason.push_str("; stderr: ");
        reason.push_str(stderr);
    }
    if let Some(error) = stderr_error {
        reason.push_str("; ");
        reason.push_str(&error);
    }
    reason
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stderr_tail_is_bounded_and_keeps_the_end() {
        let mut tail = vec![b'a'; MAX_STDERR_TAIL_BYTES - 2];
        append_tail(&mut tail, b"bcde");
        assert_eq!(tail.len(), MAX_STDERR_TAIL_BYTES);
        assert_eq!(&tail[tail.len() - 4..], b"bcde");

        append_tail(&mut tail, &vec![b'z'; MAX_STDERR_TAIL_BYTES + 10]);
        assert_eq!(tail, vec![b'z'; MAX_STDERR_TAIL_BYTES]);
    }

    #[test]
    fn primary_rpc_error_is_not_masked_by_teardown_errors() {
        let reason = exit_reason(
            Some("Pi RPC read failed: broken pipe".into()),
            Err(anyhow::anyhow!("wait failed")),
            false,
            b"fatal details\n",
            None,
        );
        assert!(reason.starts_with("Pi RPC read failed: broken pipe"));
        assert!(reason.contains("wait failed"));
        assert!(reason.contains("stderr: fatal details"));
    }
}
