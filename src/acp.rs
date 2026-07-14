use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use agent_client_protocol_schema::v1::{Error, RequestId};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{Mutex, oneshot},
};

pub const MAX_ACP_RECORD_BYTES: usize = 16 * 1024 * 1024;
const CLIENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, serde::Deserialize)]
pub struct Incoming {
    #[allow(dead_code)]
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Option<RequestId>,
    #[serde(default)]
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<Value>,
}

#[derive(Clone)]
pub struct Peer {
    writer: Arc<Mutex<Box<dyn AsyncWrite + Send + Unpin>>>,
    pending: Arc<Mutex<HashMap<RequestId, oneshot::Sender<anyhow::Result<Value>>>>>,
    next_id: Arc<AtomicI64>,
}

impl Peer {
    pub fn new(writer: impl AsyncWrite + Send + Unpin + 'static) -> Self {
        Self {
            writer: Arc::new(Mutex::new(Box::new(writer))),
            pending: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(AtomicI64::new(1_000_000_000)),
        }
    }

    pub async fn result(&self, id: RequestId, result: impl Serialize) -> anyhow::Result<()> {
        self.write(&json!({"jsonrpc": "2.0", "id": id, "result": result}))
            .await
    }

    pub async fn error(&self, id: RequestId, error: Error) -> anyhow::Result<()> {
        self.write(&json!({"jsonrpc": "2.0", "id": id, "error": error}))
            .await
    }

    pub async fn notification(&self, method: &str, params: impl Serialize) -> anyhow::Result<()> {
        self.write(&json!({"jsonrpc": "2.0", "method": method, "params": params}))
            .await
    }

    pub async fn request(&self, method: &str, params: impl Serialize) -> anyhow::Result<Value> {
        let id = RequestId::Number(self.next_id.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id.clone(), tx);
        if let Err(error) = self
            .write(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await
        {
            self.pending.lock().await.remove(&id);
            return Err(error);
        }
        match tokio::time::timeout(CLIENT_REQUEST_TIMEOUT, rx).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => anyhow::bail!("ACP client response channel closed"),
            Err(_) => {
                self.pending.lock().await.remove(&id);
                anyhow::bail!(
                    "ACP client request {method} timed out after {CLIENT_REQUEST_TIMEOUT:?}"
                )
            }
        }
    }

    pub async fn receive_response(
        &self,
        id: RequestId,
        result: Option<Value>,
        error: Option<Value>,
    ) {
        let Some(tx) = self.pending.lock().await.remove(&id) else {
            tracing::warn!(%id, "received ACP response for an unknown request");
            return;
        };
        let response = match (result, error) {
            (Some(result), None) => Ok(result),
            (_, Some(error)) => Err(anyhow::anyhow!("ACP client request failed: {error}")),
            _ => Err(anyhow::anyhow!(
                "ACP client response had no result or error"
            )),
        };
        let _ = tx.send(response);
    }

    async fn write(&self, value: &Value) -> anyhow::Result<()> {
        let mut bytes = serde_json::to_vec(value)?;
        if bytes.len() > MAX_ACP_RECORD_BYTES {
            anyhow::bail!("ACP output record exceeded {MAX_ACP_RECORD_BYTES} bytes");
        }
        bytes.push(b'\n');
        let mut writer = self.writer.lock().await;
        writer.write_all(&bytes).await?;
        writer.flush().await?;
        Ok(())
    }
}

pub struct RecordReader<R> {
    reader: R,
    buffer: Vec<u8>,
}

impl<R: AsyncRead + Unpin> RecordReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: Vec::with_capacity(8192),
        }
    }

    pub async fn next(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        loop {
            if let Some(position) = self.buffer.iter().position(|byte| *byte == b'\n') {
                let mut line: Vec<_> = self.buffer.drain(..=position).collect();
                line.pop();
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                return Ok(Some(line));
            }
            if self.buffer.len() >= MAX_ACP_RECORD_BYTES {
                anyhow::bail!("ACP input record exceeded {MAX_ACP_RECORD_BYTES} bytes");
            }
            let mut chunk = [0_u8; 8192];
            let read = self.reader.read(&mut chunk).await?;
            if read == 0 {
                return if self.buffer.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(std::mem::take(&mut self.buffer)))
                };
            }
            self.buffer.extend_from_slice(&chunk[..read]);
        }
    }
}

pub fn internal_error(error: impl ToString) -> Error {
    Error::internal_error().data(error.to_string())
}

pub fn invalid_params(error: impl ToString) -> Error {
    Error::invalid_params().data(error.to_string())
}
