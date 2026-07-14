use std::sync::Arc;

use agent_client_protocol_schema::v1::{Error, RequestId};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::Mutex,
};

pub const MAX_ACP_RECORD_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, serde::Deserialize)]
pub struct Incoming {
    #[allow(dead_code)]
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Option<RequestId>,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
}

#[derive(Clone)]
pub struct Peer {
    writer: Arc<Mutex<Box<dyn AsyncWrite + Send + Unpin>>>,
}

impl Peer {
    pub fn new(writer: impl AsyncWrite + Send + Unpin + 'static) -> Self {
        Self {
            writer: Arc::new(Mutex::new(Box::new(writer))),
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
