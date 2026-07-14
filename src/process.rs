use std::{path::Path, process::Stdio};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

pub type BoxReader = Box<dyn AsyncRead + Send + Unpin>;
pub type BoxWriter = Box<dyn AsyncWrite + Send + Unpin>;

pub struct SpawnedProcess {
    pub stdin: BoxWriter,
    pub stdout: BoxReader,
    pub stderr: BoxReader,
    pub child: Box<dyn ChildHandle>,
}

#[async_trait]
pub trait ChildHandle: Send + Sync {
    async fn kill(&mut self) -> anyhow::Result<()>;
    async fn wait(&mut self) -> anyhow::Result<i32>;
    fn id(&self) -> Option<u32>;
}

#[async_trait]
pub trait ProcessBackend: Send + Sync {
    async fn spawn(
        &self,
        executable: &str,
        args: &[String],
        cwd: &Path,
    ) -> anyhow::Result<SpawnedProcess>;
}

#[derive(Debug, Default)]
pub struct NativeProcessBackend;

struct TokioChild {
    child: tokio::process::Child,
    status: Option<i32>,
}

fn exit_code(status: std::process::ExitStatus) -> i32 {
    status.code().unwrap_or(128)
}

#[async_trait]
impl ChildHandle for TokioChild {
    async fn kill(&mut self) -> anyhow::Result<()> {
        if self.status.is_some() {
            return Ok(());
        }
        if let Some(status) = self.child.try_wait()? {
            self.status = Some(exit_code(status));
            return Ok(());
        }
        self.child.kill().await?;
        Ok(())
    }

    async fn wait(&mut self) -> anyhow::Result<i32> {
        if let Some(status) = self.status {
            return Ok(status);
        }
        let status = exit_code(self.child.wait().await?);
        self.status = Some(status);
        Ok(status)
    }

    fn id(&self) -> Option<u32> {
        self.child.id()
    }
}

#[async_trait]
impl ProcessBackend for NativeProcessBackend {
    async fn spawn(
        &self,
        executable: &str,
        args: &[String],
        cwd: &Path,
    ) -> anyhow::Result<SpawnedProcess> {
        let mut child = tokio::process::Command::new(executable)
            .args(args)
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("Pi child stdin was not piped"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("Pi child stdout was not piped"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow::anyhow!("Pi child stderr was not piped"))?;
        Ok(SpawnedProcess {
            stdin: Box::new(stdin),
            stdout: Box::new(stdout),
            stderr: Box::new(stderr),
            child: Box::new(TokioChild {
                child,
                status: None,
            }),
        })
    }
}
