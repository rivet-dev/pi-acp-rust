use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StoredSession {
    pub session_id: String,
    pub cwd: PathBuf,
    pub session_file: PathBuf,
    pub title: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct IndexFile {
    sessions: BTreeMap<String, StoredSession>,
}

pub struct SessionIndex {
    path: PathBuf,
    gate: Mutex<()>,
}

impl SessionIndex {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            gate: Mutex::new(()),
        }
    }

    async fn read_unlocked(&self) -> anyhow::Result<IndexFile> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(IndexFile::default()),
            Err(error) => Err(error.into()),
        }
    }

    async fn write_unlocked(&self, index: &IndexFile) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temporary = self
            .path
            .with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        std::fs::write(&temporary, serde_json::to_vec_pretty(index)?)?;
        std::fs::rename(&temporary, &self.path)?;
        Ok(())
    }

    pub async fn list(&self, cwd: Option<&Path>) -> anyhow::Result<Vec<StoredSession>> {
        let _guard = self.gate.lock().await;
        let mut sessions: Vec<_> = self
            .read_unlocked()
            .await?
            .sessions
            .into_values()
            .filter(|session| cwd.is_none_or(|cwd| session.cwd == cwd))
            .collect();
        sessions.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        Ok(sessions)
    }

    pub async fn get(&self, id: &str) -> anyhow::Result<Option<StoredSession>> {
        let _guard = self.gate.lock().await;
        Ok(self.read_unlocked().await?.sessions.remove(id))
    }

    pub async fn upsert(&self, session: StoredSession) -> anyhow::Result<()> {
        let _guard = self.gate.lock().await;
        let mut index = self.read_unlocked().await?;
        index.sessions.insert(session.session_id.clone(), session);
        self.write_unlocked(&index).await
    }

    pub async fn delete(&self, id: &str) -> anyhow::Result<Option<StoredSession>> {
        let _guard = self.gate.lock().await;
        let mut index = self.read_unlocked().await?;
        let removed = index.sessions.remove(id);
        self.write_unlocked(&index).await?;
        Ok(removed)
    }
}
