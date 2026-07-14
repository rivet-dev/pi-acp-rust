use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use agent_client_protocol_schema::v1::*;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex, RwLock},
};

use crate::{
    acp::{Incoming, Peer, RecordReader, internal_error, invalid_params},
    index::{SessionIndex, StoredSession},
    pi_rpc::{PiEvent, PiProcess},
    process::ProcessBackend,
    translate,
};

const LIST_PAGE_SIZE: usize = 100;
const TURN_TIMEOUT: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Clone)]
pub struct AdapterConfig {
    pub pi_command: String,
    pub state_dir: PathBuf,
    pub append_system_prompts: Vec<String>,
    pub pi_args: Vec<String>,
    pub max_queued_prompts: usize,
    pub max_tracked_tool_calls: usize,
}

struct LiveSession {
    pi: Arc<PiProcess>,
    cwd: PathBuf,
    session_file: RwLock<PathBuf>,
    state: RwLock<Value>,
    models: RwLock<Value>,
    turn: Mutex<()>,
    cancelled: AtomicBool,
}

#[derive(Clone)]
pub struct Adapter {
    config: Arc<AdapterConfig>,
    backend: Arc<dyn ProcessBackend>,
    sessions: Arc<RwLock<HashMap<String, Arc<LiveSession>>>>,
    index: Arc<SessionIndex>,
}

impl Adapter {
    pub fn new(config: AdapterConfig, backend: Arc<dyn ProcessBackend>) -> Self {
        let index = SessionIndex::new(config.state_dir.join("sessions.json"));
        Self {
            config: Arc::new(config),
            backend,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            index: Arc::new(index),
        }
    }

    pub async fn serve<R, W>(self, reader: R, writer: W) -> anyhow::Result<()>
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let peer = Peer::new(writer);
        let mut records = RecordReader::new(reader);
        while let Some(line) = records.next().await? {
            if line.is_empty() {
                continue;
            }
            let incoming = match serde_json::from_slice::<Incoming>(&line) {
                Ok(incoming) if incoming.jsonrpc == "2.0" => incoming,
                Ok(_) => {
                    peer.error(RequestId::Null, Error::invalid_request())
                        .await?;
                    continue;
                }
                Err(error) => {
                    peer.error(
                        RequestId::Null,
                        Error::parse_error().data(error.to_string()),
                    )
                    .await?;
                    continue;
                }
            };
            if let Some(id) = incoming.id.clone() {
                let adapter = self.clone();
                let peer = peer.clone();
                tokio::spawn(async move {
                    if let Err(error) = adapter.handle_request(incoming, id.clone(), &peer).await {
                        if let Err(write_error) = peer.error(id, error).await {
                            tracing::error!(%write_error, "failed to write ACP error response");
                        }
                    }
                });
            } else if let Err(error) = self.handle_notification(incoming).await {
                tracing::warn!(%error, "ACP notification failed");
            }
        }

        let sessions: Vec<_> = self
            .sessions
            .write()
            .await
            .drain()
            .map(|(_, value)| value)
            .collect();
        for session in sessions {
            if let Err(error) = session.pi.close().await {
                tracing::warn!(%error, "failed to close Pi process during ACP shutdown");
            }
        }
        Ok(())
    }

    fn session_dir(&self) -> PathBuf {
        self.config.state_dir.join("pi-sessions")
    }

    async fn spawn_session(
        &self,
        cwd: &Path,
        session_file: Option<&Path>,
        request_meta: Option<&Meta>,
    ) -> anyhow::Result<(String, Arc<LiveSession>)> {
        if !cwd.is_absolute() {
            anyhow::bail!("session cwd must be absolute: {}", cwd.display());
        }
        std::fs::create_dir_all(self.session_dir())?;
        let mut system_prompts = self.config.append_system_prompts.clone();
        if let Some(prompt) = meta_string(request_meta, "systemPrompt") {
            system_prompts.push(prompt);
        }
        let pi = PiProcess::spawn(
            self.backend.clone(),
            &self.config.pi_command,
            cwd,
            &self.session_dir(),
            session_file,
            &system_prompts,
            &self.config.pi_args,
        )
        .await?;
        let (state, models) = tokio::try_join!(pi.get_state(), pi.get_models())?;
        let session_id = state
            .get("sessionId")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let session_file = state
            .get("sessionFile")
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .or_else(|| session_file.map(Path::to_path_buf))
            .ok_or_else(|| anyhow::anyhow!("Pi did not return a persistent session file"))?;
        let session = Arc::new(LiveSession {
            pi,
            cwd: cwd.to_path_buf(),
            session_file: RwLock::new(session_file.clone()),
            state: RwLock::new(state.clone()),
            models: RwLock::new(models),
            turn: Mutex::new(()),
            cancelled: AtomicBool::new(false),
        });
        self.sessions
            .write()
            .await
            .insert(session_id.clone(), session.clone());
        self.index
            .upsert(stored_session(&session_id, cwd, &session_file, &state))
            .await?;
        Ok((session_id, session))
    }

    async fn restore(&self, id: &str) -> anyhow::Result<Arc<LiveSession>> {
        if let Some(session) = self.sessions.read().await.get(id).cloned() {
            if !session.pi.is_closed() {
                return Ok(session);
            }
        }
        let stored = self
            .index
            .get(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("unknown session: {id}"))?;
        let (_, session) = self
            .spawn_session(&stored.cwd, Some(&stored.session_file), None)
            .await?;
        self.sessions
            .write()
            .await
            .insert(id.to_owned(), session.clone());
        Ok(session)
    }

    async fn configuration(&self, session: &LiveSession) -> Vec<SessionConfigOption> {
        let state = session.state.read().await;
        let models = session.models.read().await;
        translate::config_options(&state, &models)
    }

    async fn modes(&self, session: &LiveSession) -> SessionModeState {
        let state = session.state.read().await;
        translate::mode_state(&state)
    }

    async fn notify_commands(
        session_id: String,
        session: Arc<LiveSession>,
        peer: &Peer,
    ) -> anyhow::Result<()> {
        let value = session.pi.get_commands().await?;
        peer.notification(
            "session/update",
            SessionNotification::new(
                session_id,
                SessionUpdate::AvailableCommandsUpdate(AvailableCommandsUpdate::new(
                    translate::commands(&value),
                )),
            ),
        )
        .await
    }

    async fn respond_to_slash(
        &self,
        id: &str,
        session: &LiveSession,
        prompt: &str,
        peer: &Peer,
    ) -> anyhow::Result<Option<PromptResponse>> {
        let trimmed = prompt.trim();
        let Some(command) = trimmed.strip_prefix('/') else {
            return Ok(None);
        };
        let (name, args) = command.split_once(' ').unwrap_or((command, ""));
        let (rpc, output): (Value, Option<String>) = match name {
            "compact" => (
                json!({"type": "compact", "customInstructions": nonempty(args)}),
                None,
            ),
            "session" => (json!({"type": "get_session_stats"}), None),
            "name" => (
                json!({"type": "set_session_name", "name": args}),
                Some("Session name updated.".into()),
            ),
            "steering" => (
                json!({"type": "set_steering_mode", "mode": if args.is_empty() { "one-at-a-time" } else { args }}),
                Some("Steering mode updated.".into()),
            ),
            "follow-up" => (
                json!({"type": "set_follow_up_mode", "mode": if args.is_empty() { "one-at-a-time" } else { args }}),
                Some("Follow-up mode updated.".into()),
            ),
            "export" => (
                json!({"type": "export_html", "outputPath": nonempty(args)}),
                None,
            ),
            _ => return Ok(None),
        };
        let value = session.pi.command(rpc).await?;
        if let Ok(state) = session.pi.get_state().await {
            if let Some(path) = state.get("sessionFile").and_then(Value::as_str) {
                *session.session_file.write().await = PathBuf::from(path);
            }
            *session.state.write().await = state.clone();
            self.index
                .upsert(stored_session(
                    id,
                    &session.cwd,
                    &session.session_file.read().await,
                    &state,
                ))
                .await?;
        }
        let output = output.unwrap_or_else(|| pretty_result(&value));
        peer.notification(
            "session/update",
            SessionNotification::new(
                id.to_owned(),
                translate::text_update("agent_message_chunk", &output)?,
            ),
        )
        .await?;
        Ok(Some(PromptResponse::new(StopReason::EndTurn)))
    }

    async fn prompt(&self, request: PromptRequest, peer: &Peer) -> Result<PromptResponse> {
        let id = request.session_id.to_string();
        let session = self.restore(&id).await.map_err(internal_error)?;
        let queued = session.pi.queued_prompts.fetch_add(1, Ordering::AcqRel);
        if queued >= self.config.max_queued_prompts {
            session.pi.queued_prompts.fetch_sub(1, Ordering::AcqRel);
            return Err(internal_error(format!(
                "Pi prompt queue limit ({}) reached; raise --max-queued-prompts to allow more",
                self.config.max_queued_prompts
            )));
        }
        let _queued = QueueGuard(&session.pi.queued_prompts);
        let _turn = session.turn.lock().await;
        session.cancelled.store(false, Ordering::Release);
        let (message, images) = translate::prompt_to_pi(&request.prompt).map_err(internal_error)?;
        if images.is_empty() {
            if let Some(response) = self
                .respond_to_slash(&id, &session, &message, peer)
                .await
                .map_err(internal_error)?
            {
                return Ok(response);
            }
        }

        let mut events = session.pi.subscribe();
        let mut turn_state = translate::TurnState::new(self.config.max_tracked_tool_calls);
        session
            .pi
            .command(json!({"type": "prompt", "message": message, "images": images}))
            .await
            .map_err(internal_error)?;

        let run = async {
            loop {
                match events.recv().await {
                    Ok(PiEvent::Message(event)) => {
                        for update in
                            translate::pi_event_updates(&event, &session.cwd, &mut turn_state)
                                .map_err(internal_error)?
                        {
                            peer.notification(
                                "session/update",
                                SessionNotification::new(id.clone(), update),
                            )
                            .await
                            .map_err(internal_error)?;
                        }
                        if event.get("type").and_then(Value::as_str) == Some("agent_settled") {
                            break;
                        }
                        if event.get("type").and_then(Value::as_str) == Some("agent_end")
                            && !event
                                .get("willRetry")
                                .and_then(Value::as_bool)
                                .unwrap_or(false)
                        {
                            // Pi releases before `agent_settled` only emit `agent_end`.
                            // Query state so queued continuations and compaction still drain.
                            let state = session.pi.get_state().await.map_err(internal_error)?;
                            let idle = !state
                                .get("isStreaming")
                                .and_then(Value::as_bool)
                                .unwrap_or(false)
                                && !state
                                    .get("isCompacting")
                                    .and_then(Value::as_bool)
                                    .unwrap_or(false)
                                && state
                                    .get("pendingMessageCount")
                                    .and_then(Value::as_u64)
                                    .unwrap_or(0)
                                    == 0;
                            *session.state.write().await = state;
                            if idle {
                                break;
                            }
                        }
                    }
                    Ok(PiEvent::Exited(message)) => return Err(internal_error(message)),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                        return Err(internal_error(format!(
                            "Pi event buffer overflowed by {count} records"
                        )));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        return Err(internal_error("Pi event stream closed"));
                    }
                }
            }
            Ok(())
        };
        tokio::time::timeout(TURN_TIMEOUT, run)
            .await
            .map_err(|_| internal_error(format!("Pi turn exceeded {TURN_TIMEOUT:?}")))??;

        if let Ok(state) = session.pi.get_state().await {
            *session.state.write().await = state.clone();
            if let Err(error) = self
                .index
                .upsert(stored_session(
                    &id,
                    &session.cwd,
                    &session.session_file.read().await,
                    &state,
                ))
                .await
            {
                tracing::warn!(%error, "failed to update Pi ACP session index");
            }
        }
        Ok(PromptResponse::new(
            if session.cancelled.load(Ordering::Acquire) {
                StopReason::Cancelled
            } else {
                StopReason::EndTurn
            },
        ))
    }

    async fn handle_request(&self, incoming: Incoming, id: RequestId, peer: &Peer) -> Result<()> {
        match incoming.method.as_str() {
            "initialize" => {
                let request: InitializeRequest = params(incoming.params)?;
                let capabilities = AgentCapabilities::new()
                    .load_session(true)
                    .prompt_capabilities(
                        PromptCapabilities::new().image(true).embedded_context(true),
                    )
                    .session_capabilities(
                        SessionCapabilities::new()
                            .list(SessionListCapabilities::new())
                            .resume(SessionResumeCapabilities::new())
                            .close(SessionCloseCapabilities::new())
                            .fork(SessionForkCapabilities::new()),
                    );
                peer.result(
                    id,
                    InitializeResponse::new(request.protocol_version)
                        .agent_capabilities(capabilities)
                        .agent_info(
                            Implementation::new("pi-acp-rust", env!("CARGO_PKG_VERSION"))
                                .title("Pi ACP (Rust)"),
                        ),
                )
                .await
                .map_err(internal_error)
            }
            "session/new" => {
                let request: NewSessionRequest = params(incoming.params)?;
                validate_workspace_inputs(&request.mcp_servers, &request.additional_directories)?;
                let (session_id, session) = self
                    .spawn_session(&request.cwd, None, request.meta.as_ref())
                    .await
                    .map_err(internal_error)?;
                let response = NewSessionResponse::new(session_id.clone())
                    .modes(self.modes(&session).await)
                    .config_options(self.configuration(&session).await);
                peer.result(id, response).await.map_err(internal_error)?;
                Self::notify_commands(session_id, session, peer)
                    .await
                    .map_err(internal_error)
            }
            "session/load" => {
                let request: LoadSessionRequest = params(incoming.params)?;
                validate_workspace_inputs(&request.mcp_servers, &request.additional_directories)?;
                let session_id = request.session_id.to_string();
                let session = self.restore(&session_id).await.map_err(internal_error)?;
                let messages = session
                    .pi
                    .command(json!({"type": "get_messages"}))
                    .await
                    .map_err(internal_error)?;
                for update in
                    translate::message_history_updates(&messages).map_err(internal_error)?
                {
                    peer.notification(
                        "session/update",
                        SessionNotification::new(session_id.clone(), update),
                    )
                    .await
                    .map_err(internal_error)?;
                }
                peer.result(
                    id,
                    LoadSessionResponse::new()
                        .modes(self.modes(&session).await)
                        .config_options(self.configuration(&session).await),
                )
                .await
                .map_err(internal_error)
            }
            "session/resume" => {
                let request: ResumeSessionRequest = params(incoming.params)?;
                validate_workspace_inputs(&request.mcp_servers, &request.additional_directories)?;
                let session = self
                    .restore(&request.session_id.to_string())
                    .await
                    .map_err(internal_error)?;
                peer.result(
                    id,
                    ResumeSessionResponse::new()
                        .modes(self.modes(&session).await)
                        .config_options(self.configuration(&session).await),
                )
                .await
                .map_err(internal_error)
            }
            "session/list" => {
                let request: ListSessionsRequest = params(incoming.params)?;
                let sessions = self
                    .index
                    .list(request.cwd.as_deref())
                    .await
                    .map_err(internal_error)?;
                let offset = request
                    .cursor
                    .as_deref()
                    .unwrap_or("0")
                    .parse::<usize>()
                    .unwrap_or(0);
                let page = sessions
                    .iter()
                    .skip(offset)
                    .take(LIST_PAGE_SIZE)
                    .map(|session| {
                        SessionInfo::new(session.session_id.clone(), session.cwd.clone())
                            .title(session.title.clone())
                            .updated_at(session.updated_at.clone())
                    })
                    .collect();
                let next = (offset + LIST_PAGE_SIZE < sessions.len())
                    .then(|| (offset + LIST_PAGE_SIZE).to_string());
                peer.result(id, ListSessionsResponse::new(page).next_cursor(next))
                    .await
                    .map_err(internal_error)
            }
            "session/close" => {
                let request: CloseSessionRequest = params(incoming.params)?;
                let session_id = request.session_id.to_string();
                if let Some(session) = self.sessions.write().await.remove(&session_id) {
                    session.pi.close().await.map_err(internal_error)?;
                }
                peer.result(id, CloseSessionResponse::new())
                    .await
                    .map_err(internal_error)
            }
            "session/fork" => {
                let request: ForkSessionRequest = params(incoming.params)?;
                validate_workspace_inputs(&request.mcp_servers, &request.additional_directories)?;
                let source_id = request.session_id.to_string();
                let stored = self
                    .index
                    .get(&source_id)
                    .await
                    .map_err(internal_error)?
                    .ok_or_else(|| internal_error(format!("unknown session: {source_id}")))?;
                let original_live = self.sessions.read().await.get(&source_id).cloned();
                let (temporary_id, session) = self
                    .spawn_session(
                        &request.cwd,
                        Some(&stored.session_file),
                        request.meta.as_ref(),
                    )
                    .await
                    .map_err(internal_error)?;
                if let Some(original) = original_live {
                    self.sessions
                        .write()
                        .await
                        .insert(source_id.clone(), original);
                } else {
                    self.sessions.write().await.remove(&temporary_id);
                }
                session
                    .pi
                    .command(json!({"type": "clone"}))
                    .await
                    .map_err(internal_error)?;
                let state = session.pi.get_state().await.map_err(internal_error)?;
                let fork_id = state
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .unwrap_or(&source_id)
                    .to_owned();
                let fork_file = state
                    .get("sessionFile")
                    .and_then(Value::as_str)
                    .map(PathBuf::from)
                    .ok_or_else(|| internal_error("Pi clone did not return a session file"))?;
                *session.session_file.write().await = fork_file.clone();
                *session.state.write().await = state.clone();
                self.sessions
                    .write()
                    .await
                    .insert(fork_id.clone(), session.clone());
                self.index
                    .upsert(stored_session(&fork_id, &session.cwd, &fork_file, &state))
                    .await
                    .map_err(internal_error)?;
                peer.result(
                    id,
                    ForkSessionResponse::new(fork_id)
                        .modes(self.modes(&session).await)
                        .config_options(self.configuration(&session).await),
                )
                .await
                .map_err(internal_error)
            }
            "session/set_mode" => {
                let request: SetSessionModeRequest = params(incoming.params)?;
                let session = self
                    .restore(&request.session_id.to_string())
                    .await
                    .map_err(internal_error)?;
                let mode = request.mode_id.to_string();
                session
                    .pi
                    .command(json!({"type": "set_thinking_level", "level": mode}))
                    .await
                    .map_err(internal_error)?;
                if let Ok(state) = session.pi.get_state().await {
                    *session.state.write().await = state;
                }
                peer.notification(
                    "session/update",
                    SessionNotification::new(
                        request.session_id,
                        SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(request.mode_id)),
                    ),
                )
                .await
                .map_err(internal_error)?;
                peer.result(id, SetSessionModeResponse::new())
                    .await
                    .map_err(internal_error)
            }
            "session/set_config_option" => {
                let request: SetSessionConfigOptionRequest = params(incoming.params)?;
                let session = self
                    .restore(&request.session_id.to_string())
                    .await
                    .map_err(internal_error)?;
                let value = serde_json::to_value(&request.value)
                    .ok()
                    .and_then(|value| {
                        value
                            .get("value")
                            .and_then(Value::as_str)
                            .or_else(|| value.as_str())
                            .map(str::to_owned)
                    })
                    .unwrap_or_default();
                let command = match request.config_id.to_string().as_str() {
                    "model" => value.split_once('/').map(|(provider, model)| {
                        json!({"type": "set_model", "provider": provider, "modelId": model})
                    }),
                    "thought_level" => {
                        Some(json!({"type": "set_thinking_level", "level": value}))
                    }
                    _ => None,
                }
                .ok_or_else(|| {
                    invalid_params(format!("unknown config option: {}", request.config_id))
                })?;
                session.pi.command(command).await.map_err(internal_error)?;
                if let Ok(state) = session.pi.get_state().await {
                    *session.state.write().await = state;
                }
                let config_options = self.configuration(&session).await;
                peer.notification(
                    "session/update",
                    SessionNotification::new(
                        request.session_id,
                        SessionUpdate::ConfigOptionUpdate(ConfigOptionUpdate::new(
                            config_options.clone(),
                        )),
                    ),
                )
                .await
                .map_err(internal_error)?;
                peer.result(id, SetSessionConfigOptionResponse::new(config_options))
                    .await
                    .map_err(internal_error)
            }
            "session/prompt" => {
                let request: PromptRequest = params(incoming.params)?;
                let response = self.prompt(request, peer).await?;
                peer.result(id, response).await.map_err(internal_error)
            }
            _ => Err(Error::method_not_found().data(incoming.method)),
        }
    }

    async fn handle_notification(&self, incoming: Incoming) -> Result<()> {
        match incoming.method.as_str() {
            "session/cancel" => {
                let notification: CancelNotification = params(incoming.params)?;
                if let Some(session) = self
                    .sessions
                    .read()
                    .await
                    .get(&notification.session_id.to_string())
                    .cloned()
                {
                    session.cancelled.store(true, Ordering::Release);
                    session.pi.abort().await.map_err(internal_error)?;
                }
                Ok(())
            }
            _ => Err(Error::method_not_found().data(incoming.method)),
        }
    }
}

struct QueueGuard<'a>(&'a std::sync::atomic::AtomicUsize);

impl Drop for QueueGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn params<T: DeserializeOwned>(value: Option<Value>) -> Result<T> {
    serde_json::from_value(value.unwrap_or_else(|| json!({}))).map_err(invalid_params)
}

fn validate_workspace_inputs(
    mcp_servers: &[McpServer],
    additional_directories: &[PathBuf],
) -> Result<()> {
    if !mcp_servers.is_empty() {
        return Err(invalid_params(
            "Pi does not provide native MCP support; install a Pi extension or expose the MCP tool as a CLI",
        ));
    }
    if !additional_directories.is_empty() {
        return Err(invalid_params(
            "Pi RPC sessions do not support ACP additionalDirectories",
        ));
    }
    Ok(())
}

fn meta_string(meta: Option<&Meta>, key: &str) -> Option<String> {
    meta.and_then(|meta| serde_json::to_value(meta).ok())
        .and_then(|value| value.get(key).and_then(Value::as_str).map(str::to_owned))
}

fn nonempty(value: &str) -> Option<&str> {
    (!value.trim().is_empty()).then_some(value.trim())
}

fn pretty_result(value: &Value) -> String {
    if let Some(path) = value.get("path").and_then(Value::as_str) {
        return format!("Exported session to {path}");
    }
    if let Some(summary) = value.get("summary").and_then(Value::as_str) {
        return summary.to_owned();
    }
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn stored_session(id: &str, cwd: &Path, file: &Path, state: &Value) -> StoredSession {
    let updated_at = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .ok();
    StoredSession {
        session_id: id.to_owned(),
        cwd: cwd.to_path_buf(),
        session_file: file.to_path_buf(),
        title: state
            .get("sessionName")
            .and_then(Value::as_str)
            .map(str::to_owned),
        updated_at,
    }
}
