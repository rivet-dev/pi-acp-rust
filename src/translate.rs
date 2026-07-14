use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
};

use agent_client_protocol_schema::v1::{
    AvailableCommand, ContentBlock, SessionConfigOption, SessionConfigOptionCategory,
    SessionConfigSelectGroup, SessionConfigSelectOption, SessionMode, SessionModeState,
    SessionUpdate,
};
use serde_json::{Value, json};

pub fn prompt_to_pi(blocks: &[ContentBlock]) -> anyhow::Result<(String, Vec<Value>)> {
    let mut text = Vec::new();
    let mut images = Vec::new();
    for block in blocks {
        match block {
            ContentBlock::Text(content) => text.push(content.text.clone()),
            ContentBlock::Image(image) => images.push(json!({
                "type": "image",
                "data": image.data,
                "mimeType": image.mime_type,
            })),
            ContentBlock::ResourceLink(link) => {
                text.push(format!("[Resource: {}]({})", link.name, link.uri));
            }
            ContentBlock::Resource(resource) => {
                text.push(format!(
                    "<context>\n{}\n</context>",
                    embedded_text(resource)?
                ));
            }
            ContentBlock::Audio(_) => anyhow::bail!("Pi does not support audio prompt content"),
            _ => anyhow::bail!("unsupported ACP prompt content"),
        }
    }
    Ok((text.join("\n\n"), images))
}

fn embedded_text(value: &impl serde::Serialize) -> anyhow::Result<String> {
    let value = serde_json::to_value(value)?;
    Ok(value
        .pointer("/resource/text")
        .and_then(Value::as_str)
        .or_else(|| value.get("text").and_then(Value::as_str))
        .unwrap_or_else(|| {
            value
                .pointer("/resource/uri")
                .and_then(Value::as_str)
                .unwrap_or("")
        })
        .to_owned())
}

pub fn text_update(kind: &str, text: &str) -> anyhow::Result<SessionUpdate> {
    Ok(serde_json::from_value(json!({
        "sessionUpdate": kind,
        "content": {"type": "text", "text": text},
    }))?)
}

pub struct TurnState {
    streamed_text: HashSet<i64>,
    edit_snapshots: HashMap<String, (PathBuf, String)>,
    max_tracked_tool_calls: usize,
    warned_near_limit: bool,
}

impl TurnState {
    pub fn new(max_tracked_tool_calls: usize) -> Self {
        Self {
            streamed_text: HashSet::new(),
            edit_snapshots: HashMap::new(),
            max_tracked_tool_calls,
            warned_near_limit: false,
        }
    }
}

pub fn pi_event_updates(
    event: &Value,
    cwd: &Path,
    state: &mut TurnState,
) -> anyhow::Result<Vec<SessionUpdate>> {
    let mut updates = Vec::new();
    match event.get("type").and_then(Value::as_str) {
        Some("message_update") => {
            let delta = &event["assistantMessageEvent"];
            match delta.get("type").and_then(Value::as_str) {
                Some("text_delta") => {
                    if let Some(text) = delta.get("delta").and_then(Value::as_str) {
                        state.streamed_text.insert(content_index(delta));
                        updates.push(text_update("agent_message_chunk", text)?);
                    }
                }
                Some("text_end") => {
                    let index = content_index(delta);
                    if !state.streamed_text.contains(&index) {
                        if let Some(text) = delta.get("content").and_then(Value::as_str) {
                            updates.push(text_update("agent_message_chunk", text)?);
                        }
                    }
                }
                Some("thinking_delta") => {
                    if let Some(text) = delta.get("delta").and_then(Value::as_str) {
                        updates.push(text_update("agent_thought_chunk", text)?);
                    }
                }
                _ => {}
            }
        }
        Some("tool_execution_start") => {
            let tool_name = event.get("toolName").and_then(Value::as_str);
            let location = tool_location(event.get("args"), cwd);
            if tool_name == Some("edit") {
                if let Some(path) = location.as_ref() {
                    if let Ok(old_text) = std::fs::read_to_string(path) {
                        if let Some(id) = event.get("toolCallId").and_then(Value::as_str) {
                            if state.edit_snapshots.len() >= state.max_tracked_tool_calls {
                                anyhow::bail!(
                                    "tracked edit tool limit ({}) reached; raise --max-tracked-tool-calls to allow more",
                                    state.max_tracked_tool_calls
                                );
                            }
                            if !state.warned_near_limit
                                && state.edit_snapshots.len().saturating_add(1) * 5
                                    >= state.max_tracked_tool_calls * 4
                            {
                                tracing::warn!(
                                    current = state.edit_snapshots.len().saturating_add(1),
                                    limit = state.max_tracked_tool_calls,
                                    "Pi ACP tracked edit tools are near the configured limit"
                                );
                                state.warned_near_limit = true;
                            }
                            state
                                .edit_snapshots
                                .insert(id.to_owned(), (path.clone(), old_text));
                        }
                    }
                }
            }
            updates.push(serde_json::from_value(json!({
                "sessionUpdate": "tool_call",
                "toolCallId": event["toolCallId"],
                "title": tool_title(event),
                "kind": tool_kind(event.get("toolName").and_then(Value::as_str)),
                "status": "in_progress",
                "locations": location.map(|path| vec![json!({"path": path})]),
                "rawInput": event.get("args").cloned().unwrap_or(Value::Null),
            }))?);
        }
        Some("tool_execution_update") => {
            updates.push(serde_json::from_value(json!({
                "sessionUpdate": "tool_call_update",
                "toolCallId": event["toolCallId"],
                "status": "in_progress",
                "content": tool_content(event.get("partialResult")),
            }))?);
        }
        Some("tool_execution_end") => {
            let tool_call_id = event
                .get("toolCallId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let mut content = tool_content(event.get("result"));
            if let Some((path, old_text)) = state.edit_snapshots.remove(tool_call_id) {
                if let Ok(new_text) = std::fs::read_to_string(&path) {
                    if new_text != old_text {
                        content.insert(
                            0,
                            json!({"type": "diff", "path": path, "oldText": old_text, "newText": new_text}),
                        );
                    }
                }
            }
            updates.push(serde_json::from_value(json!({
                "sessionUpdate": "tool_call_update",
                "toolCallId": event["toolCallId"],
                "status": if event.get("isError").and_then(Value::as_bool).unwrap_or(false) { "failed" } else { "completed" },
                "content": content,
                "rawOutput": event.get("result").cloned().unwrap_or(Value::Null),
            }))?);
        }
        Some("extension_error") | Some("auto_retry_start") => {
            let message = event
                .get("error")
                .or_else(|| event.get("errorMessage"))
                .and_then(Value::as_str)
                .unwrap_or("Pi is retrying after an error");
            updates.push(text_update("agent_thought_chunk", message)?);
        }
        _ => {}
    }
    Ok(updates)
}

fn content_index(event: &Value) -> i64 {
    event
        .get("contentIndex")
        .and_then(Value::as_i64)
        .unwrap_or(-1)
}

fn tool_location(args: Option<&Value>, cwd: &Path) -> Option<PathBuf> {
    let path = args?.get("path")?.as_str()?;
    let path = Path::new(path);
    Some(if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    })
}

fn tool_title(event: &Value) -> String {
    let name = event
        .get("toolName")
        .and_then(Value::as_str)
        .unwrap_or("tool");
    if name == "bash" {
        if let Some(command) = event.pointer("/args/command").and_then(Value::as_str) {
            return command.to_owned();
        }
    }
    name.to_owned()
}

fn tool_kind(name: Option<&str>) -> &'static str {
    match name.unwrap_or_default() {
        "read" | "grep" | "find" | "ls" => "search",
        "write" => "edit",
        "bash" => "execute",
        _ => "other",
    }
}

fn tool_content(result: Option<&Value>) -> Vec<Value> {
    result
        .and_then(|result| result.get("content"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|content| match content.get("type").and_then(Value::as_str) {
            Some("text") => Some(json!({
                "type": "content",
                "content": {"type": "text", "text": content.get("text").and_then(Value::as_str).unwrap_or("")}
            })),
            Some("image") => Some(json!({"type": "content", "content": content})),
            _ => None,
        })
        .collect()
}

pub fn commands(value: &Value) -> Vec<AvailableCommand> {
    let mut result = vec![
        AvailableCommand::new("compact", "Compact the session context"),
        AvailableCommand::new("session", "Show session usage and cost"),
        AvailableCommand::new("name", "Set the session display name"),
        AvailableCommand::new("steering", "Set Pi steering queue behavior"),
        AvailableCommand::new("follow-up", "Set Pi follow-up queue behavior"),
        AvailableCommand::new("export", "Export the session as HTML"),
    ];
    for command in value
        .get("commands")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(name) = command.get("name").and_then(Value::as_str) else {
            continue;
        };
        let description = command
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("Pi command");
        if !result.iter().any(|existing| existing.name == name) {
            result.push(AvailableCommand::new(name, description));
        }
    }
    result
}

pub fn config_options(state: &Value, available: &Value) -> Vec<SessionConfigOption> {
    let current_model = state
        .pointer("/model/id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let current_provider = state
        .pointer("/model/provider")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let current_model_value = format!("{current_provider}/{current_model}");

    let mut providers: BTreeMap<String, Vec<SessionConfigSelectOption>> = BTreeMap::new();
    for model in available
        .get("models")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(provider) = model.get("provider").and_then(Value::as_str) else {
            continue;
        };
        let Some(id) = model.get("id").and_then(Value::as_str) else {
            continue;
        };
        let name = model.get("name").and_then(Value::as_str).unwrap_or(id);
        providers.entry(provider.to_owned()).or_default().push(
            SessionConfigSelectOption::new(format!("{provider}/{id}"), name).description(
                model
                    .get("description")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
        );
    }
    let groups = providers
        .into_iter()
        .map(|(provider, options)| {
            SessionConfigSelectGroup::new(provider.clone(), provider, options)
        })
        .collect::<Vec<_>>();

    let mut result = Vec::new();
    if !groups.is_empty() {
        result.push(
            SessionConfigOption::select("model", "Model", current_model_value, groups)
                .category(SessionConfigOptionCategory::Model),
        );
    }

    let thinking = state
        .get("thinkingLevel")
        .and_then(Value::as_str)
        .unwrap_or("off")
        .to_owned();
    let reasoning = state
        .pointer("/model/reasoning")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let levels = if reasoning {
        vec!["off", "minimal", "low", "medium", "high", "xhigh", "max"]
    } else {
        vec!["off"]
    };
    let levels = levels
        .into_iter()
        .map(|level| SessionConfigSelectOption::new(level, title_case(level)))
        .collect::<Vec<_>>();
    result.push(
        SessionConfigOption::select("thought_level", "Thinking", thinking, levels)
            .category(SessionConfigOptionCategory::ThoughtLevel),
    );
    result
}

pub fn mode_state(state: &Value) -> SessionModeState {
    let current = state
        .get("thinkingLevel")
        .and_then(Value::as_str)
        .unwrap_or("off")
        .to_owned();
    let reasoning = state
        .pointer("/model/reasoning")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let levels = if reasoning {
        vec!["off", "minimal", "low", "medium", "high", "xhigh", "max"]
    } else {
        vec!["off"]
    };
    SessionModeState::new(
        current,
        levels
            .into_iter()
            .map(|level| SessionMode::new(level, format!("Thinking: {}", title_case(level))))
            .collect(),
    )
}

fn title_case(value: &str) -> String {
    let mut chars = value.chars();
    chars
        .next()
        .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
        .unwrap_or_default()
}

pub fn message_history_updates(messages: &Value) -> anyhow::Result<Vec<SessionUpdate>> {
    let mut updates = Vec::new();
    for message in messages
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let content = message.get("content");
        let text = match content {
            Some(Value::String(text)) => text.clone(),
            Some(Value::Array(parts)) => parts
                .iter()
                .filter_map(|part| {
                    (part.get("type").and_then(Value::as_str) == Some("text"))
                        .then(|| part.get("text").and_then(Value::as_str))
                        .flatten()
                })
                .collect::<Vec<_>>()
                .join(""),
            _ => String::new(),
        };
        if !text.is_empty() {
            let kind = if role == "user" {
                "user_message_chunk"
            } else {
                "agent_message_chunk"
            };
            updates.push(text_update(kind, &text)?);
        }
    }
    Ok(updates)
}
