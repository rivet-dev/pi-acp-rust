#!/usr/bin/env python3
"""Deterministic Pi RPC fixture shared by tests and benchmarks."""

import json
import os
import pathlib
import sys
import time
import uuid


def arg_value(flag, default=None):
    try:
        return sys.argv[sys.argv.index(flag) + 1]
    except (ValueError, IndexError):
        return default


session_dir = pathlib.Path(arg_value("--session-dir", os.environ.get("TMPDIR", "/tmp")))
session_dir.mkdir(parents=True, exist_ok=True)
loaded = arg_value("--session")
session_id = pathlib.Path(loaded).stem if loaded else os.environ.get("FAKE_PI_SESSION_ID", "fake-session")
session_file = pathlib.Path(loaded) if loaded else session_dir / f"{session_id}.jsonl"
session_file.touch(exist_ok=True)
session_name = None
model = {"provider": "anthropic", "id": "claude-sonnet", "name": "Claude Sonnet", "reasoning": True}
thinking = "medium"
waiting = False


def emit(value):
    sys.stdout.write(json.dumps(value, separators=(",", ":"), ensure_ascii=False) + "\n")
    sys.stdout.flush()


def response(request, data=None, success=True, error=None):
    value = {
        "id": request.get("id"),
        "type": "response",
        "command": request.get("type"),
        "success": success,
    }
    if data is not None:
        value["data"] = data
    if error is not None:
        value["error"] = error
    emit(value)


for raw in sys.stdin:
    request = json.loads(raw.rstrip("\r\n"))
    command = request.get("type")
    if command == "get_state":
        response(
            request,
            {
                "model": model,
                "thinkingLevel": thinking,
                "isStreaming": waiting,
                "isCompacting": False,
                "sessionFile": str(session_file),
                "sessionId": session_id,
                "sessionName": session_name,
                "messageCount": 2,
                "pendingMessageCount": 0,
            },
        )
    elif command == "get_available_models":
        response(
            request,
            {
                "models": [
                    {"provider": "anthropic", "id": "claude-sonnet", "name": "Claude Sonnet", "reasoning": True},
                    {"provider": "anthropic", "id": "claude-haiku", "name": "Claude Haiku", "reasoning": False},
                    {"provider": "openai", "id": "gpt-5", "name": "GPT-5", "reasoning": True},
                ]
            },
        )
    elif command == "get_commands":
        response(request, {"commands": [{"name": "review", "description": "Review current changes", "source": "prompt"}]})
    elif command == "get_messages":
        response(request, {"messages": [{"role": "user", "content": "prior question"}, {"role": "assistant", "content": [{"type": "text", "text": "prior answer"}]}]})
    elif command == "prompt":
        response(request)
        waiting = request.get("message") in ("wait", "ui")
        emit({"type": "agent_start"})
        if request.get("message") == "ui":
            emit({"type": "extension_ui_request", "id": "ui-1", "method": "select", "title": "Choose", "options": ["first", "second"]})
        if not waiting:
            emit({"type": "message_update", "assistantMessageEvent": {"type": "thinking_delta", "delta": "checking "}})
            if request.get("message") == "edit":
                edit_path = pathlib.Path.cwd() / "edit.txt"
                edit_path.write_text("before\n")
                emit({"type": "tool_execution_start", "toolCallId": "tool-1", "toolName": "edit", "args": {"path": "edit.txt"}})
                time.sleep(0.05)
                edit_path.write_text("after\n")
                emit({"type": "tool_execution_end", "toolCallId": "tool-1", "toolName": "edit", "result": {"content": [{"type": "text", "text": "edited"}]}, "isError": False})
            else:
                emit({"type": "tool_execution_start", "toolCallId": "tool-1", "toolName": "bash", "args": {"command": "pwd"}})
                emit({"type": "tool_execution_end", "toolCallId": "tool-1", "toolName": "bash", "result": {"content": [{"type": "text", "text": "/workspace"}]}, "isError": False})
            emit({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "delta": f"reply:{request.get('message', '')}"}})
            emit({"type": "agent_end", "messages": [], "willRetry": False})
            emit({"type": "agent_settled"})
    elif command == "abort":
        response(request)
        waiting = False
        emit({"type": "agent_settled"})
    elif command == "extension_ui_response":
        waiting = False
        emit({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "delta": f"selected:{request.get('value', 'cancelled')}"}})
        emit({"type": "agent_end", "messages": [], "willRetry": False})
        emit({"type": "agent_settled"})
    elif command == "set_model":
        model = {"provider": request["provider"], "id": request["modelId"], "name": request["modelId"], "reasoning": True}
        response(request, model)
    elif command == "set_thinking_level":
        thinking = request["level"]
        response(request)
    elif command == "set_session_name":
        session_name = request.get("name")
        response(request)
    elif command == "get_session_stats":
        response(request, {"sessionId": session_id, "tokens": {"input": 10, "output": 4, "total": 14}, "cost": 0.001})
    elif command == "compact":
        response(request, {"summary": "compacted", "tokensBefore": 100})
    elif command == "export_html":
        response(request, {"path": request.get("outputPath") or str(session_dir / "export.html")})
    elif command in ("set_steering_mode", "set_follow_up_mode"):
        response(request)
    elif command == "clone":
        session_id = "fork-" + uuid.uuid4().hex[:8]
        session_file = session_dir / f"{session_id}.jsonl"
        session_file.touch()
        response(request, {"cancelled": False})
    else:
        response(request, success=False, error=f"unsupported fake command: {command}")
