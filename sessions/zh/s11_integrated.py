"""
Section 11: Integrated Gateway
"集大成 -- 十层在一个可运行文件里接成一条产线"

把 claw0 的全部 10 层按生产形态接线进一个自包含可运行文件:

    飞书(WS 长连接)──┐                              ┌── FeishuChannel.send (经投递队列)
    Telegram(长轮询,可选)──┤   InboundMessage       │
    CLI(stdin 非阻塞)──┘      │                       │
            msg_queue + q_lock                         │
                   │                                   │
           ┌───────▼───────┐    resolve_route          │
           │  drain 入站   │───► BindingTable ──► agent_id
           └───────┬───────┘    build_session_key ──► session_key
                   │                                   │
            入队 LANE_MAIN                              │
                   ▼                                   │
   ┌───────────────────────────────────┐               │
   │ LaneQueue(main) / (cron) / (hb)   │  CommandQueue  │
   └───────────┬───────────────────────┘               │
               ▼                                       │
   run_agent_turn:                                     │
     build_system_prompt(+auto memory recall)           │
     → ResilientAgent.run(三层洋葱:轮换/溢出压缩/工具循环)
       → 文本回复                                      │
               ▼                                       │
   DeliveryQueue.enqueue(channel, to, text) ───────────┘
     → DeliveryRunner: chunk_message → channel.send → ack/fail(退避)

各层来源(复现,非 import):
  s01/s02 工具循环  s03 会话+ContextGuard  s04 飞书/TG/CLI 通道
  s05 网关五级路由  s06 灵魂+记忆(含硬遗忘)  s07 心跳+croniter
  s08 可靠投递(WAL+退避)  s09 韧性三层洋葱  s10 命名车道并发

用法:
    cd claw0
    .venv/bin/python sessions/zh/s11_integrated.py

依赖: ANTHROPIC_API_KEY, MODEL_ID (.env); 可选 FEISHU_*, TELEGRAM_BOT_TOKEN, ANTHROPIC_API_KEY_2
工作区: workspace/(SOUL/IDENTITY/TOOLS/USER/AGENTS/MEMORY/HEARTBEAT/BOOTSTRAP, CRON.json)
状态: state/(delivery 队列, telegram offset, sessions)

REPL 命令:
    /channels /accounts /bindings /agents /sessions
    /soul /prompt /memory /search <q> /forget date=...|category=...
    /heartbeat /trigger /cron
    /lanes /queue /enqueue <lane> <msg> /concurrency <lane> <N> /generation /reset
    /delivery /profiles /help   quit/exit
"""

# ---------------------------------------------------------------------------
# 导入与配置
# ---------------------------------------------------------------------------
import json
import math
import os
import re
import sys
import time
import uuid
import random
import select
import shutil
import subprocess
import threading
import concurrent.futures
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable

from dotenv import load_dotenv
from anthropic import Anthropic

# 可选依赖: 飞书长连接 / cron / telegram
try:
    import lark_oapi as lark
    HAS_LARK = True
except Exception:
    lark = None
    HAS_LARK = False

try:
    from croniter import croniter
    HAS_CRON = True
except Exception:
    croniter = None
    HAS_CRON = False

try:
    import httpx
    HAS_HTTPX = True
except Exception:
    httpx = None
    HAS_HTTPX = False


load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env", override=True)

MODEL_ID = os.getenv("MODEL_ID", "claude-sonnet-4-20250514")
ANTHROPIC_BASE_URL = os.getenv("ANTHROPIC_BASE_URL") or None

# 主 client: 用于心跳/cron 单轮 + 摘要压缩 (韧性 agent 用 per-profile client)
client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"), base_url=ANTHROPIC_BASE_URL)

WORKSPACE_DIR = Path(__file__).resolve().parent.parent.parent / "workspace"
STATE_DIR = Path(__file__).resolve().parent.parent.parent / "state"

BOOTSTRAP_FILES = [
    "SOUL.md", "IDENTITY.md", "TOOLS.md", "USER.md",
    "HEARTBEAT.md", "BOOTSTRAP.md", "AGENTS.md", "MEMORY.md",
]
MAX_FILE_CHARS = 20000
MAX_TOTAL_CHARS = 150000
MAX_TOOL_OUTPUT = 50000

# 投递退避 (毫秒)
BACKOFF_MS = [500, 1000, 2000, 5000, 10000, 30000]
MAX_RETRIES = 6

# 通道分片上限
CHANNEL_LIMITS: dict[str, int] = {
    "telegram": 4096, "feishu": 4096, "cli": 100000, "default": 4096,
}

# ---------------------------------------------------------------------------
# ANSI 颜色
# ---------------------------------------------------------------------------
CYAN = "\033[36m"; GREEN = "\033[32m"; YELLOW = "\033[33m"; DIM = "\033[2m"
RESET = "\033[0m"; BOLD = "\033[1m"; MAGENTA = "\033[35m"; RED = "\033[31m"
BLUE = "\033[34m"; ORANGE = "\033[38;5;208m"


def colored_prompt() -> str:
    # \001 / \002 标记其间为非打印字符, 让 readline/libedit 不计入提示符宽度.
    return f"\001{CYAN}\002\001{BOLD}\002You > \001{RESET}\002"


def print_assistant(text: str) -> None:
    print(f"\n{GREEN}{BOLD}Assistant:{RESET} {text}\n")

def print_tool(name: str, detail: str) -> None:
    print(f"  {DIM}[tool: {name}] {detail}{RESET}")

def print_info(text: str) -> None:
    print(f"{DIM}{text}{RESET}")

def print_warn(text: str) -> None:
    print(f"{YELLOW}{text}{RESET}")

def print_error(text: str) -> None:
    print(f"{RED}{text}{RESET}")

def print_section(title: str) -> None:
    print(f"\n{CYAN}{BOLD}--- {title} ---{RESET}")

def print_channel(text: str) -> None:
    print(f"{ORANGE}{text}{RESET}")

def print_lane(lane_name: str, text: str) -> None:
    color = {"main": CYAN, "cron": MAGENTA, "heartbeat": BLUE}.get(lane_name, YELLOW)
    print(f"{color}{BOLD}[{lane_name}]{RESET} {text}")

def print_heartbeat(text: str) -> None:
    print(f"{BLUE}{BOLD}[heartbeat]{RESET} {text}")

def print_cron(text: str) -> None:
    print(f"{MAGENTA}{BOLD}[cron]{RESET} {text}")

def print_delivery(text: str) -> None:
    print(f"{ORANGE}{BOLD}[delivery]{RESET} {text}")

def print_resilience(text: str) -> None:
    print(f"{CYAN}[resilience] {text}{RESET}")

def print_session(text: str) -> None:
    print(f"{DIM}{text}{RESET}")


# ---------------------------------------------------------------------------
# 通道层 (s04)
# ---------------------------------------------------------------------------

@dataclass
class InboundMessage:
    text: str
    sender_id: str
    channel: str = ""          # "cli", "telegram", "feishu"
    account_id: str = ""
    peer_id: str = ""          # DM=user_id, 群=chat_id
    is_group: bool = False
    media: list = field(default_factory=list)
    raw: dict = field(default_factory=dict)


@dataclass
class ChannelAccount:
    channel: str
    account_id: str
    token: str = ""
    config: dict = field(default_factory=dict)


class Channel:
    name: str = "unknown"
    account_id: str = ""

    def receive(self) -> InboundMessage | None: ...
    def send(self, to: str, text: str, **kwargs: Any) -> bool: ...
    def close(self) -> None: ...


class CLIChannel(Channel):
    name = "cli"

    def __init__(self) -> None:
        self.account_id = "cli-local"

    def receive(self) -> InboundMessage | None:
        try:
            text = input(colored_prompt()).strip()
        except (KeyboardInterrupt, EOFError):
            return None
        if not text:
            return None
        return InboundMessage(
            text=text, sender_id="cli-user", channel="cli",
            account_id="cli-local", peer_id="cli-user",
        )

    def send(self, to: str, text: str, **kwargs: Any) -> bool:
        print_assistant(text)
        return True

    def close(self) -> None:
        pass


def _save_offset(path: Path, offset: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(offset))

def _load_offset(path: Path) -> int:
    try:
        return int(path.read_text().strip())
    except Exception:
        return 0


class TelegramChannel(Channel):
    name = "telegram"
    MAX_MSG_LEN = 4096

    def __init__(self, account: ChannelAccount) -> None:
        if not HAS_HTTPX:
            raise RuntimeError("TelegramChannel requires httpx")
        self.account_id = account.account_id
        self.base_url = f"https://api.telegram.org/bot{account.token}"
        self._http = httpx.Client(timeout=35.0)
        raw = account.config.get("allowed_chats", "")
        self.allowed_chats = {c.strip() for c in raw.split(",") if c.strip()} if raw else set()
        self._offset_path = STATE_DIR / "telegram" / f"offset-{self.account_id}.txt"
        self._offset = _load_offset(self._offset_path)
        self._seen: set[int] = set()
        self._text_buf: dict[tuple[str, str], dict] = {}

    def _api(self, method: str, **params: Any) -> dict:
        filtered = {k: v for k, v in params.items() if v is not None}
        try:
            resp = self._http.post(f"{self.base_url}/{method}", json=filtered)
            data = resp.json()
            if not data.get("ok"):
                print(f"  {RED}[telegram] {method}: {data.get('description', '?')}{RESET}")
                return {}
            return data.get("result", {})
        except Exception as exc:
            print(f"  {RED}[telegram] {method}: {exc}{RESET}")
            return {}

    def send_typing(self, chat_id: str) -> None:
        self._api("sendChatAction", chat_id=chat_id, action="typing")

    def poll(self) -> list[InboundMessage]:
        result = self._api("getUpdates", offset=self._offset, timeout=30,
                           allowed_updates=["message"])
        if not result or not isinstance(result, list):
            return self._flush_text()
        for update in result:
            uid = update.get("update_id", 0)
            if uid >= self._offset:
                self._offset = uid + 1
                _save_offset(self._offset_path, self._offset)
            if uid in self._seen:
                continue
            self._seen.add(uid)
            if len(self._seen) > 5000:
                self._seen.clear()
            msg = update.get("message")
            if not msg:
                continue
            inbound = self._parse(msg, update)
            if not inbound:
                continue
            if self.allowed_chats and inbound.peer_id not in self.allowed_chats:
                continue
            self._buf_text(inbound)
        return self._flush_text()

    def _buf_text(self, inbound: InboundMessage) -> None:
        key = (inbound.peer_id, inbound.sender_id)
        now = time.monotonic()
        if key in self._text_buf:
            self._text_buf[key]["text"] += "\n" + inbound.text
            self._text_buf[key]["ts"] = now
        else:
            self._text_buf[key] = {"text": inbound.text, "msg": inbound, "ts": now}

    def _flush_text(self) -> list[InboundMessage]:
        now = time.monotonic()
        ready: list[InboundMessage] = []
        for key in [k for k, b in self._text_buf.items() if (now - b["ts"]) >= 1.0]:
            buf = self._text_buf.pop(key)
            buf["msg"].text = buf["text"]
            ready.append(buf["msg"])
        return ready

    def _parse(self, msg: dict, raw_update: dict) -> InboundMessage | None:
        chat = msg.get("chat", {})
        chat_type = chat.get("type", "")
        chat_id = str(chat.get("id", ""))
        user_id = str(msg.get("from", {}).get("id", ""))
        text = msg.get("text", "") or msg.get("caption", "")
        if not text:
            return None
        thread_id = msg.get("message_thread_id")
        is_forum = chat.get("is_forum", False)
        is_group = chat_type in ("group", "supergroup")
        if chat_type == "private":
            peer_id = user_id
        elif is_group and is_forum and thread_id is not None:
            peer_id = f"{chat_id}:topic:{thread_id}"
        else:
            peer_id = chat_id
        return InboundMessage(
            text=text, sender_id=user_id, channel="telegram",
            account_id=self.account_id, peer_id=peer_id,
            is_group=is_group, raw=raw_update,
        )

    def receive(self) -> InboundMessage | None:
        msgs = self.poll()
        return msgs[0] if msgs else None

    def send(self, to: str, text: str, **kwargs: Any) -> bool:
        chat_id, thread_id = to, None
        if ":topic:" in to:
            parts = to.split(":topic:")
            chat_id, thread_id = parts[0], int(parts[1]) if len(parts) > 1 else None
        ok = True
        for chunk in self._chunk(text):
            if not self._api("sendMessage", chat_id=chat_id, text=chunk,
                             message_thread_id=thread_id):
                ok = False
        return ok

    def _chunk(self, text: str) -> list[str]:
        if len(text) <= self.MAX_MSG_LEN:
            return [text]
        chunks = []
        while text:
            if len(text) <= self.MAX_MSG_LEN:
                chunks.append(text); break
            cut = text.rfind("\n", 0, self.MAX_MSG_LEN)
            if cut <= 0:
                cut = self.MAX_MSG_LEN
            chunks.append(text[:cut])
            text = text[cut:].lstrip("\n")
        return chunks

    def close(self) -> None:
        self._http.close()


class FeishuChannel(Channel):
    name = "feishu"

    def __init__(self, account: ChannelAccount) -> None:
        if not HAS_HTTPX:
            raise RuntimeError("FeishuChannel requires httpx")
        self.account_id = account.account_id
        self.app_id = account.config.get("app_id", "")
        self.app_secret = account.config.get("app_secret", "")
        self._bot_open_id = account.config.get("bot_open_id", "")
        self._is_lark = account.config.get("is_lark", False)
        self.api_base = ("https://open.larksuite.com/open-apis" if self._is_lark
                         else "https://open.feishu.cn/open-apis")
        self._tenant_token: str = ""
        self._token_expires_at: float = 0.0
        self._http = httpx.Client(timeout=15.0)

    def _refresh_token(self) -> str:
        if self._tenant_token and time.time() < self._token_expires_at:
            return self._tenant_token
        try:
            resp = self._http.post(
                f"{self.api_base}/auth/v3/tenant_access_token/internal",
                json={"app_id": self.app_id, "app_secret": self.app_secret},
            )
            data = resp.json()
            if data.get("code") != 0:
                print(f"  {RED}[feishu] Token error: {data.get('msg', '?')}{RESET}")
                return ""
            self._tenant_token = data.get("tenant_access_token", "")
            self._token_expires_at = time.time() + data.get("expire", 7200) - 300
            return self._tenant_token
        except Exception as exc:
            print(f"  {RED}[feishu] Token error: {exc}{RESET}")
            return ""

    def _parse_content(self, message: dict) -> tuple[str, list]:
        msg_type = message.get("msg_type", "text")
        raw = message.get("content", "{}")
        try:
            content = json.loads(raw) if isinstance(raw, str) else raw
        except json.JSONDecodeError:
            return "", []
        media: list[dict] = []
        if msg_type == "text":
            return content.get("text", ""), media
        if msg_type == "post":
            texts: list[str] = []
            for lc in content.values():
                if not isinstance(lc, dict):
                    continue
                title = lc.get("title", "")
                if title:
                    texts.append(title)
                for para in lc.get("content", []):
                    for node in para:
                        tag = node.get("tag")
                        if tag == "text":
                            texts.append(node.get("text", ""))
                        elif tag == "a":
                            texts.append(node.get("text", "") + " " + node.get("href", ""))
            return "\n".join(texts), media
        if msg_type == "image":
            key = content.get("image_key", "")
            if key:
                media.append({"type": "image", "key": key})
            return "[image]", media
        return "", media

    def _ws_bot_mentioned(self, message: Any) -> bool:
        for m in (getattr(message, "mentions", None) or []):
            mid = getattr(m, "id", None)
            if mid is not None and getattr(mid, "open_id", None) == self._bot_open_id:
                return True
            if getattr(m, "key", None) == self._bot_open_id:
                return True
        return False

    def parse_ws_event(self, data: Any) -> InboundMessage | None:
        """解析长连接 P2ImMessageReceiveV1 事件 -> InboundMessage."""
        try:
            event = getattr(data, "event", None)
            if event is None:
                return None
            message = event.message
            sender = event.sender
            if message is None or sender is None:
                return None
            user_id = ""
            sid = sender.sender_id
            if sid is not None:
                user_id = sid.open_id or sid.user_id or sid.union_id or ""
            chat_id = message.chat_id or ""
            chat_type = message.chat_type or ""
            is_group = chat_type == "group"
            if is_group and self._bot_open_id and not self._ws_bot_mentioned(message):
                return None
            text, media = self._parse_content(
                {"msg_type": message.message_type, "content": message.content or "{}"}
            )
            if not text:
                return None
            raw: dict = {}
            if lark is not None:
                try:
                    raw = lark.JSON.marshal(data)
                except Exception:
                    raw = {}
            return InboundMessage(
                text=text, sender_id=user_id, channel="feishu",
                account_id=self.account_id,
                peer_id=user_id if chat_type == "p2p" else chat_id,
                media=media, is_group=is_group, raw=raw,
            )
        except Exception as exc:
            print(f"  {RED}[feishu] ws parse error: {exc}{RESET}")
            return None

    def start_long_connection(self, msg_queue: list, q_lock: threading.Lock) -> threading.Thread | None:
        """守护线程启动 lark.ws.Client; 入站事件 -> msg_queue (与 s04 一致)."""
        if not HAS_LARK:
            print(f"  {RED}[feishu] 长连接需要 lark-oapi: pip install lark-oapi{RESET}")
            return None
        if not (self.app_id and self.app_secret):
            print(f"  {RED}[feishu] 长连接需要 FEISHU_APP_ID + FEISHU_APP_SECRET{RESET}")
            return None

        def _on_msg(data: Any) -> None:
            inbound = self.parse_ws_event(data)
            if inbound is not None:
                with q_lock:
                    msg_queue.append(inbound)
                print_channel(f"  [feishu/ws] {inbound.sender_id}: {inbound.text[:80]}")

        dispatcher = (lark.EventDispatcherHandler.builder("", "")
                      .register_p2_im_message_receive_v1(_on_msg).build())
        ws_client = lark.ws.Client(
            self.app_id, self.app_secret,
            event_handler=dispatcher, log_level=lark.LogLevel.INFO,
            domain=("https://open.larksuite.com" if self._is_lark
                    else "https://open.feishu.cn"),
        )

        def _run() -> None:
            print_channel(f"  [feishu/ws] 已为 {self.account_id} 启动长连接")
            try:
                ws_client.start()
            except Exception as exc:
                print(f"  {RED}[feishu/ws] 连接错误: {exc}{RESET}")

        t = threading.Thread(target=_run, daemon=True, name="feishu-ws")
        t.start()
        return t

    def receive(self) -> InboundMessage | None:
        return None

    def send(self, to: str, text: str, **kwargs: Any) -> bool:
        token = self._refresh_token()
        if not token:
            return False
        # receive_id_type 必须与 to 的实际类型匹配: ou_=open_id(私聊), oc_/其他=chat_id(群)
        rid_type = "open_id" if to.startswith("ou_") else "chat_id"
        try:
            resp = self._http.post(
                f"{self.api_base}/im/v1/messages",
                params={"receive_id_type": rid_type},
                headers={"Authorization": f"Bearer {token}"},
                json={"receive_id": to, "msg_type": "text",
                      "content": json.dumps({"text": text})},
            )
            data = resp.json()
            if data.get("code") != 0:
                print(f"  {RED}[feishu] Send: {data.get('msg', '?')}{RESET}")
                return False
            return True
        except Exception as exc:
            print(f"  {RED}[feishu] Send: {exc}{RESET}")
            return False

    def close(self) -> None:
        self._http.close()


class ChannelManager:
    def __init__(self) -> None:
        self.channels: dict[str, Channel] = {}
        self.accounts: list[ChannelAccount] = []

    def register(self, channel: Channel, account: ChannelAccount | None = None) -> None:
        self.channels[channel.name] = channel
        if account:
            self.accounts.append(account)

    def get(self, name: str) -> Channel | None:
        return self.channels.get(name)

    def list_channels(self) -> list[str]:
        return list(self.channels.keys())

    def close_all(self) -> None:
        for ch in self.channels.values():
            try:
                ch.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 工具层 (s01/s02/s03)
# ---------------------------------------------------------------------------

def safe_path(raw: str) -> Path:
    target = (WORKSPACE_DIR / raw).resolve()
    if not str(target).startswith(str(WORKSPACE_DIR.resolve())):
        raise ValueError(f"Path traversal blocked: {raw}")
    return target

def truncate(text: str, limit: int = MAX_TOOL_OUTPUT) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n... [truncated, {len(text)} total chars]"


def tool_bash(command: str, timeout: int = 30) -> str:
    dangerous = ["rm -rf /", "mkfs", "> /dev/sd", "dd if="]
    for pattern in dangerous:
        if pattern in command:
            return f"Error: Refused to run dangerous command containing '{pattern}'"
    print_tool("bash", command)
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True,
                                timeout=timeout, cwd=str(WORKSPACE_DIR))
        output = ""
        if result.stdout:
            output += result.stdout
        if result.stderr:
            output += ("\n--- stderr ---\n" + result.stderr) if output else result.stderr
        if result.returncode != 0:
            output += f"\n[exit code: {result.returncode}]"
        return truncate(output) if output else "[no output]"
    except subprocess.TimeoutExpired:
        return f"Error: Command timed out after {timeout}s"
    except Exception as exc:
        return f"Error: {exc}"

def tool_read_file(file_path: str) -> str:
    print_tool("read_file", file_path)
    try:
        target = safe_path(file_path)
        if not target.exists():
            return f"Error: File not found: {file_path}"
        if not target.is_file():
            return f"Error: Not a file: {file_path}"
        return truncate(target.read_text(encoding="utf-8"))
    except ValueError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error: {exc}"

def tool_write_file(file_path: str, content: str) -> str:
    print_tool("write_file", file_path)
    try:
        target = safe_path(file_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return f"Successfully wrote {len(content)} chars to {file_path}"
    except ValueError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error: {exc}"

def tool_edit_file(file_path: str, old_string: str, new_string: str) -> str:
    print_tool("edit_file", f"{file_path} (replace {len(old_string)} chars)")
    try:
        target = safe_path(file_path)
        if not target.exists():
            return f"Error: File not found: {file_path}"
        content = target.read_text(encoding="utf-8")
        count = content.count(old_string)
        if count == 0:
            return "Error: old_string not found in file. Make sure it matches exactly."
        if count > 1:
            return (f"Error: old_string found {count} times. "
                    "It must be unique. Provide more surrounding context.")
        target.write_text(content.replace(old_string, new_string, 1), encoding="utf-8")
        return f"Successfully edited {file_path}"
    except ValueError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error: {exc}"

def tool_list_directory(directory: str = ".") -> str:
    print_tool("list_directory", directory)
    try:
        target = safe_path(directory)
        if not target.is_dir():
            return f"Error: Not a directory: {directory}"
        entries = []
        for p in sorted(target.iterdir()):
            entries.append(f"{'[d]' if p.is_dir() else '[f]'} {p.name}")
        return "\n".join(entries) if entries else "[empty]"
    except ValueError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error: {exc}"

def tool_get_current_time() -> str:
    print_tool("get_current_time", "")
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# 记忆工具 handler (memory_store 在下面定义; 用闭包绑定在 agent_loop 里)
# 这里先定义 TOOLS schema 和分发表, handler 在 agent_loop 内组装 (memory_store 需先构造).

TOOLS = [
    {"name": "bash", "description": "Run a shell command and return its output.",
     "input_schema": {"type": "object", "properties": {
         "command": {"type": "string", "description": "The shell command to execute."},
         "timeout": {"type": "integer", "description": "Timeout in seconds. Default 30."}},
         "required": ["command"]}},
    {"name": "read_file", "description": "Read the contents of a file.",
     "input_schema": {"type": "object", "properties": {
         "file_path": {"type": "string", "description": "Path relative to working directory."}},
         "required": ["file_path"]}},
    {"name": "write_file", "description": "Write content to a file. Overwrites existing.",
     "input_schema": {"type": "object", "properties": {
         "file_path": {"type": "string"}, "content": {"type": "string"}},
         "required": ["file_path", "content"]}},
    {"name": "edit_file", "description": "Replace an exact unique string in a file.",
     "input_schema": {"type": "object", "properties": {
         "file_path": {"type": "string"}, "old_string": {"type": "string"},
         "new_string": {"type": "string"}},
         "required": ["file_path", "old_string", "new_string"]}},
    {"name": "list_directory", "description": "List directory entries.",
     "input_schema": {"type": "object", "properties": {
         "directory": {"type": "string", "description": "Default '.'"}}}},
    {"name": "get_current_time", "description": "Get current UTC date/time.",
     "input_schema": {"type": "object", "properties": {}}},
    {"name": "memory_write",
     "description": "Save an important fact or observation to long-term memory. "
                    "Pass ttl_hours for a temporary memory (e.g. a one-time reminder) "
                    "that auto-expires and is lazily skipped after that many hours.",
     "input_schema": {"type": "object", "properties": {
         "content": {"type": "string"},
         "category": {"type": "string"},
         "ttl_hours": {"type": "number", "description": "Optional TTL in hours."}},
         "required": ["content"]}},
    {"name": "memory_search", "description": "Search stored memories for relevant info.",
     "input_schema": {"type": "object", "properties": {
         "query": {"type": "string"}, "top_k": {"type": "integer"}},
         "required": ["query"]}},
    {"name": "memory_forget",
     "description": "Explicitly forget (remove) memories from the daily log. "
                    "Pass date (YYYY-MM-DD) to drop a whole day's file, or category to "
                    "remove matching entries across all days. Never touches MEMORY.md.",
     "input_schema": {"type": "object", "properties": {
         "category": {"type": "string"}, "date": {"type": "string"}}, "required": []}},
]

_BASE_TOOL_HANDLERS: dict[str, Any] = {
    "bash": tool_bash, "read_file": tool_read_file, "write_file": tool_write_file,
    "edit_file": tool_edit_file, "list_directory": tool_list_directory,
    "get_current_time": tool_get_current_time,
}


def process_tool_call(tool_name: str, tool_input: dict, handlers: dict[str, Any]) -> str:
    handler = handlers.get(tool_name)
    if handler is None:
        return f"Error: Unknown tool '{tool_name}'"
    try:
        return handler(**tool_input)
    except TypeError as exc:
        return f"Error: Invalid arguments for {tool_name}: {exc}"
    except Exception as exc:
        return f"Error: {tool_name} failed: {exc}"


# ---------------------------------------------------------------------------
# 会话持久化 (s03)
# ---------------------------------------------------------------------------

class SessionStore:
    def __init__(self, agent_id: str = "default") -> None:
        self.agent_id = agent_id
        self.base_dir = WORKSPACE_DIR / ".sessions" / "agents" / agent_id / "sessions"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._index_path = self.base_dir.parent / "sessions.json"
        self._index: dict[str, dict] = self._load_index()
        self.current_session_id: str | None = None

    def _load_index(self) -> dict[str, dict]:
        if self._index_path.exists():
            try:
                return json.loads(self._index_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                return {}
        return {}

    def _save_index(self) -> None:
        self._index_path.write_text(
            json.dumps(self._index, indent=2, ensure_ascii=False), encoding="utf-8")

    def _session_path(self, session_id: str) -> Path:
        return self.base_dir / f"{session_id}.jsonl"

    def create_session(self, label: str = "") -> str:
        session_id = uuid.uuid4().hex[:12]
        now = datetime.now(timezone.utc).isoformat()
        self._index[session_id] = {"label": label, "created_at": now,
                                   "last_active": now, "message_count": 0}
        self._save_index()
        self._session_path(session_id).touch()
        self.current_session_id = session_id
        return session_id

    def load_session(self, session_id: str) -> list[dict]:
        path = self._session_path(session_id)
        if not path.exists():
            return []
        self.current_session_id = session_id
        return self._rebuild_history(path)

    def save_turn(self, role: str, content: Any) -> None:
        if not self.current_session_id:
            return
        self.append_transcript(self.current_session_id,
                               {"type": role, "content": content, "ts": time.time()})

    def append_transcript(self, session_id: str, record: dict) -> None:
        path = self._session_path(session_id)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        if session_id in self._index:
            self._index[session_id]["last_active"] = datetime.now(timezone.utc).isoformat()
            self._index[session_id]["message_count"] += 1
            self._save_index()

    def _rebuild_history(self, path: Path) -> list[dict]:
        messages: list[dict] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            rtype = record.get("type")
            if rtype == "user":
                messages.append({"role": "user", "content": record["content"]})
            elif rtype == "assistant":
                content = record["content"]
                if isinstance(content, str):
                    content = [{"type": "text", "text": content}]
                messages.append({"role": "assistant", "content": content})
            elif rtype == "tool_use":
                block = {"type": "tool_use", "id": record["tool_use_id"],
                         "name": record["name"], "input": record["input"]}
                if messages and messages[-1]["role"] == "assistant":
                    c = messages[-1]["content"]
                    if isinstance(c, list):
                        c.append(block)
                    else:
                        messages[-1]["content"] = [{"type": "text", "text": str(c)}, block]
                else:
                    messages.append({"role": "assistant", "content": [block]})
            elif rtype == "tool_result":
                rb = {"type": "tool_result", "tool_use_id": record["tool_use_id"],
                      "content": record["content"]}
                if (messages and messages[-1]["role"] == "user"
                        and isinstance(messages[-1]["content"], list)
                        and messages[-1]["content"]
                        and isinstance(messages[-1]["content"][0], dict)
                        and messages[-1]["content"][0].get("type") == "tool_result"):
                    messages[-1]["content"].append(rb)
                else:
                    messages.append({"role": "user", "content": [rb]})
        return messages

    def list_sessions(self) -> list[tuple[str, dict]]:
        items = list(self._index.items())
        items.sort(key=lambda x: x[1].get("last_active", ""), reverse=True)
        return items


# ---------------------------------------------------------------------------
# 上下文溢出保护 (s03/s09)
# ---------------------------------------------------------------------------

class ContextGuard:
    def __init__(self, max_tokens: int = 100000) -> None:
        self.max_tokens = max_tokens

    @staticmethod
    def estimate_tokens(text: str) -> int:
        return len(text) // 4

    def estimate_messages_tokens(self, messages: list[dict]) -> int:
        total = 0
        for msg in messages:
            content = msg.get("content", "")
            if isinstance(content, str):
                total += self.estimate_tokens(content)
            elif isinstance(content, list):
                for block in content:
                    if isinstance(block, dict):
                        if "text" in block:
                            total += self.estimate_tokens(block["text"])
                        elif block.get("type") == "tool_result":
                            rc = block.get("content", "")
                            if isinstance(rc, str):
                                total += self.estimate_tokens(rc)
                        elif block.get("type") == "tool_use":
                            total += self.estimate_tokens(json.dumps(block.get("input", {})))
        return total

    def truncate_tool_results(self, messages: list[dict]) -> list[dict]:
        max_chars = int(self.max_tokens * 4 * 0.3)
        result = []
        for msg in messages:
            content = msg.get("content", "")
            if isinstance(content, list):
                new_blocks = []
                for block in content:
                    if (isinstance(block, dict) and block.get("type") == "tool_result"
                            and isinstance(block.get("content"), str)
                            and len(block["content"]) > max_chars):
                        block = dict(block)
                        original = len(block["content"])
                        block["content"] = (
                            block["content"][:max_chars]
                            + f"\n\n[... truncated ({original} chars total, "
                            f"showing first {max_chars}) ...]")
                    new_blocks.append(block)
                result.append({"role": msg["role"], "content": new_blocks})
            else:
                result.append(msg)
        return result

    def compact_history(self, messages: list[dict], api_client: Anthropic, model: str) -> list[dict]:
        total = len(messages)
        if total <= 4:
            return messages
        keep_count = max(4, int(total * 0.2))
        compress_count = max(2, int(total * 0.5))
        compress_count = min(compress_count, total - keep_count)
        if compress_count < 2:
            return messages
        old_messages = messages[:compress_count]
        recent_messages = messages[compress_count:]
        parts: list[str] = []
        for msg in old_messages:
            role = msg["role"]
            content = msg.get("content", "")
            if isinstance(content, str):
                parts.append(f"[{role}]: {content}")
            elif isinstance(content, list):
                for block in content:
                    if isinstance(block, dict):
                        if block.get("type") == "text":
                            parts.append(f"[{role}]: {block['text']}")
                        elif block.get("type") == "tool_use":
                            parts.append(f"[{role} called {block.get('name', '?')}]: "
                                         f"{json.dumps(block.get('input', {}), ensure_ascii=False)}")
                        elif block.get("type") == "tool_result":
                            rc = block.get("content", "")
                            prev = rc[:500] if isinstance(rc, str) else str(rc)[:500]
                            parts.append(f"[tool_result]: {prev}")
        old_text = "\n".join(parts)
        summary_prompt = ("Summarize the following conversation concisely, "
                           "preserving key facts and decisions. Output only the summary.\n\n"
                           f"{old_text}")
        try:
            resp = api_client.messages.create(
                model=model, max_tokens=2048,
                system="You are a conversation summarizer. Be concise and factual.",
                messages=[{"role": "user", "content": summary_prompt}],
            )
            summary = "".join(getattr(b, "text", "") for b in resp.content)
            print_resilience(f"Compacted {len(old_messages)} msgs -> summary ({len(summary)} chars)")
        except Exception as exc:
            print_warn(f"Summary failed ({exc}), dropping old messages")
            return recent_messages
        compacted = [
            {"role": "user", "content": "[Previous conversation summary]\n" + summary},
            {"role": "assistant", "content": [
                {"type": "text", "text": "Understood, I have the context from our previous conversation."}]},
        ]
        compacted.extend(recent_messages)
        return compacted


# ---------------------------------------------------------------------------
# Bootstrap + 灵魂 + 系统提示词 (s06)
# ---------------------------------------------------------------------------

class BootstrapLoader:
    def __init__(self, workspace_dir: Path) -> None:
        self.workspace_dir = workspace_dir

    def load_file(self, name: str) -> str:
        path = self.workspace_dir / name
        if not path.is_file():
            return ""
        try:
            return path.read_text(encoding="utf-8")
        except Exception:
            return ""

    def truncate_file(self, content: str, max_chars: int = MAX_FILE_CHARS) -> str:
        if len(content) <= max_chars:
            return content
        cut = content.rfind("\n", 0, max_chars)
        if cut <= 0:
            cut = max_chars
        return content[:cut] + f"\n\n[... truncated ({len(content)} total, first {cut}) ...]"

    def load_all(self, mode: str = "full") -> dict[str, str]:
        if mode == "none":
            return {}
        names = ["AGENTS.md", "TOOLS.md"] if mode == "minimal" else list(BOOTSTRAP_FILES)
        result: dict[str, str] = {}
        total = 0
        for name in names:
            raw = self.load_file(name)
            if not raw:
                continue
            truncated = self.truncate_file(raw)
            if total + len(truncated) > MAX_TOTAL_CHARS:
                remaining = MAX_TOTAL_CHARS - total
                if remaining > 0:
                    truncated = self.truncate_file(raw, remaining)
                else:
                    break
            result[name] = truncated
            total += len(truncated)
        return result


def build_system_prompt(mode: str = "full", bootstrap: dict[str, str] | None = None,
                        memory_context: str = "", agent_id: str = "main",
                        channel: str = "terminal") -> str:
    if bootstrap is None:
        bootstrap = {}
    sections: list[str] = []
    identity = bootstrap.get("IDENTITY.md", "").strip()
    sections.append(identity if identity else "You are a helpful personal AI assistant.")
    if mode == "full":
        soul = bootstrap.get("SOUL.md", "").strip()
        if soul:
            sections.append(f"## Personality\n\n{soul}")
    tools_md = bootstrap.get("TOOLS.md", "").strip()
    if tools_md:
        sections.append(f"## Tool Usage Guidelines\n\n{tools_md}")
    if mode == "full":
        mem_md = bootstrap.get("MEMORY.md", "").strip()
        parts: list[str] = []
        if mem_md:
            parts.append(f"### Evergreen Memory\n\n{mem_md}")
        if memory_context:
            parts.append(f"### Recalled Memories (auto-searched)\n\n{memory_context}")
        if parts:
            sections.append("## Memory\n\n" + "\n\n".join(parts))
        sections.append(
            "## Memory Instructions\n\n"
            "- Use memory_write to save important user facts and preferences.\n"
            "- Reference remembered facts naturally in conversation.\n"
            "- Use memory_search to recall specific past information.\n"
            "- Use memory_forget to remove outdated memories by date or category.")
    if mode in ("full", "minimal"):
        for name in ["HEARTBEAT.md", "BOOTSTRAP.md", "AGENTS.md", "USER.md"]:
            content = bootstrap.get(name, "").strip()
            if content:
                sections.append(f"## {name.replace('.md', '')}\n\n{content}")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    sections.append(
        f"## Runtime Context\n\n- Agent ID: {agent_id}\n- Model: {MODEL_ID}\n"
        f"- Channel: {channel}\n- Current time: {now}\n- Prompt mode: {mode}")
    hints = {
        "terminal": "You are responding via a terminal REPL. Markdown is supported.",
        "cli": "You are responding via a terminal REPL. Markdown is supported.",
        "telegram": "You are responding via Telegram. Keep messages concise.",
        "feishu": "You are responding via Feishu. Keep messages concise.",
    }
    sections.append(f"## Channel\n\n{hints.get(channel, f'You are responding via {channel}.')}")
    return "\n\n".join(sections)


# ---------------------------------------------------------------------------
# 记忆系统 (s06, 含硬遗忘)
# ---------------------------------------------------------------------------

class MemoryStore:
    def __init__(self, workspace_dir: Path) -> None:
        self.workspace_dir = workspace_dir
        self.memory_dir = workspace_dir / "memory" / "daily"
        self.memory_dir.mkdir(parents=True, exist_ok=True)
        self.retention_days = int(os.getenv("MEMORY_RETENTION_DAYS", "30"))
        self.auto_expired = 0
        self.explicit_forgotten = 0

    def _retention_cutoff(self) -> datetime:
        return datetime.now(timezone.utc) - timedelta(days=self.retention_days)

    def _purge_over_retention(self) -> int:
        if not self.memory_dir.is_dir():
            return 0
        cutoff = self._retention_cutoff()
        removed = 0
        for jf in list(self.memory_dir.glob("*.jsonl")):
            m = re.search(r"(\d{4}-\d{2}-\d{2})", jf.name)
            if not m:
                continue
            try:
                file_date = datetime.strptime(m.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if file_date < cutoff:
                try:
                    with open(jf, encoding="utf-8") as f:
                        removed += sum(1 for line in f if line.strip())
                    jf.unlink()
                except Exception:
                    continue
        self.auto_expired += removed
        return removed

    def write_memory(self, content: str, category: str = "general", ttl_hours: float | None = None) -> str:
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")
        path = self.memory_dir / f"{today}.jsonl"
        entry: dict[str, Any] = {"ts": now.isoformat(), "category": category, "content": content}
        if ttl_hours is not None:
            entry["expires_at"] = (now + timedelta(hours=ttl_hours)).isoformat()
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            ttl_note = f", expires_at={entry['expires_at']}" if ttl_hours is not None else ""
            return f"Memory saved to {today}.jsonl ({category}{ttl_note})"
        except Exception as exc:
            return f"Error writing memory: {exc}"

    def load_evergreen(self) -> str:
        path = self.workspace_dir / "MEMORY.md"
        if not path.is_file():
            return ""
        try:
            return path.read_text(encoding="utf-8").strip()
        except Exception:
            return ""

    def _load_all_chunks(self) -> list[dict[str, str]]:
        chunks: list[dict[str, str]] = []
        evergreen = self.load_evergreen()
        if evergreen:
            for para in evergreen.split("\n\n"):
                para = para.strip()
                if para:
                    chunks.append({"path": "MEMORY.md", "text": para})
        self._purge_over_retention()
        if self.memory_dir.is_dir():
            now = datetime.now(timezone.utc)
            for jf in sorted(self.memory_dir.glob("*.jsonl")):
                try:
                    for line in jf.read_text(encoding="utf-8").splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        entry = json.loads(line)
                        exp = entry.get("expires_at")
                        if exp:
                            try:
                                if datetime.fromisoformat(exp) < now:
                                    self.auto_expired += 1
                                    continue
                            except (ValueError, TypeError):
                                pass
                        text = entry.get("content", "")
                        if text:
                            cat = entry.get("category", "")
                            label = f"{jf.name} [{cat}]" if cat else jf.name
                            chunks.append({"path": label, "text": text})
                except Exception:
                    continue
        return chunks

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        tokens = re.findall(r"[a-z0-9一-鿿]+", text.lower())
        return [t for t in tokens if len(t) > 1 or "一" <= t <= "鿿"]

    @staticmethod
    def _hash_vector(text: str, dim: int = 64) -> list[float]:
        tokens = MemoryStore._tokenize(text)
        vec = [0.0] * dim
        for token in tokens:
            h = hash(token)
            for i in range(dim):
                bit = (h >> (i % 62)) & 1
                vec[i] += 1.0 if bit else -1.0
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]

    @staticmethod
    def _vector_cosine(a: list[float], b: list[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b))
        na = math.sqrt(sum(x * x for x in a))
        nb = math.sqrt(sum(x * x for x in b))
        return dot / (na * nb) if na and nb else 0.0

    @staticmethod
    def _jaccard(a: list[str], b: list[str]) -> float:
        sa, sb = set(a), set(b)
        inter = len(sa & sb)
        union = len(sa | sb)
        return inter / union if union else 0.0

    def _keyword_search(self, query: str, chunks: list[dict[str, str]], top_k: int = 10) -> list[dict[str, Any]]:
        qt = self._tokenize(query)
        if not qt:
            return []
        ct = [self._tokenize(c["text"]) for c in chunks]
        n = len(chunks)
        df: dict[str, int] = {}
        for tokens in ct:
            for t in set(tokens):
                df[t] = df.get(t, 0) + 1

        def tfidf(tokens):
            tf: dict[str, int] = {}
            for t in tokens:
                tf[t] = tf.get(t, 0) + 1
            return {t: c * (math.log((n + 1) / (df.get(t, 0) + 1)) + 1) for t, c in tf.items()}

        def cosine(a, b):
            common = set(a) & set(b)
            if not common:
                return 0.0
            dot = sum(a[k] * b[k] for k in common)
            na = math.sqrt(sum(v * v for v in a.values()))
            nb = math.sqrt(sum(v * v for v in b.values()))
            return dot / (na * nb) if na and nb else 0.0

        qvec = tfidf(qt)
        scored = []
        for i, tokens in enumerate(ct):
            if not tokens:
                continue
            score = cosine(qvec, tfidf(tokens))
            if score > 0.0:
                scored.append({"chunk": chunks[i], "score": score})
        scored.sort(key=lambda x: x["score"], reverse=True)
        return scored[:top_k]

    def _vector_search(self, query: str, chunks: list[dict[str, str]], top_k: int = 10) -> list[dict[str, Any]]:
        qv = self._hash_vector(query)
        scored = []
        for chunk in chunks:
            score = self._vector_cosine(qv, self._hash_vector(chunk["text"]))
            if score > 0.0:
                scored.append({"chunk": chunk, "score": score})
        scored.sort(key=lambda x: x["score"], reverse=True)
        return scored[:top_k]

    @staticmethod
    def _merge_hybrid(vector_results, keyword_results, vw=0.7, tw=0.3):
        merged: dict[str, dict[str, Any]] = {}
        for r in vector_results:
            key = r["chunk"]["text"][:100]
            merged[key] = {"chunk": r["chunk"], "score": r["score"] * vw}
        for r in keyword_results:
            key = r["chunk"]["text"][:100]
            if key in merged:
                merged[key]["score"] += r["score"] * tw
            else:
                merged[key] = {"chunk": r["chunk"], "score": r["score"] * tw}
        result = list(merged.values())
        result.sort(key=lambda x: x["score"], reverse=True)
        return result

    @staticmethod
    def _temporal_decay(results, decay_rate=0.01):
        now = datetime.now(timezone.utc)
        for r in results:
            path = r["chunk"].get("path", "")
            age_days = 0.0
            m = re.search(r"(\d{4}-\d{2}-\d{2})", path)
            if m:
                try:
                    cd = datetime.strptime(m.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    age_days = (now - cd).total_seconds() / 86400.0
                except ValueError:
                    pass
            r["score"] *= math.exp(-decay_rate * age_days)
        return results

    @staticmethod
    def _mmr_rerank(results, lambda_param=0.7):
        if len(results) <= 1:
            return results
        tokenized = [MemoryStore._tokenize(r["chunk"]["text"]) for r in results]
        selected: list[int] = []
        remaining = list(range(len(results)))
        reranked: list[dict[str, Any]] = []
        while remaining:
            best_idx, best_mmr = -1, float("-inf")
            for idx in remaining:
                relevance = results[idx]["score"]
                max_sim = 0.0
                for sel_idx in selected:
                    sim = MemoryStore._jaccard(tokenized[idx], tokenized[sel_idx])
                    if sim > max_sim:
                        max_sim = sim
                mmr = lambda_param * relevance - (1 - lambda_param) * max_sim
                if mmr > best_mmr:
                    best_mmr, best_idx = mmr, idx
            selected.append(best_idx)
            remaining.remove(best_idx)
            reranked.append(results[best_idx])
        return reranked

    def hybrid_search(self, query: str, top_k: int = 5) -> list[dict[str, Any]]:
        chunks = self._load_all_chunks()
        if not chunks:
            return []
        merged = self._merge_hybrid(self._vector_search(query, chunks),
                                    self._keyword_search(query, chunks))
        decayed = self._temporal_decay(merged)
        reranked = self._mmr_rerank(decayed)
        result = []
        for r in reranked[:top_k]:
            snippet = r["chunk"]["text"]
            if len(snippet) > 200:
                snippet = snippet[:200] + "..."
            result.append({"path": r["chunk"]["path"], "score": round(r["score"], 4), "snippet": snippet})
        return result

    def forget(self, category: str | None = None, date: str | None = None) -> int:
        if date is None and category is None:
            return 0
        removed = 0
        if date is not None:
            target = self.memory_dir / f"{date}.jsonl"
            if target.is_file():
                try:
                    with open(target, encoding="utf-8") as f:
                        removed += sum(1 for line in f if line.strip())
                    target.unlink()
                except Exception:
                    pass
            self.explicit_forgotten += removed
            return removed
        if not self.memory_dir.is_dir():
            return 0
        for jf in sorted(self.memory_dir.glob("*.jsonl")):
            try:
                lines = jf.read_text(encoding="utf-8").splitlines()
            except Exception:
                continue
            kept: list[str] = []
            file_removed = 0
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except Exception:
                    kept.append(line); continue
                if entry.get("category") == category:
                    file_removed += 1
                else:
                    kept.append(json.dumps(entry, ensure_ascii=False))
            if file_removed:
                tmp = jf.with_name(jf.name + ".tmp")
                try:
                    with open(tmp, "w", encoding="utf-8") as f:
                        for k in kept:
                            f.write(k + "\n")
                    tmp.replace(jf)
                    removed += file_removed
                except Exception:
                    try:
                        tmp.unlink()
                    except Exception:
                        pass
        self.explicit_forgotten += removed
        return removed

    def get_stats(self) -> dict[str, Any]:
        evergreen = self.load_evergreen()
        self._purge_over_retention()
        daily_files = list(self.memory_dir.glob("*.jsonl")) if self.memory_dir.is_dir() else []
        total_entries = 0
        for f in daily_files:
            try:
                total_entries += sum(1 for line in f.read_text(encoding="utf-8").splitlines() if line.strip())
            except Exception:
                pass
        return {
            "evergreen_chars": len(evergreen),
            "daily_files": len(daily_files),
            "daily_entries": total_entries,
            "total_entries": total_entries,
            "auto_expired": self.auto_expired,
            "explicit_forgotten": self.explicit_forgotten,
        }


# ---------------------------------------------------------------------------
# 网关路由 (s05)
# ---------------------------------------------------------------------------

DEFAULT_AGENT_ID = "default"
VALID_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
INVALID_CHARS_RE = re.compile(r"[^a-z0-9_-]+")


def normalize_agent_id(value: str) -> str:
    trimmed = value.strip()
    if not trimmed:
        return DEFAULT_AGENT_ID
    if VALID_ID_RE.match(trimmed):
        return trimmed.lower()
    cleaned = INVALID_CHARS_RE.sub("-", trimmed.lower()).strip("-")[:64]
    return cleaned or DEFAULT_AGENT_ID


@dataclass
class Binding:
    agent_id: str
    tier: int
    match_key: str
    match_value: str
    priority: int = 0


class BindingTable:
    def __init__(self) -> None:
        self._bindings: list[Binding] = []

    def add(self, binding: Binding) -> None:
        self._bindings.append(binding)
        self._bindings.sort(key=lambda b: (b.tier, -b.priority))

    def list_all(self) -> list[Binding]:
        return list(self._bindings)

    def resolve(self, channel: str = "", account_id: str = "",
                guild_id: str = "", peer_id: str = "") -> tuple[str | None, Binding | None]:
        for b in self._bindings:
            if b.tier == 1 and b.match_key == "peer_id":
                if ":" in b.match_value:
                    if b.match_value == f"{channel}:{peer_id}":
                        return b.agent_id, b
                elif b.match_value == peer_id:
                    return b.agent_id, b
            elif b.tier == 2 and b.match_key == "guild_id" and b.match_value == guild_id:
                return b.agent_id, b
            elif b.tier == 3 and b.match_key == "account_id" and b.match_value == account_id:
                return b.agent_id, b
            elif b.tier == 4 and b.match_key == "channel" and b.match_value == channel:
                return b.agent_id, b
            elif b.tier == 5 and b.match_key == "default":
                return b.agent_id, b
        return None, None


def build_session_key(agent_id: str, channel: str = "", account_id: str = "",
                      peer_id: str = "", dm_scope: str = "per-peer") -> str:
    aid = normalize_agent_id(agent_id)
    ch = (channel or "unknown").strip().lower()
    pid = (peer_id or "").strip().lower()
    if dm_scope == "per-peer" and pid:
        return f"agent:{aid}:direct:{pid}"
    if pid:
        return f"agent:{aid}:{ch}:direct:{pid}"
    return f"agent:{aid}:main"


@dataclass
class AgentConfig:
    id: str
    name: str
    personality: str = ""
    model: str = ""

    @property
    def effective_model(self) -> str:
        return self.model or MODEL_ID


class AgentManager:
    def __init__(self) -> None:
        self._agents: dict[str, AgentConfig] = {}
        self._sessions: dict[str, list[dict]] = {}

    def register(self, config: AgentConfig) -> None:
        aid = normalize_agent_id(config.id)
        config.id = aid
        self._agents[aid] = config

    def get_agent(self, agent_id: str) -> AgentConfig | None:
        return self._agents.get(normalize_agent_id(agent_id))

    def list_agents(self) -> list[AgentConfig]:
        return list(self._agents.values())

    def get_session(self, session_key: str) -> list[dict]:
        if session_key not in self._sessions:
            self._sessions[session_key] = []
        return self._sessions[session_key]

    def list_sessions(self, agent_id: str = "") -> dict[str, int]:
        aid = normalize_agent_id(agent_id) if agent_id else ""
        return {k: len(v) for k, v in self._sessions.items()
                if not aid or k.startswith(f"agent:{aid}:")}


def resolve_route(bindings: BindingTable, mgr: AgentManager,
                  channel: str, peer_id: str,
                  account_id: str = "", guild_id: str = "") -> tuple[str, str]:
    agent_id, matched = bindings.resolve(channel=channel, account_id=account_id,
                                         guild_id=guild_id, peer_id=peer_id)
    if not agent_id:
        return DEFAULT_AGENT_ID, build_session_key(DEFAULT_AGENT_ID, channel, account_id, peer_id)
    session_key = build_session_key(agent_id, channel, account_id, peer_id)
    return agent_id, session_key


# ---------------------------------------------------------------------------
# 韧性: 认证轮换 + 三层重试洋葱 (s09)
# ---------------------------------------------------------------------------

class FailoverReason(Enum):
    rate_limit = "rate_limit"
    auth = "auth"
    timeout = "timeout"
    billing = "billing"
    overflow = "overflow"
    unknown = "unknown"


def classify_failure(exc: Exception) -> FailoverReason:
    msg = str(exc).lower()
    if "rate" in msg or "429" in msg:
        return FailoverReason.rate_limit
    if "auth" in msg or "401" in msg or "key" in msg:
        return FailoverReason.auth
    if "timeout" in msg or "timed out" in msg:
        return FailoverReason.timeout
    if "billing" in msg or "quota" in msg or "402" in msg:
        return FailoverReason.billing
    if "context" in msg or "token" in msg or "overflow" in msg:
        return FailoverReason.overflow
    return FailoverReason.unknown


@dataclass
class AuthProfile:
    name: str
    provider: str
    api_key: str
    base_url: str | None = None
    cooldown_until: float = 0.0
    failure_reason: str | None = None
    last_good_at: float = 0.0


class ProfileManager:
    def __init__(self, profiles: list[AuthProfile]) -> None:
        self.profiles = profiles

    def select_profile(self) -> AuthProfile | None:
        now = time.time()
        for p in self.profiles:
            if now >= p.cooldown_until:
                return p
        return None

    def mark_failure(self, profile: AuthProfile, reason: FailoverReason,
                     cooldown_seconds: float = 300.0) -> None:
        profile.cooldown_until = time.time() + cooldown_seconds
        profile.failure_reason = reason.value
        print_resilience(f"Profile '{profile.name}' -> cooldown {cooldown_seconds:.0f}s ({reason.value})")

    def mark_success(self, profile: AuthProfile) -> None:
        profile.failure_reason = None
        profile.last_good_at = time.time()

    def list_profiles(self) -> list[dict[str, Any]]:
        now = time.time()
        result = []
        for p in self.profiles:
            remaining = max(0, p.cooldown_until - now)
            status = "available" if remaining == 0 else f"cooldown ({remaining:.0f}s)"
            result.append({"name": p.name, "provider": p.provider, "status": status,
                           "failure_reason": p.failure_reason,
                           "last_good": (time.strftime("%H:%M:%S", time.localtime(p.last_good_at))
                                         if p.last_good_at > 0 else "never")})
        return result


MAX_OVERFLOW_COMPACTION = 3


class ResilientAgent:
    """三层重试洋葱: L1 认证轮换 -> L2 溢出压缩 -> L3 工具循环."""

    def __init__(self, profile_manager: ProfileManager, model_id: str,
                 context_guard: ContextGuard | None = None,
                 fallback_models: list[str] | None = None,
                 tool_handlers: dict[str, Any] | None = None) -> None:
        self.pm = profile_manager
        self.model_id = model_id
        self.fallback_models = fallback_models or []
        self.guard = context_guard or ContextGuard()
        self.tool_handlers = tool_handlers or {}
        self.total_attempts = 0
        self.total_successes = 0
        self.total_failures = 0
        self.total_compactions = 0
        self.total_rotations = 0

    def _run_attempt(self, api_client: Anthropic, model: str, system: str,
                     messages: list[dict], tools: list[dict]) -> tuple[Any, list[dict]]:
        """Layer 3: 工具循环 (与 s06/s10 一致). 返回 (final_response, updated_messages)."""
        local = list(messages)
        while True:
            response = api_client.messages.create(
                model=model, max_tokens=8096, system=system,
                tools=tools, messages=local,
            )
            local.append({"role": "assistant", "content": response.content})
            if response.stop_reason == "end_turn":
                return response, local
            elif response.stop_reason == "tool_use":
                results = []
                for block in response.content:
                    if getattr(block, "type", None) != "tool_use":
                        continue
                    print_tool(block.name, json.dumps(block.input, ensure_ascii=False)[:80])
                    result = process_tool_call(block.name, block.input, self.tool_handlers)
                    results.append({"type": "tool_result", "tool_use_id": block.id, "content": result})
                local.append({"role": "user", "content": results})
            else:
                return response, local

    def run(self, system: str, messages: list[dict], tools: list[dict]) -> tuple[Any, list[dict]]:
        current = list(messages)
        tried: set[str] = set()
        for _ in range(len(self.pm.profiles)):
            profile = self.pm.select_profile()
            if profile is None:
                print_warn("All profiles on cooldown")
                break
            if profile.name in tried:
                break
            tried.add(profile.name)
            if len(tried) > 1:
                self.total_rotations += 1
                print_resilience(f"Rotating to profile '{profile.name}'")
            api_client = Anthropic(api_key=profile.api_key, base_url=profile.base_url)
            layer2 = list(current)
            for attempt in range(MAX_OVERFLOW_COMPACTION):
                try:
                    self.total_attempts += 1
                    result, layer2 = self._run_attempt(api_client, self.model_id,
                                                        system, layer2, tools)
                    self.pm.mark_success(profile)
                    self.total_successes += 1
                    return result, layer2
                except Exception as exc:
                    reason = classify_failure(exc)
                    self.total_failures += 1
                    if reason == FailoverReason.overflow:
                        if attempt < MAX_OVERFLOW_COMPACTION - 1:
                            self.total_compactions += 1
                            print_resilience(f"Overflow ({attempt+1}/{MAX_OVERFLOW_COMPACTION}), compacting...")
                            layer2 = self.guard.truncate_tool_results(layer2)
                            layer2 = self.guard.compact_history(layer2, api_client, self.model_id)
                            continue
                        self.pm.mark_failure(profile, reason, cooldown_seconds=600)
                        break
                    elif reason in (FailoverReason.auth, FailoverReason.billing):
                        self.pm.mark_failure(profile, reason, cooldown_seconds=300); break
                    elif reason == FailoverReason.rate_limit:
                        self.pm.mark_failure(profile, reason, cooldown_seconds=120); break
                    elif reason == FailoverReason.timeout:
                        self.pm.mark_failure(profile, reason, cooldown_seconds=60); break
                    else:
                        self.pm.mark_failure(profile, reason, cooldown_seconds=120); break
        if self.fallback_models:
            print_resilience("Profiles exhausted, trying fallback models...")
            for fm in self.fallback_models:
                profile = self.pm.select_profile()
                if profile is None:
                    for p in self.pm.profiles:
                        if p.failure_reason in (FailoverReason.rate_limit.value,
                                                FailoverReason.timeout.value):
                            p.cooldown_until = 0.0
                    profile = self.pm.select_profile()
                if profile is None:
                    continue
                print_resilience(f"Fallback: model='{fm}', profile='{profile.name}'")
                api_client = Anthropic(api_key=profile.api_key, base_url=profile.base_url)
                try:
                    self.total_attempts += 1
                    result, updated = self._run_attempt(api_client, fm, system, current, tools)
                    self.pm.mark_success(profile)
                    self.total_successes += 1
                    return result, updated
                except Exception:
                    continue
        raise RuntimeError("All profiles and fallback models exhausted")


# ---------------------------------------------------------------------------
# 可靠投递 (s08)
# ---------------------------------------------------------------------------

@dataclass
class QueuedDelivery:
    id: str
    channel: str
    to: str
    text: str
    retry_count: int = 0
    last_error: str | None = None
    enqueued_at: float = field(default_factory=time.time)
    next_retry_at: float = 0.0

    def to_dict(self) -> dict:
        return {"id": self.id, "channel": self.channel, "to": self.to, "text": self.text,
                "retry_count": self.retry_count, "last_error": self.last_error,
                "enqueued_at": self.enqueued_at, "next_retry_at": self.next_retry_at}

    @staticmethod
    def from_dict(d: dict) -> "QueuedDelivery":
        return QueuedDelivery(id=d["id"], channel=d["channel"], to=d["to"], text=d["text"],
                              retry_count=d.get("retry_count", 0), last_error=d.get("last_error"),
                              enqueued_at=d.get("enqueued_at", 0.0),
                              next_retry_at=d.get("next_retry_at", 0.0))


def compute_backoff_ms(retry_count: int) -> int:
    if retry_count <= 0:
        return 0
    idx = min(retry_count - 1, len(BACKOFF_MS) - 1)
    base = BACKOFF_MS[idx]
    jitter = random.randint(-base // 5, base // 5)
    return max(0, base + jitter)


def chunk_message(text: str, channel: str = "default") -> list[str]:
    if not text:
        return []
    limit = CHANNEL_LIMITS.get(channel, CHANNEL_LIMITS["default"])
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    for para in text.split("\n\n"):
        if chunks and len(chunks[-1]) + len(para) + 2 <= limit:
            chunks[-1] += "\n\n" + para
        else:
            while len(para) > limit:
                chunks.append(para[:limit])
                para = para[limit:]
            if para:
                chunks.append(para)
    return chunks or [text[:limit]]


class DeliveryQueue:
    def __init__(self, queue_dir: Path | None = None):
        self.queue_dir = queue_dir or (STATE_DIR / "delivery")
        self.failed_dir = self.queue_dir / "failed"
        self.queue_dir.mkdir(parents=True, exist_ok=True)
        self.failed_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def enqueue(self, channel: str, to: str, text: str) -> str:
        delivery_id = uuid.uuid4().hex[:12]
        entry = QueuedDelivery(id=delivery_id, channel=channel, to=to, text=text,
                               enqueued_at=time.time(), next_retry_at=0.0)
        self._write_entry(entry)
        return delivery_id

    def _write_entry(self, entry: QueuedDelivery) -> None:
        final = self.queue_dir / f"{entry.id}.json"
        tmp = self.queue_dir / f".tmp.{os.getpid()}.{entry.id}.json"
        data = json.dumps(entry.to_dict(), indent=2, ensure_ascii=False)
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(data); f.flush(); os.fsync(f.fileno())
        os.replace(str(tmp), str(final))

    def _read_entry(self, delivery_id: str) -> QueuedDelivery | None:
        path = self.queue_dir / f"{delivery_id}.json"
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return QueuedDelivery.from_dict(json.load(f))
        except (json.JSONDecodeError, KeyError):
            return None

    def ack(self, delivery_id: str) -> None:
        try:
            (self.queue_dir / f"{delivery_id}.json").unlink()
        except FileNotFoundError:
            pass

    def fail(self, delivery_id: str, error: str) -> None:
        entry = self._read_entry(delivery_id)
        if entry is None:
            return
        entry.retry_count += 1
        entry.last_error = error
        if entry.retry_count >= MAX_RETRIES:
            self._move_to_failed(delivery_id); return
        entry.next_retry_at = time.time() + compute_backoff_ms(entry.retry_count) / 1000.0
        self._write_entry(entry)

    def _move_to_failed(self, delivery_id: str) -> None:
        src = self.queue_dir / f"{delivery_id}.json"
        dst = self.failed_dir / f"{delivery_id}.json"
        try:
            os.replace(str(src), str(dst))
        except FileNotFoundError:
            pass

    def load_pending(self) -> list[QueuedDelivery]:
        entries: list[QueuedDelivery] = []
        for fp in self.queue_dir.glob("*.json"):
            if not fp.is_file():
                continue
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    entries.append(QueuedDelivery.from_dict(json.load(f)))
            except (json.JSONDecodeError, KeyError, OSError):
                continue
        entries.sort(key=lambda e: e.enqueued_at)
        return entries

    def load_failed(self) -> list[QueuedDelivery]:
        entries: list[QueuedDelivery] = []
        for fp in self.failed_dir.glob("*.json"):
            if not fp.is_file():
                continue
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    entries.append(QueuedDelivery.from_dict(json.load(f)))
            except (json.JSONDecodeError, KeyError, OSError):
                continue
        entries.sort(key=lambda e: e.enqueued_at)
        return entries

    def retry_failed(self) -> int:
        count = 0
        for fp in list(self.failed_dir.glob("*.json")):
            if not fp.is_file():
                continue
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    entry = QueuedDelivery.from_dict(json.load(f))
                entry.retry_count = 0; entry.last_error = None; entry.next_retry_at = 0.0
                self._write_entry(entry)
                fp.unlink()
                count += 1
            except (json.JSONDecodeError, KeyError, OSError):
                continue
        return count

    def stats(self) -> dict[str, int]:
        return {"pending": len(self.load_pending()), "failed": len(self.load_failed())}


class DeliveryRunner:
    def __init__(self, queue: DeliveryQueue, deliver_fn: Callable[[str, str, str], bool]):
        self.queue = queue
        self.deliver_fn = deliver_fn
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                now = time.time()
                for entry in self.queue.load_pending():
                    if entry.next_retry_at and now < entry.next_retry_at:
                        continue
                    ok = False
                    err = ""
                    try:
                        ok = self.deliver_fn(entry.channel, entry.to, entry.text)
                    except Exception as exc:
                        err = str(exc)
                    if ok:
                        self.queue.ack(entry.id)
                    else:
                        self.queue.fail(entry.id, err or "send returned False")
            except Exception:
                pass
            self._stop.wait(timeout=1.0)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="delivery-runner")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3.0)
            self._thread = None


# ---------------------------------------------------------------------------
# 并发: 命名车道 (s10)
# ---------------------------------------------------------------------------

LANE_MAIN = "main"
LANE_CRON = "cron"
LANE_HEARTBEAT = "heartbeat"


class LaneQueue:
    def __init__(self, name: str, max_concurrency: int = 1) -> None:
        self.name = name
        self.max_concurrency = max(1, max_concurrency)
        self._deque: deque[tuple[Callable, concurrent.futures.Future, int]] = deque()
        self._condition = threading.Condition()
        self._active_count = 0
        self._generation = 0

    @property
    def generation(self) -> int:
        with self._condition:
            return self._generation

    @generation.setter
    def generation(self, value: int) -> None:
        with self._condition:
            self._generation = value
            self._condition.notify_all()

    def enqueue(self, fn: Callable[[], Any], generation: int | None = None) -> concurrent.futures.Future:
        future: concurrent.futures.Future = concurrent.futures.Future()
        with self._condition:
            gen = generation if generation is not None else self._generation
            self._deque.append((fn, future, gen))
            self._pump()
        return future

    def _pump(self) -> None:
        while self._active_count < self.max_concurrency and self._deque:
            fn, future, gen = self._deque.popleft()
            self._active_count += 1
            t = threading.Thread(target=self._run_task, args=(fn, future, gen),
                                 daemon=True, name=f"lane-{self.name}")
            t.start()

    def _run_task(self, fn, future, gen) -> None:
        try:
            future.set_result(fn())
        except Exception as exc:
            future.set_exception(exc)
        finally:
            self._task_done(gen)

    def _task_done(self, gen: int) -> None:
        with self._condition:
            self._active_count -= 1
            if gen == self._generation:
                self._pump()
            self._condition.notify_all()

    def wait_for_idle(self, timeout: float | None = None) -> bool:
        deadline = (time.monotonic() + timeout) if timeout is not None else None
        with self._condition:
            while self._active_count > 0 or len(self._deque) > 0:
                remaining = None
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                self._condition.wait(timeout=remaining)
            return True

    def stats(self) -> dict[str, Any]:
        with self._condition:
            return {"name": self.name, "queue_depth": len(self._deque),
                    "active": self._active_count, "max_concurrency": self.max_concurrency,
                    "generation": self._generation}


class CommandQueue:
    def __init__(self) -> None:
        self._lanes: dict[str, LaneQueue] = {}
        self._lock = threading.Lock()

    def get_or_create_lane(self, name: str, max_concurrency: int = 1) -> LaneQueue:
        with self._lock:
            if name not in self._lanes:
                self._lanes[name] = LaneQueue(name, max_concurrency)
            return self._lanes[name]

    def enqueue(self, lane_name: str, fn: Callable[[], Any]) -> concurrent.futures.Future:
        return self.get_or_create_lane(lane_name).enqueue(fn)

    def reset_all(self) -> dict[str, int]:
        result: dict[str, int] = {}
        with self._lock:
            for name, lane in self._lanes.items():
                with lane._condition:
                    lane._generation += 1
                    result[name] = lane._generation
        return result

    def wait_for_all(self, timeout: float = 10.0) -> bool:
        deadline = time.monotonic() + timeout
        with self._lock:
            lanes = list(self._lanes.values())
        for lane in lanes:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            if not lane.wait_for_idle(timeout=remaining):
                return False
        return True

    def stats(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {name: lane.stats() for name, lane in self._lanes.items()}

    def lane_names(self) -> list[str]:
        with self._lock:
            return list(self._lanes.keys())


# ---------------------------------------------------------------------------
# 心跳 (s07/s10, 经车道)
# ---------------------------------------------------------------------------

def run_agent_single_turn(prompt: str, system_prompt: str | None = None) -> str:
    """单轮 LLM 调用 (心跳/cron 共用), 无工具."""
    sys_prompt = system_prompt or "You are a helpful assistant performing a background check."
    try:
        response = client.messages.create(
            model=MODEL_ID, max_tokens=2048, system=sys_prompt,
            messages=[{"role": "user", "content": prompt}],
        )
        return "".join(b.text for b in response.content if hasattr(b, "text")).strip()
    except Exception as exc:
        return f"[agent error: {exc}]"


class HeartbeatRunner:
    def __init__(self, workspace: Path, command_queue: CommandQueue,
                 interval: float = 1800.0, active_hours: tuple[int, int] = (9, 22)) -> None:
        self.workspace = workspace
        self.heartbeat_path = workspace / "HEARTBEAT.md"
        self.cq = command_queue
        self.interval = interval
        self.active_hours = active_hours
        self.last_run_at: float = 0.0
        self._stopped = False
        self._thread: threading.Thread | None = None
        self._output_queue: list[str] = []
        self._queue_lock = threading.Lock()
        self._last_output = ""

    def should_run(self) -> tuple[bool, str]:
        if not self.heartbeat_path.exists():
            return False, "HEARTBEAT.md not found"
        if not self.heartbeat_path.read_text(encoding="utf-8").strip():
            return False, "HEARTBEAT.md is empty"
        elapsed = time.time() - self.last_run_at
        if elapsed < self.interval:
            return False, f"interval not elapsed ({self.interval - elapsed:.0f}s remaining)"
        hour = datetime.now().hour
        s, e = self.active_hours
        in_hours = (s <= hour < e) if s <= e else not (e <= hour < s)
        if not in_hours:
            return False, f"outside active hours ({s}:00-{e}:00)"
        return True, "all checks passed"

    def _build_prompt(self, soul_text: str, mem_text: str) -> tuple[str, str]:
        instructions = self.heartbeat_path.read_text(encoding="utf-8").strip()
        extra = ""
        if mem_text:
            extra = f"## Known Context\n\n{mem_text}\n\n"
        extra += f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        sys_prompt = (soul_text or "You are a helpful assistant.") + "\n\n" + extra
        return instructions, sys_prompt

    def _parse(self, response: str) -> str | None:
        if "HEARTBEAT_OK" in response:
            stripped = response.replace("HEARTBEAT_OK", "").strip()
            return stripped if len(stripped) > 5 else None
        return response.strip() or None

    def heartbeat_tick(self, soul_text: str, mem_text: str) -> None:
        ok, _ = self.should_run()
        if not ok:
            return
        lane = self.cq.get_or_create_lane(LANE_HEARTBEAT)
        if lane.stats()["active"] > 0:
            return

        def _do() -> str | None:
            instructions, sys_prompt = self._build_prompt(soul_text, mem_text)
            if not instructions:
                return None
            return self._parse(run_agent_single_turn(instructions, sys_prompt))

        future = self.cq.enqueue(LANE_HEARTBEAT, _do)

        def _on_done(f: concurrent.futures.Future) -> None:
            self.last_run_at = time.time()
            try:
                meaningful = f.result()
                if meaningful is None:
                    return
                if meaningful.strip() == self._last_output:
                    return
                self._last_output = meaningful.strip()
                with self._queue_lock:
                    self._output_queue.append(meaningful)
                print_lane(LANE_HEARTBEAT, f"output queued ({len(meaningful)} chars)")
            except Exception as exc:
                with self._queue_lock:
                    self._output_queue.append(f"[heartbeat error: {exc}]")

        future.add_done_callback(_on_done)

    def _loop(self, soul_text: str, mem_text: str) -> None:
        while not self._stopped:
            try:
                self.heartbeat_tick(soul_text, mem_text)
            except Exception:
                pass
            time.sleep(1.0)

    def start(self, soul_text: str, mem_text: str) -> None:
        if self._thread is not None:
            return
        self._stopped = False
        self._thread = threading.Thread(target=self._loop, args=(soul_text, mem_text),
                                        daemon=True, name="heartbeat-timer")
        self._thread.start()

    def stop(self) -> None:
        self._stopped = True
        if self._thread:
            self._thread.join(timeout=3.0)
            self._thread = None

    def drain_output(self) -> list[str]:
        with self._queue_lock:
            items = list(self._output_queue)
            self._output_queue.clear()
            return items

    def status(self) -> dict[str, Any]:
        now = time.time()
        elapsed = now - self.last_run_at if self.last_run_at > 0 else None
        next_in = max(0.0, self.interval - elapsed) if elapsed is not None else self.interval
        ok, reason = self.should_run()
        with self._queue_lock:
            qsize = len(self._output_queue)
        return {
            "enabled": self.heartbeat_path.exists(), "should_run": ok, "reason": reason,
            "last_run": datetime.fromtimestamp(self.last_run_at).isoformat() if self.last_run_at > 0 else "never",
            "next_in": f"{round(next_in)}s", "interval": f"{self.interval}s",
            "active_hours": f"{self.active_hours[0]}:00-{self.active_hours[1]}:00",
            "queue_size": qsize,
        }


# ---------------------------------------------------------------------------
# Cron (s07, croniter 真 cron 表达式, 经车道)
# ---------------------------------------------------------------------------

class CronService:
    def __init__(self, cron_file: Path, command_queue: CommandQueue) -> None:
        self.cron_file = cron_file
        self.cq = command_queue
        self.jobs: list[dict[str, Any]] = []
        self._output_queue: list[str] = []
        self._queue_lock = threading.Lock()
        self.load_jobs()

    def load_jobs(self) -> None:
        self.jobs.clear()
        if not self.cron_file.exists():
            return
        try:
            raw = json.loads(self.cron_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return
        now = datetime.now(timezone.utc)
        for jd in raw.get("jobs", []):
            sched = jd.get("schedule", {})
            cron_expr = sched.get("cron", "")
            every = sched.get("every_seconds", 0)
            if not cron_expr and every <= 0:
                continue
            next_run = 0.0
            if cron_expr and HAS_CRON:
                try:
                    next_run = croniter(cron_expr, now).get_next(datetime).timestamp()
                except Exception:
                    continue
            elif every > 0:
                next_run = time.time() + every
            else:
                continue
            self.jobs.append({
                "id": jd.get("id", ""), "name": jd.get("name", ""),
                "enabled": jd.get("enabled", True), "cron": cron_expr,
                "every_seconds": every, "payload": jd.get("payload", {}),
                "last_run_at": 0.0, "next_run_at": next_run,
                "consecutive_errors": 0,
            })

    def cron_tick(self) -> None:
        now = time.time()
        for job in self.jobs:
            if not job["enabled"]:
                continue
            if now < job["next_run_at"]:
                continue
            self._enqueue_job(job, now)

    def _enqueue_job(self, job: dict[str, Any], now: float) -> None:
        payload = job["payload"]
        message = payload.get("message", "")
        job_name = job["name"]
        cron_expr = job.get("cron", "")
        every = job.get("every_seconds", 0)
        if not message:
            self._advance_next(job, cron_expr, every, now)
            return

        def _do() -> str:
            sys_prompt = ("You are performing a scheduled background task. Be concise. "
                          f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            return run_agent_single_turn(message, sys_prompt)

        future = self.cq.enqueue(LANE_CRON, _do)

        def _on_done(f: concurrent.futures.Future, j: dict = job, n: str = job_name) -> None:
            j["last_run_at"] = time.time()
            try:
                result = f.result()
                j["consecutive_errors"] = 0
                if result:
                    with self._queue_lock:
                        self._output_queue.append(f"[{n}] {result}")
                    print_lane(LANE_CRON, f"job '{n}' completed")
            except Exception as exc:
                j["consecutive_errors"] += 1
                with self._queue_lock:
                    self._output_queue.append(f"[{n}] error: {exc}")
                if j["consecutive_errors"] >= 5:
                    j["enabled"] = False
                    print_lane(LANE_CRON, f"job '{n}' auto-disabled after 5 errors")
            self._advance_next(j, j.get("cron", ""), j.get("every_seconds", 0), time.time())

        future.add_done_callback(_on_done)
        self._advance_next(job, cron_expr, every, now)

    @staticmethod
    def _advance_next(job: dict, cron_expr: str, every: int, now_ts: float) -> None:
        if cron_expr and HAS_CRON:
            try:
                job["next_run_at"] = croniter(cron_expr, datetime.now(timezone.utc)).get_next(datetime).timestamp()
                return
            except Exception:
                pass
        job["next_run_at"] = now_ts + max(every, 60)

    def drain_output(self) -> list[str]:
        with self._queue_lock:
            items = list(self._output_queue)
            self._output_queue.clear()
            return items

    def list_jobs(self) -> list[dict[str, Any]]:
        now = time.time()
        result = []
        for j in self.jobs:
            nxt = max(0.0, j["next_run_at"] - now) if j["next_run_at"] > 0 else None
            result.append({
                "id": j["id"], "name": j["name"], "enabled": j["enabled"],
                "cron": j.get("cron", "") or f"every {j.get('every_seconds', 0)}s",
                "errors": j["consecutive_errors"],
                "last_run": datetime.fromtimestamp(j["last_run_at"]).isoformat() if j["last_run_at"] > 0 else "never",
                "next_in": round(nxt) if nxt is not None else None,
            })
        return result


# ---------------------------------------------------------------------------
# Agent 回合 (集成接线核心)
# ---------------------------------------------------------------------------

def run_agent_turn(inbound: InboundMessage, mgr: AgentManager, bindings: BindingTable,
                   soul_text: str, bootstrap: dict[str, str], memory_store: MemoryStore,
                   resilient: ResilientAgent, delivery: DeliveryQueue,
                   tool_handlers: dict[str, Any]) -> str:
    """端到端: 路由 -> 会话 -> 提示词(含 auto recall) -> 韧性工具循环 -> 投递回复."""
    agent_id, session_key = resolve_route(bindings, mgr, inbound.channel, inbound.peer_id,
                                         account_id=inbound.account_id)
    messages = mgr.get_session(session_key)
    messages.append({"role": "user", "content": inbound.text})

    # auto-recall: 用入站文本检索记忆注入提示词
    recall = memory_store.hybrid_search(inbound.text, top_k=3)
    memory_context = "\n".join(f"- [{r['path']}] {r['snippet']}" for r in recall) if recall else ""
    if memory_context:
        print_info("  [auto-recall] 找到相关记忆")

    system_prompt = build_system_prompt(
        mode="full", bootstrap=bootstrap, memory_context=memory_context,
        agent_id=agent_id, channel=inbound.channel or "terminal",
    )
    sys_base = (soul_text or "You are a helpful assistant.") + "\n\n" + system_prompt

    try:
        response, messages = resilient.run(sys_base, messages, TOOLS)
    except Exception as exc:
        # 失败时回滚最后一条 user 消息
        while messages and messages[-1]["role"] != "user":
            messages.pop()
        if messages:
            messages.pop()
        text = f"[agent error: {exc}]"
        print_error(text)
    else:
        text = "".join(getattr(b, "text", "") for b in response.content if hasattr(b, "text")) if response else ""

    # 回复一律经投递队列 (崩溃不丢, 失败退避重试)
    if text:
        delivery.enqueue(inbound.channel or "cli", inbound.peer_id, text)
    return text


# ---------------------------------------------------------------------------
# REPL + 主循环
# ---------------------------------------------------------------------------

def _init_profiles() -> list[AuthProfile]:
    """从 env 构造认证 profile 池 (支持多 key 轮换)."""
    profiles: list[AuthProfile] = []
    primary = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if primary:
        profiles.append(AuthProfile(name="primary", provider="anthropic",
                                    api_key=primary, base_url=ANTHROPIC_BASE_URL))
    for i in range(2, 6):
        k = os.getenv(f"ANTHROPIC_API_KEY_{i}", "").strip()
        if k:
            profiles.append(AuthProfile(name=f"key{i}", provider="anthropic",
                                        api_key=k, base_url=ANTHROPIC_BASE_URL))
    return profiles


def print_repl_help() -> None:
    print_info("REPL 命令:")
    print_info("  /channels /accounts                 -- 通道/账户")
    print_info("  /bindings /agents /sessions         -- 网关路由")
    print_info("  /soul /prompt                       -- 灵魂/完整提示词")
    print_info("  /memory /search <q> /forget date=..|category=..  -- 记忆(含硬遗忘)")
    print_info("  /heartbeat /trigger /cron           -- 心跳/cron")
    print_info("  /lanes /queue /enqueue <lane> <msg> /concurrency <lane> <N> /generation /reset  -- 车道")
    print_info("  /delivery /profiles                 -- 投递队列/认证轮换")
    print_info("  /help   quit/exit                   -- 帮助/退出")


def agent_loop() -> None:
    bootstrap_loader = BootstrapLoader(WORKSPACE_DIR)
    bootstrap = bootstrap_loader.load_all(mode="full")
    soul_text = bootstrap.get("SOUL.md", "").strip()
    memory_store = MemoryStore(WORKSPACE_DIR)
    mem_evergreen = memory_store.load_evergreen()

    # 通道
    mgr_ch = ChannelManager()
    cli = CLIChannel()
    mgr_ch.register(cli, ChannelAccount(channel="cli", account_id="cli-local"))

    fs_id = os.getenv("FEISHU_APP_ID", "").strip()
    fs_secret = os.getenv("FEISHU_APP_SECRET", "").strip()
    fs_channel: FeishuChannel | None = None
    if fs_id and fs_secret and HAS_HTTPX:
        fs_channel = FeishuChannel(ChannelAccount(
            channel="feishu", account_id=fs_id,
            config={"app_id": fs_id, "app_secret": fs_secret,
                    "bot_open_id": os.getenv("FEISHU_BOT_OPEN_ID", ""),
                    "is_lark": os.getenv("FEISHU_IS_LARK", "").lower() in ("1", "true")}))
        mgr_ch.register(fs_channel, ChannelAccount(channel="feishu", account_id=fs_id))

    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    tg_channel: TelegramChannel | None = None
    if tg_token and HAS_HTTPX:
        tg_channel = TelegramChannel(ChannelAccount(
            channel="telegram", account_id=tg_token.split(":")[0], token=tg_token,
            config={"allowed_chats": os.getenv("TELEGRAM_ALLOWED_CHATS", "")}))
        mgr_ch.register(tg_channel, ChannelAccount(
            channel="telegram", account_id=tg_token.split(":")[0], token=tg_token))

    # 网关: 单 agent 默认绑定
    bindings = BindingTable()
    bindings.add(Binding(agent_id=DEFAULT_AGENT_ID, tier=5, match_key="default", match_value="*"))
    agent_mgr = AgentManager()
    agent_mgr.register(AgentConfig(id=DEFAULT_AGENT_ID, name="claw0"))

    # 韧性
    profiles = _init_profiles()
    pm = ProfileManager(profiles)
    tool_handlers = dict(_BASE_TOOL_HANDLERS)
    tool_handlers["memory_write"] = lambda content, category="general", ttl_hours=None: \
        memory_store.write_memory(content, category, ttl_hours=ttl_hours)
    tool_handlers["memory_search"] = lambda query, top_k=5: \
        (lambda r: "\n".join(f"[{x['path']}] (score: {x['score']}) {x['snippet']}" for x in r)
         if r else "No relevant memories found.")(memory_store.hybrid_search(query, top_k))
    tool_handlers["memory_forget"] = lambda category=None, date=None: \
        (lambda n: "No matching memories to forget." if n == 0
         else (f"Forgot {n} entries from {date}" if date else f"Forgot {n} entries (category={category})")
        )(memory_store.forget(category=category, date=date))

    resilient = ResilientAgent(pm, MODEL_ID, ContextGuard(),
                               fallback_models=[], tool_handlers=tool_handlers)

    # 投递
    delivery = DeliveryQueue()
    def _deliver(channel: str, to: str, text: str) -> bool:
        ch = mgr_ch.get(channel)
        if ch is None:
            print_error(f"[delivery] no channel '{channel}'")
            return False
        ok = True
        for chunk in chunk_message(text, channel):
            if not ch.send(to, chunk):
                ok = False
        return ok
    delivery_runner = DeliveryRunner(delivery, _deliver)
    delivery_runner.start()

    # 车道
    cq = CommandQueue()
    cq.get_or_create_lane(LANE_MAIN, max_concurrency=1)
    cq.get_or_create_lane(LANE_CRON, max_concurrency=1)
    cq.get_or_create_lane(LANE_HEARTBEAT, max_concurrency=1)

    # 心跳 + cron
    heartbeat = HeartbeatRunner(
        WORKSPACE_DIR, cq,
        interval=float(os.getenv("HEARTBEAT_INTERVAL", "1800")),
        active_hours=(int(os.getenv("HEARTBEAT_ACTIVE_START", "9")),
                      int(os.getenv("HEARTBEAT_ACTIVE_END", "22"))))
    heartbeat.start(soul_text, mem_evergreen)
    cron_svc = CronService(WORKSPACE_DIR / "CRON.json", cq)
    cron_stop = threading.Event()

    def cron_loop() -> None:
        while not cron_stop.is_set():
            try:
                cron_svc.cron_tick()
            except Exception:
                pass
            cron_stop.wait(timeout=1.0)
    threading.Thread(target=cron_loop, daemon=True, name="cron-tick").start()

    # 飞书长连接
    msg_queue: list[InboundMessage] = []
    q_lock = threading.Lock()
    if fs_channel is not None:
        fs_channel.start_long_connection(msg_queue, q_lock)
    # Telegram 轮询
    tg_stop = threading.Event()
    if tg_channel is not None:
        def tg_poll_loop() -> None:
            while not tg_stop.is_set():
                try:
                    for m in tg_channel.poll():
                        with q_lock:
                            msg_queue.append(m)
                        print_channel(f"  [telegram] {m.sender_id}: {m.text[:80]}")
                except Exception:
                    pass
                tg_stop.wait(timeout=1.0)
        threading.Thread(target=tg_poll_loop, daemon=True, name="telegram-poll").start()

    # 启动横幅
    print_info("=" * 60)
    print_info("  claw0  |  Section 11: Integrated Gateway (集大成)")
    print_info(f"  Model: {MODEL_ID}  |  Profiles: {len(profiles)}")
    print_info(f"  Channels: {', '.join(mgr_ch.list_channels())}")
    print_info(f"  Lanes: {', '.join(cq.lane_names())}")
    hb_st = heartbeat.status()
    print_info(f"  Heartbeat: {'on' if hb_st['enabled'] else 'off'} ({heartbeat.interval}s)  |  Cron jobs: {len(cron_svc.jobs)}")
    print_info(f"  Delivery: pending={delivery.stats()['pending']}  |  Workspace: {WORKSPACE_DIR}")
    print_info("  /help for commands. quit/Ctrl-D to exit.")
    print_info("=" * 60)
    print()

    while True:
        # 排空心跳/cron 输出
        for m in heartbeat.drain_output():
            print_lane(LANE_HEARTBEAT, m)
        for m in cron_svc.drain_output():
            print_lane(LANE_CRON, m)
        # 排空入站消息队列 -> 入队 main 车道
        with q_lock:
            pending = list(msg_queue)
            msg_queue.clear()
        for inbound in pending:
            print_channel(f"  [{inbound.channel}] {inbound.sender_id}: {inbound.text[:80]}")
            cq.enqueue(LANE_MAIN, lambda ib=inbound: run_agent_turn(
                ib, agent_mgr, bindings, soul_text, bootstrap, memory_store,
                resilient, delivery, tool_handlers))

        # 非阻塞 stdin (实时排空, EOF 退出)
        if not select.select([sys.stdin], [], [], 0.5)[0]:
            continue
        try:
            sys.stdout.write(colored_prompt())
            sys.stdout.flush()
            line = sys.stdin.readline()
        except (KeyboardInterrupt, EOFError):
            break
        if line == "":
            break
        user_input = line.strip()
        if not user_input:
            continue
        if user_input.lower() in ("quit", "exit"):
            break

        # REPL 命令
        if user_input.startswith("/"):
            parts = user_input.split(maxsplit=2)
            cmd = parts[0].lower()
            arg = parts[1] if len(parts) > 1 else ""
            if cmd == "/help":
                print_repl_help()
            elif cmd == "/channels":
                for name in mgr_ch.list_channels():
                    print_info(f"  - {name}")
            elif cmd == "/accounts":
                for acc in mgr_ch.accounts:
                    masked = acc.token[:8] + "..." if len(acc.token) > 8 else "(none)"
                    print_info(f"  - {acc.channel}/{acc.account_id}  token={masked}")
            elif cmd == "/bindings":
                for b in bindings.list_all():
                    print_info(f"  tier{b.tier} {b.match_key}={b.match_value} -> {b.agent_id}")
            elif cmd == "/agents":
                for a in agent_mgr.list_agents():
                    print_info(f"  - {a.id} ({a.name}) model={a.effective_model}")
            elif cmd == "/sessions":
                for sk, n in agent_mgr.list_sessions().items():
                    print_info(f"  {sk}: {n} msgs")
            elif cmd == "/soul":
                print_section("SOUL.md")
                print(soul_text or f"{DIM}(未找到 SOUL.md){RESET}")
            elif cmd == "/prompt":
                print_section("完整系统提示词")
                p = build_system_prompt(mode="full", bootstrap=bootstrap,
                                        memory_context=_auto_recall_placeholder(memory_store),
                                        agent_id=DEFAULT_AGENT_ID, channel="terminal")
                print(p[:3000] + (f"\n{DIM}... ({len(p)-3000} more){RESET}" if len(p) > 3000 else ""))
            elif cmd == "/memory":
                print_section("记忆统计")
                s = memory_store.get_stats()
                print_info(f"  长期 (MEMORY.md): {s['evergreen_chars']} 字符")
                print_info(f"  每日文件: {s['daily_files']}  条目: {s['daily_entries']}")
                print_info(f"  自动过期移除: {s['auto_expired']}  显式遗忘: {s['explicit_forgotten']}")
            elif cmd == "/search":
                if not arg:
                    print_warn("用法: /search <query>")
                else:
                    for r in memory_store.hybrid_search(arg):
                        print_info(f"  [{r['score']:.4f}] {r['path']}")
                        print_info(f"    {r['snippet']}")
            elif cmd == "/forget":
                if not arg:
                    print_warn("用法: /forget date=YYYY-MM-DD | /forget category=<cat>")
                else:
                    kwargs = {}
                    for part in arg.split():
                        if "=" in part:
                            k, v = part.split("=", 1)
                            kwargs[k.strip()] = v.strip()
                    n = memory_store.forget(category=kwargs.get("category"), date=kwargs.get("date"))
                    print_info(f"  移除 {n} 条记忆 (evergreen 未触及)")
            elif cmd == "/heartbeat":
                for k, v in heartbeat.status().items():
                    print_info(f"  {k}: {v}")
            elif cmd == "/trigger":
                heartbeat.heartbeat_tick(soul_text, mem_evergreen)
                print_info("  Heartbeat tick triggered.")
                time.sleep(0.5)
                for m in heartbeat.drain_output():
                    print_lane(LANE_HEARTBEAT, m)
            elif cmd == "/cron":
                jobs = cron_svc.list_jobs()
                if not jobs:
                    print_info("  No cron jobs.")
                for j in jobs:
                    tag = f"{GREEN}ON{RESET}" if j["enabled"] else f"{RED}OFF{RESET}"
                    err = f" {YELLOW}err:{j['errors']}{RESET}" if j["errors"] else ""
                    nxt = f" in {j['next_in']}s" if j["next_in"] is not None else ""
                    print(f"  [{tag}] {j['id']} - {j['name']} ({j['cron']}){err}{nxt}")
            elif cmd == "/lanes":
                for name, st in cq.stats().items():
                    bar = "*" * st["active"] + "." * (st["max_concurrency"] - st["active"])
                    print_info(f"  {name:12s} active=[{bar}] queued={st['queue_depth']} max={st['max_concurrency']} gen={st['generation']}")
            elif cmd == "/queue":
                total = sum(st["queue_depth"] for st in cq.stats().values())
                if total == 0:
                    print_info("  All lanes empty.")
                else:
                    for name, st in cq.stats().items():
                        if st["queue_depth"] > 0 or st["active"] > 0:
                            print_info(f"  {name}: {st['queue_depth']} queued, {st['active']} active")
            elif cmd == "/enqueue":
                if len(parts) < 3:
                    print_warn("用法: /enqueue <lane> <message>")
                else:
                    ln, msg = parts[1], parts[2]
                    f = cq.enqueue(ln, lambda m=msg: run_agent_single_turn(m))
                    f.add_done_callback(lambda fut, l=ln: print_lane(l, f"result: {(fut.result() or '')[:200]}"))
                    print_info(f"  Enqueued into '{ln}'.")
            elif cmd == "/concurrency":
                if len(parts) < 3:
                    print_warn("用法: /concurrency <lane> <N>")
                else:
                    try:
                        n = max(1, int(parts[2]))
                    except ValueError:
                        print_warn("N must be integer."); continue
                    lane = cq.get_or_create_lane(parts[1])
                    old = lane.max_concurrency
                    lane.max_concurrency = n
                    print_info(f"  {parts[1]}: max {old} -> {n}")
                    with lane._condition:
                        lane._pump()
            elif cmd == "/generation":
                for name, st in cq.stats().items():
                    print_info(f"  {name}: generation={st['generation']}")
            elif cmd == "/reset":
                res = cq.reset_all()
                print_info("  Generation incremented:")
                for name, g in res.items():
                    print_info(f"    {name}: -> {g}")
            elif cmd == "/delivery":
                st = delivery.stats()
                print_info(f"  pending: {st['pending']}  failed: {st['failed']}")
            elif cmd == "/profiles":
                for p in pm.list_profiles():
                    print_info(f"  {p['name']} ({p['provider']}) {p['status']} {p['failure_reason'] or ''} last_good={p['last_good']}")
            else:
                print_warn(f"Unknown: {cmd}. /help for commands.")
            continue

        # CLI 用户输入 -> 入队 main 车道
        inbound = InboundMessage(text=user_input, sender_id="cli-user", channel="cli",
                                 account_id="cli-local", peer_id="cli-user")
        print_lane(LANE_MAIN, "processing...")
        future = cq.enqueue(LANE_MAIN, lambda ib=inbound: run_agent_turn(
            ib, agent_mgr, bindings, soul_text, bootstrap, memory_store,
            resilient, delivery, tool_handlers))
        try:
            future.result(timeout=120)
        except concurrent.futures.TimeoutError:
            print_warn("Request timed out (still running in background).")
        except Exception as exc:
            print_error(f"Error: {exc}")

    # 退出清理
    print(f"\n{DIM}Goodbye.{RESET}")
    heartbeat.stop()
    tg_stop.set()
    cron_stop.set()
    delivery_runner.stop()
    cq.wait_for_all(timeout=3.0)
    mgr_ch.close_all()


def _auto_recall_placeholder(memory_store: MemoryStore) -> str:
    """给 /prompt 命令用的 auto-recall (用通用 query)."""
    results = memory_store.hybrid_search("recent context and preferences", top_k=3)
    return "\n".join(f"- [{r['path']}] {r['snippet']}" for r in results) if results else ""


def main() -> None:
    if not os.getenv("ANTHROPIC_API_KEY"):
        print(f"{YELLOW}Error: ANTHROPIC_API_KEY not set.{RESET}")
        print(f"{DIM}Copy .env.example to .env and fill in your key.{RESET}")
        sys.exit(1)
    if not WORKSPACE_DIR.is_dir():
        WORKSPACE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    agent_loop()


if __name__ == "__main__":
    main()
