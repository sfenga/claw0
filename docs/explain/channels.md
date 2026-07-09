# 第 04 节详解：Channels（通道）

> 「同一大脑，多个嘴巴」—— 每个平台都不同，但它们都产生相同的 `InboundMessage`。

本文是对 claw0 `sessions/zh/s04_channels.py` 的逐层拆解。读完你应能回答：通道这层抽象为什么存在、它的边界协议是什么、Telegram/飞书两个真实平台实现里有哪些工程细节、主循环怎么把多通道并发跑起来。

---

## 1. 这层抽象要解决什么问题

一个 agent 大脑（agent loop）本身是平台无关的：它就是个 `while True` + `stop_reason` + 工具循环。但真实世界里，用户的消息来自 Telegram、飞书、Slack、命令行……每个平台的 API、消息格式、鉴权方式、推送/拉取模型都不同。

如果让 agent 循环直接处理这些平台差异，会出现两个灾难：

1. **agent 逻辑被平台细节污染**：满屏 `if channel == "telegram"` 分支，每加一个平台都要改 agent 核心。
2. **会话/并发/投递的复杂性和 agent 逻辑纠缠**：消息碎片合并、offset 持久化、并发队列这些和「怎么回答用户」是两件事，必须分开。

s04 的解法是定义一条**边界协议**：

```
   平台原始负载  ──→  [Channel 边界]  ──→  InboundMessage  ──→  agent 循环
                                                       ↑
              agent 回复  ←──  Channel.send(to, text)  ┘
```

所有平台差异在进入 agent 循环**之前**就被抹平成 `InboundMessage`；agent 回复时也只调统一的 `send()`，由对应通道翻译回平台 API。**一旦这条协议定下来，agent 循环就再也不需要知道「消息从哪来、往哪回」。**

这也是为什么后面 s05（网关路由）、s07（心跳）、s10（并发车道）都能在这条协议之上叠加，而不动 agent 循环本身。

---

## 2. 架构总览

```
    Telegram ----.                          .---- sendMessage API
    Feishu -------+-- InboundMessage ---+---- im/v1/messages
    CLI (stdin) --'    Agent Loop        '---- print(stdout)
                       (same brain)

    Telegram 内部细节:
    getUpdates (long-poll, 30s)
        |
    offset 持久化 (磁盘)
        |
    media_group_id? --yes--> 缓冲 500ms --> 合并 caption
        |no
    文本缓冲 (1s 静默) --> flush
        |
    InboundMessage --> allowed_chats 过滤 --> agent 回合
```

代码模块构成（自上而下）：

| 模块 | 职责 | 源码位置 |
|---|---|---|
| `InboundMessage` / `ChannelAccount` | 统一消息格式 + bot 配置 | `s04_channels.py:79` / `:92` |
| `Channel` ABC | 两方法接口契约 | `:110` |
| `CLIChannel` | 最小参考实现 | `:126` |
| `TelegramChannel` | 长轮询 + offset + 三层缓冲 | `:166` |
| `FeishuChannel` | webhook + token + @提及 + 富文本解析 | `:356` |
| 工具 `memory_*` | agent 可调用的工具（沿用 s02）| `:499` |
| `ChannelManager` | 通道注册中心 | `:551` |
| `telegram_poll_loop` | 后台轮询线程 | `:574` |
| `run_agent_turn` | 通道无关的 agent 回合 | `:612` |
| `agent_loop` | 主循环 + 多通道并发编排 | `:674` |

---

## 3. 核心数据结构

### 3.1 `InboundMessage` —— 统一入站格式

```python
@dataclass
class InboundMessage:
    text: str
    sender_id: str
    channel: str = ""          # "cli" / "telegram" / "feishu"
    account_id: str = ""       # 收到消息的那个 bot
    peer_id: str = ""          # 会话范围标识
    is_group: bool = False
    media: list = field(default_factory=list)
    raw: dict = field(default_factory=dict)   # 原始平台负载，调试用
```

**最关键字段是 `peer_id`**：它把「会话范围」编码进一个字符串，是后续会话隔离（`build_session_key`）的基石。不同场景下 `peer_id` 取值不同：

| 场景 | peer_id 格式 |
|---|---|
| Telegram 私聊 | `user_id` |
| Telegram 群组 | `chat_id` |
| Telegram 话题（forum topic）| `chat_id:topic:thread_id` |
| 飞书单聊 | `user_id` |
| 飞书群组 | `chat_id` |

注意 Telegram 话题用 `:topic:` 这种**约定符号**分隔的复合键。这是一种「自描述的复合键」：发送时 `TelegramChannel.send` 会反向解析出 `chat_id` 和 `message_thread_id`（见 `:323`）。好处是一个字符串就能携带「发给哪个群、哪个话题」的全部信息，不用额外的结构体。

`raw` 字段保留原始平台负载，方便调试和未来扩展（比如要做更精细的回复渲染），但 agent 循环**不读它**。

### 3.2 `ChannelAccount` —— 单个 bot 的配置

```python
@dataclass
class ChannelAccount:
    channel: str
    account_id: str
    token: str = ""
    config: dict = field(default_factory=dict)
```

它的存在是为了支持「**同一通道类型跑多个 bot**」——比如两个 Telegram bot、一个国内飞书一个国际 Lark。每个 bot 是一个 `ChannelAccount`，`config` 装该平台专属配置（飞书的 `app_id/app_secret`、Telegram 的 `allowed_chats` 等）。

### 3.3 会话键

```python
def build_session_key(channel, account_id, peer_id) -> str:
    return f"agent:main:direct:{channel}:{peer_id}"
```

把 `(channel, peer_id)` 拼成会话键。不同通道、不同 peer 的对话存进 `conversations` 字典的不同 key，互不串扰。（注意 `account_id` 没进 key——教学版简化了；生产版会把它也纳入，以隔离同通道不同 bot 的会话。）

---

## 4. `Channel` 抽象基类 —— 两方法契约

```python
class Channel(ABC):
    name: str = "unknown"

    @abstractmethod
    def receive(self) -> InboundMessage | None: ...

    @abstractmethod
    def send(self, to: str, text: str, **kwargs: Any) -> bool: ...

    def close(self) -> None:
        pass
```

**接口契约只有两个方法**：

- `receive()`：从平台拿一条消息，归一化成 `InboundMessage` 返回；没有则返回 `None`。
- `send(to, text)`：把回复文本发到 `to`（通常是 `peer_id`）指定的会话；返回是否成功。

这就是「添加新平台 = 实现这两个方法，agent 循环零改动」的全部秘密。`close()` 是可选的生命周期钩子（关闭 http client 等）。

---

## 5. 三个通道实现

### 5.1 `CLIChannel` —— 参考实现（`:126`）

最简单的通道，是理解其他两个复杂实现的参照系：

```python
class CLIChannel(Channel):
    name = "cli"
    def __init__(self): self.account_id = "cli-local"

    def receive(self) -> InboundMessage | None:
        text = input("You > ").strip()
        if not text: return None
        return InboundMessage(text=text, sender_id="cli-user",
                              channel="cli", account_id="cli-local",
                              peer_id="cli-user")

    def send(self, to, text, **kwargs) -> bool:
        print_assistant(text)
        return True
```

`receive()` 包 `input()`，`send()` 包 `print()`。它证明了一件事：**任何能「读一行 / 写一行」的东西都能成为通道**。stdin/stdout 如此，Telegram API 亦如此——只是中间多了一层 HTTP 和格式转换。

### 5.2 `TelegramChannel` —— 长轮询 + 工程细节（`:166`）

本节代码量最大、工程最扎实的部分，体现了真实接入 IM 平台的几个典型难点。

#### ① 长轮询拿消息：`poll()`（`:201`）

```python
result = self._api("getUpdates", offset=self._offset, timeout=30,
                   allowed_updates=["message"])
```

调用 Telegram Bot API 的 `getUpdates`，`timeout=30` 让请求挂起最多 30 秒等新消息（长轮询，省请求又近实时）。`offset` 是 Telegram 的「已读游标」：

```python
if uid >= self._offset:
    self._offset = uid + 1
    save_offset(self._offset_path, self._offset)   # 立刻落盘
```

每拿到一个 `update_id`，就把 offset 推进到 `uid + 1` 并**立刻持久化到磁盘**（`workspace/.state/telegram/offset-<account>.txt`）。这样进程崩溃重启不会重复处理已处理过的消息——offset 是「at-least-once 语义」的关键。

#### ② 三道缓冲层：把碎片化消息组装成语义完整的一次发言

Telegram 有个恼人特性：用户「一次」发送的内容，到了 API 可能被拆成多条独立 message（多张图、长文本粘贴）。直接每条都触发一次 agent 回合会非常糟。这里用三层缓冲在通道边界重新组装：

**a. `_seen` 去重集合（`:181`）**
```python
if uid in self._seen: continue
self._seen.add(uid)
if len(self._seen) > 5000: self._seen.clear()
```
幂等保护：offset 失误或重复投递时，同一条 update 不处理两次。超过 5000 条清空避免无限增长（一个粗略的 LRU 替代）。

**b. 媒体组缓冲 `_media_groups`（500ms 窗口，`:240`）**
用户一次发多张图，Telegram 拆成多条 message，但带同一个 `media_group_id`。这里按 mgid 收集：

```python
def _buf_media(self, msg, update):
    mgid = msg["media_group_id"]
    if mgid not in self._media_groups:
        self._media_groups[mgid] = {"ts": time.monotonic(), "entries": []}
    self._media_groups[mgid]["entries"].append((msg, update))
```

500ms 静默后 `_flush_media` 把所有 caption 拼接成 `text`、所有 `file_id` 收进 `media`，产出**一条** InboundMessage。

**c. 文本缓冲 `_text_buf`（1s 窗口，`:272`）**
用户粘贴一大段文字，Telegram 拆成多条短消息。按 `(peer_id, sender_id)` 作 key，1 秒内连续到达的文本用 `\n` 拼接，静默 1s 后 flush：

```python
def _buf_text(self, inbound):
    key = (inbound.peer_id, inbound.sender_id)
    if key in self._text_buf:
        self._text_buf[key]["text"] += "\n" + inbound.text
        self._text_buf[key]["ts"] = now
    else:
        self._text_buf[key] = {"text": inbound.text, "msg": inbound, "ts": now}
```

> **设计哲学**：把平台的传输层碎片化，在通道边界重新组装成语义完整的「一次发言」，再交给 agent。agent 看到的永远是一整条用户意图，而不是被网络拆碎的片段。

#### ③ 消息解析 `_parse`（`:293`）

从原始 update 提取 `chat_id` / `user_id` / `text`/`caption`，并按 chat 类型决定 `peer_id`：

```python
if chat_type == "private":
    peer_id = user_id
elif is_group and is_forum and thread_id is not None:
    peer_id = f"{chat_id}:topic:{thread_id}"
else:
    peer_id = chat_id
```

私聊→user_id（一对一对话天然由用户标识）；群→chat_id；论坛话题→复合键（同一群里不同话题是不同会话）。

#### ④ 白名单过滤

```python
if self.allowed_chats and inbound.peer_id not in self.allowed_chats:
    continue
```

不在 `allowed_chats` 白名单里的群消息直接丢弃。这是「谁能跟这个 bot 说话」的访问控制——对应 coding agent 里「哪些操作要人审批」的权限模型雏形。

#### ⑤ 发送 `send`（`:323`）

```python
chat_id, thread_id = to, None
if ":topic:" in to:
    parts = to.split(":topic:")
    chat_id, thread_id = parts[0], int(parts[1])
for chunk in self._chunk(text):
    self._api("sendMessage", chat_id=chat_id, text=chunk,
              message_thread_id=thread_id)
```

反向解析 `:topic:` 复合 peer；按 4096 字符上限 `_chunk` 切片（优先在换行处切，保持可读），逐段发送。`_chunk` 的切分逻辑：先找 4096 内最后一个换行，没有就硬切。

### 5.3 `FeishuChannel` —— webhook + 鉴权模型（`:356`）

飞书是**推送模型**（webhook 回调），和 Telegram 的拉取模型相反，设计思路也不同。

#### ① token 管理 `_refresh_token`（`:374`）

飞书 API 用 `tenant_access_token` 鉴权，有效期约 2 小时。这里做带提前量的内存缓存：

```python
if self._tenant_token and time.time() < self._token_expires_at:
    return self._tenant_token
# 否则刷新，并提前 5 分钟续期：
self._token_expires_at = time.time() + data.get("expire", 7200) - 300
```

`expire - 300`（提前 5 分钟）避免请求途中 token 刚好过期的竞态。这是 s09「认证轮换」的雏形。

#### ② 事件解析 `parse_event`（`:438`）

处理 webhook 回调的几个细节：

- **`challenge` 握手**：飞书首次配置 webhook 时发来的验证请求，直接返回不打扰 agent。
- **token 校验**：用 `encrypt_key` 做简单鉴权，校验失败拒绝。
- **@提及检测 `_bot_mentioned`（`:393`）**：群里只有 @机器人 才响应，否则忽略——IM bot 的标准行为，避免 bot 对群里每句话都插嘴。

```python
if is_group and self._bot_open_id and not self._bot_mentioned(event):
    return None
```

- **多消息类型解析 `_parse_content`（`:404`）**：飞书的富文本 `post` 类型是嵌套的段落/节点结构（locale→content→para→node），这里递归抽取出纯文本和链接；`image` 类型则把 `image_key` 收进 `media`。

#### ③ 注意：`receive()` 直接返回 `None`（`:469`）

```python
def receive(self) -> InboundMessage | None:
    return None
```

因为飞书不主动拉取，靠外部 webhook 把 payload 喂给 `parse_event`。本节里飞书**还没接入主循环**（主循环只接了 CLI + Telegram）——它作为「通道抽象的第二个实现样本」存在，完整 webhook 服务器要等后续 gateway 章节把推送模型接进来。这也说明 ABC 契约足够灵活：拉取通道实现 `receive`，推送通道实现 `parse_event` + 由外部驱动。

---

## 6. `ChannelManager` —— 注册中心（`:551`）

```python
class ChannelManager:
    def __init__(self):
        self.channels: dict[str, Channel] = {}
        self.accounts: list[ChannelAccount] = []

    def register(self, channel: Channel) -> None: ...
    def get(self, name: str) -> Channel | None: ...
    def close_all(self) -> None: ...
```

就是个 `name -> Channel` 的字典 + accounts 列表。它让主循环通过 `mgr.get(inbound.channel)` 拿到来源通道来回复，实现「从哪来回哪去」：

```python
ch = mgr.get(inbound.channel)
if ch: ch.send(inbound.peer_id, text)
```

agent 回合里**不需要 if 分支判断平台**——只要从 manager 里按名字取出对应通道，调统一的 `send`。这正是抽象成功的标志。

---

## 7. 主循环与并发模型（`:674`）

多通道并存时的并发处理是本节一个值得重点看的设计。

```
Telegram 轮询线程 (daemon)  ──→  共享 msg_queue  ──┐
        (telegram_poll_loop)        (加锁)          │
                                                    ├──→ 主循环逐条 run_agent_turn
        sys.stdin (CLI)  ──────────────────────────┘
```

- **Telegram 在后台 daemon 线程里持续 `poll()`**，把消息塞进带锁的共享 `msg_queue`：

```python
def telegram_poll_loop(tg, queue, lock, stop):
    while not stop.is_set():
        msgs = tg.poll()
        if msgs:
            with lock:
                queue.extend(msgs)
```

- **主循环每轮先排空 Telegram 队列**，逐条 `run_agent_turn`：

```python
with q_lock:
    tg_msgs = msg_queue[:]
    msg_queue.clear()
for m in tg_msgs:
    run_agent_turn(m, conversations, mgr)
```

- **关键技巧：CLI 非阻塞读（`:737`）**

```python
if tg_channel:
    import select
    if not select.select([sys.stdin], [], [], 0.5)[0]:
        continue          # 0.5s 内没输入，回头处理 Telegram
    user_input = sys.stdin.readline().strip()
```

当 Telegram 活跃时，CLI 用 `select` 做非阻塞读。否则 `input()` 会阻塞主线程，Telegram 队列永远没人排空。**0.5 秒超时**是个精巧的平衡点：既能让主循环及时处理 Telegram 消息，又不会空转烧 CPU。

> 这是典型的「一个生产者（轮询线程）+ 一个消费者（主循环）+ 共享有界队列」模型。教学版用线程 + 全局 list + Lock；s10 会把它升级成命名车道系统。daemon=True 让线程随主进程退出，`stop_event` + `join(timeout=3.0)` 做优雅收尾。

退出流程也很干净：

```python
stop_event.set()
if tg_thread and tg_thread.is_alive():
    tg_thread.join(timeout=3.0)
mgr.close_all()
```

---

## 8. `run_agent_turn` —— 通道无关的 agent 回合（`:612`）

这是承上启下的函数，**证明通道抽象确实成功**：

```python
def run_agent_turn(inbound, conversations, mgr):
    sk = build_session_key(inbound.channel, inbound.account_id, inbound.peer_id)
    conversations.setdefault(sk, [])
    messages = conversations[sk]
    messages.append({"role": "user", "content": inbound.text})

    if inbound.channel == "telegram":
        tg = mgr.get("telegram")
        if isinstance(tg, TelegramChannel):
            tg.send_typing(inbound.peer_id.split(":topic:")[0])

    while True:
        response = client.messages.create(model=MODEL_ID, ..., messages=messages)
        messages.append({"role": "assistant", "content": response.content})
        if response.stop_reason == "end_turn":
            text = "".join(b.text for b in response.content if hasattr(b, "text"))
            ch = mgr.get(inbound.channel)
            if ch: ch.send(inbound.peer_id, text)
            break
        elif response.stop_reason == "tool_use":
            # 分发工具，追加结果，继续循环（沿用 s02）
            ...
```

要点：

1. **整个函数几乎没有任何 `if channel == ...` 的平台分支**去碰平台 API——唯一例外是 Telegram 的 typing 指示器，属体验优化（让用户看到「正在输入」），不影响正确性。
2. 它跑的是 s01/s02 已建立的**标准工具循环**：调 API → 看 `stop_reason` → `end_turn` 就发回复退出，`tool_use` 就执行工具把结果塞回去继续。
3. 回复时 `mgr.get(inbound.channel)` 取来源通道、`send(inbound.peer_id, text)` 发回——**从哪来回哪去**，且 peer_id 自动路由到正确的群/话题。
4. 会话隔离靠 `build_session_key`，不同通道/peer 的对话互不串扰。

> API 出错时的回退逻辑（`:634`）也值得注意：pop 掉最后非 user 消息和那条 user 消息，相当于「这一轮作废，下次重试」——s09 会把它发展成三层重试洋葱。

---

## 9. 工程亮点小结

把 s04 里真正值得记到脑子里的工程模式列一下：

1. **统一入站协议（`InboundMessage`）+ 两方法出站契约（`Channel.send`）**：抽象的边界。一旦定下，平台增减不污染 agent 核心。
2. **offset 立即落盘**：拉取模型的 at-least-once 语义，崩溃不丢不重。
3. **三层缓冲（去重 / 媒体组 500ms / 文本 1s）**：在通道边界把传输碎片重组为语义完整的「一次发言」。
4. **复合 peer_id（`:topic:` 约定）**：一个字符串自描述「群+话题」，收发双向解析。
5. **白名单 `allowed_chats`**：通道级的访问控制。
6. **token 带提前量缓存**：推送模型鉴权，`expire - 300` 规避过期竞态。
7. **生产者-消费者 + 非阻塞 stdin（`select` 0.5s）**：多通道并发的最小可用模型，不阻塞、不空转。
8. **daemon 线程 + stop_event + join**：优雅启停。

这些模式不是 Telegram/飞书专属的——它们是「**任何把 agent 接到外部世界**」都会遇到的：拉取 vs 推送、去重、碎片重组、鉴权、并发、崩溃恢复。

---

## 10. 教学版 vs 生产版

`.md` 末尾给的对照表很有价值，说明这节是「骨架」，生产版是在每个点上加固：

| 方面 | claw0 教学版 | OpenClaw 生产版 |
|---|---|---|
| Channel 接口 | `receive()` + `send()` | 相同接口 + 生命周期钩子 |
| 平台数量 | CLI / Telegram / 飞书 | 10+（含 Discord / Slack 等）|
| 并发模型 | 每通道一线程 + 共享队列 | 同线程模型 + 异步网关 |
| 消息格式 | `InboundMessage` dataclass | 相同的统一消息类型 |
| Offset 存储 | 纯文本文件 | 带版本号 JSON + 原子写入 |
| 会话键 | 不含 `account_id` | 含 account_id，隔离同通道不同 bot |
| 重试 | 单层 pop 重试 | 三层重试洋葱（s09）|
| 并发 | 单一 Lock + 全局 list | 命名车道 + FIFO 队列（s10）|

---

## 11. 在 10 节大图里的位置

- **s03** 让 agent 有了会话持久化（JSONL）；
- **s04** 在会话**前面**加了一层**通道抽象**——让同一个 agent 大脑能从多个 IM 平台收发消息，且每个平台的消息都先归一化成 `InboundMessage`；
- **s05** 接着把这层「通道消息」接到**网关路由**上，做 5 级绑定（哪个 channel+peer 路由给哪个 agent）。

所以 s04 的核心贡献是定义了**系统与外部世界的边界协议**：`InboundMessage` 入、`Channel.send` 出。后面加平台、加路由、加并发车道，都是在这个协议之上叠加，不再改动 agent 循环本身。

---

## 附：运行方法

```sh
cd claw0
pip install -r requirements.txt
cp .env.example .env   # 填 ANTHROPIC_API_KEY + MODEL_ID

# 仅 CLI（除 API key 外不需其他环境变量）
python sessions/zh/s04_channels.py

# 启用 Telegram —— 在 .env 加:
#   TELEGRAM_BOT_TOKEN=123456:ABC-DEF...
#   TELEGRAM_ALLOWED_CHATS=12345,67890   (可选白名单)

# 启用飞书 —— 在 .env 加:
#   FEISHU_APP_ID=cli_xxxxx
#   FEISHU_APP_SECRET=xxxxx

# REPL 命令:
#   /channels    列出已注册通道
#   /accounts    显示 bot 账号
#   /help        帮助
#   quit/exit    退出
```
