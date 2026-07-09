# 第 05 节详解：网关与路由（Gateway & Routing）

> 每条入站消息都能找到归宿——一张五层绑定表，把 (channel, peer) 映射到 agent_id。

本文是对 `sessions/zh/s05_gateway_routing.py`（625 行）和 `s05_gateway_routing.md` 的逐层深读。如果说 s04（Channels）解决的是「消息怎么从不同平台进来、又怎么回出去」，那么 **s05 解决的是「进来的这条消息，到底该交给哪个 agent、算作哪段对话」**。这是从「单 agent」到「多 agent」的跨越。

---

## 0. 一句话定位

s05 = **路由层**。它把 s04 那个「固定写死 `agent:main:direct:...`」的会话键，升级成一个可配置的决策过程：

```
入站消息 (channel, account_id, peer_id, text)
        │
        ▼
   路由解析  ──►  agent_id（交给哪个 agent）
        │
        ▼
   会话键构建  ──►  session_key（算哪段对话）
        │
        ▼
   AgentManager  ──►  取出该 agent 的配置 / 会话历史
        │
        ▼
   run_agent  ──►  跑工具循环，产出回复
```

在 s04 里，会话键是 `build_session_key(channel, account_id, peer_id)` 硬拼出来的（永远是 `main` 这个 agent）。到了 s05，「用哪个 agent」本身成了一个问题：不同的用户、不同的群、不同的平台，可能要接到不同的 agent（不同的性格、不同的模型、不同的工作区）。路由系统就是回答这个问题的机构。

---

## 1. 架构总览

```
    Inbound Message (channel, account_id, peer_id, text)
           |
    +------v------+     +----------+
    |   Gateway    | <-- | WS/REPL  |  JSON-RPC 2.0
    +------+------+     +----------+
           |
    +------v------+
    | BindingTable |  5-tier resolution:
    +------+------+    T1: peer_id     (最具体)
           |           T2: guild_id
           |           T3: account_id
           |           T4: channel
           |           T5: default     (最泛化)
           |
     (agent_id, binding)
           |
    +------v---------+
    | build_session_key() |  dm_scope 控制隔离粒度
    +------+---------+
           |
    +------v------+
    | AgentManager |  每个 agent 的配置 / 性格 / 会话
    +------+------+
           |
        LLM API
```

四个核心组件，对应代码四个区块：

| 组件 | 代码位置 | 职责 |
|------|---------|------|
| `Binding` + `BindingTable` | `:87-138` | 五层路由匹配，回答「交给哪个 agent」 |
| `build_session_key` | `:149-161` | 会话隔离，回答「算哪段对话」 |
| `AgentConfig` + `AgentManager` | `:167-214` | 多 agent 注册中心，存配置和会话 |
| `GatewayServer` | `:358-465` | WebSocket + JSON-RPC 2.0，把路由能力对外暴露 |

外加两个支撑件：共享 asyncio 事件循环（`:258-275`，让 agent 跑在后台线程里异步并发）和 `run_agent` 运行器（`:305-352`，带信号量限流的工具循环）。

---

## 2. 路由的五层绑定表

这是本节的灵魂。先看数据结构，再看匹配规则。

### 2.1 `Binding`：一条路由规则

```python
@dataclass
class Binding:
    agent_id: str           # 匹配后交给谁
    tier: int               # 1-5, 越小越具体（优先级越高）
    match_key: str          # "peer_id" | "guild_id" | "account_id" | "channel" | "default"
    match_value: str        # 例如 "telegram:12345", "discord", "*"
    priority: int = 0      # 同层内的二级排序，越大越优先
```

一条绑定就是一句陈述：「**当入站消息在 `match_key` 这个维度上等于 `match_value` 时，把它交给 `agent_id` 这个 agent**」。`tier` 标明它属于哪一层（决定先后顺序），`priority` 是同层内的微调。

五个层级的含义，从最具体到最泛化：

| Tier | match_key | 含义 | 举例 |
|------|-----------|------|------|
| 1 | `peer_id` | 精确到某个人/某个会话 | discord 用户 admin-001 永远找 Sage |
| 2 | `guild_id` | 精确到某个服务器（Discord guild） | 某个 Discord 服务器整体交给某 agent |
| 3 | `account_id` | 精确到某个 bot 账号 | tg-primary 这个 bot 的所有消息交给某 agent |
| 4 | `channel` | 整个通道 | 所有 Telegram 消息交给 Sage |
| 5 | `default` | 兜底 | 谁都没匹配上就用 Luna |

层级越靠前，匹配条件越「窄」（命中范围越小），优先级越高。这是一个**从具体到泛化的漏斗**：先看能不能精确到某个人，不行就退到服务器级，再不行退到账号级……最后用默认兜底，保证任何消息都不会「找不到归宿」。

### 2.2 `BindingTable`：排序 + 线性扫描

```python
class BindingTable:
    def __init__(self) -> None:
        self._bindings: list[Binding] = []

    def add(self, binding: Binding) -> None:
        self._bindings.append(binding)
        self._bindings.sort(key=lambda b: (b.tier, -b.priority))
```

`add` 之后立刻重排：按 `(tier 升序, priority 降序)` 排。这样 `_bindings` 始终是「tier 1 优先级最高的在最前，tier 5 在最后」的有序列表。**排序在写入时做一次，读取时不再排序**——这是个刻意的设计：绑定改动不频繁，而路由解析在每条消息上都跑，把成本挪到写入端。

### 2.3 `resolve`：首次匹配即返回

```python
def resolve(self, channel="", account_id="",
            guild_id="", peer_id="") -> tuple[str | None, Binding | None]:
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
```

由于 `_bindings` 已按 tier 排好，这里只需**线性遍历，第一个匹配的就 `return`**——天然实现了「最具体的先赢」。匹配规则逐层不同：

- **T1 peer_id**：有两种写法。`match_value` 里**含冒号**（`channel:peer_id` 复合形式）则要求 channel 和 peer_id 都对上（`telegram:12345` 精确到 Telegram 里的 12345）；**不含冒号**则只比 peer_id（跨平台，只要这个 peer_id 就匹配）。这个复合形式是 s04 那个 `peer_id` 概念的延伸——peer_id 在不同平台命名空间不同，带 channel 前缀才能消歧。
- **T2~T4**：单值相等，简单。
- **T5 default**：无条件命中（`match_value="*"` 约定俗成表示「任意」）。

返回两个东西：命中的 `agent_id` 和命中的 `Binding` 对象本身（后者用于显示「是哪条规则匹配上的」，方便调试）。

### 2.4 演示绑定的匹配走查

`setup_demo`（`:471`）装了三条绑定：

```python
bt.add(Binding(agent_id="luna", tier=5, match_key="default", match_value="*"))
bt.add(Binding(agent_id="sage", tier=4, match_key="channel", match_value="telegram"))
bt.add(Binding(agent_id="sage", tier=1, match_key="peer_id",
               match_value="discord:admin-001", priority=10))
```

排序后顺序是：`T1(discord:admin-001)` → `T4(telegram)` → `T5(default)`。代入几种输入：

| 输入 | 命中 Tier | 交给 |
|------|----------|------|
| `channel=cli, peer=user1` | 没命中 T1/T4，落到 T5 | Luna |
| `channel=telegram, peer=user2` | T4 命中（telegram） | Sage |
| `channel=discord, peer=admin-001` | T1 命中（discord:admin-001） | Sage |
| `channel=discord, peer=user3` | 没命中 T1（不是 admin-001），T4 不命中（不是 telegram），落 T5 | Luna |

注意第四行：`peer=user3` 在 discord 上，但 T1 只认 `admin-001`，T4 只认 `telegram`，于是掉到默认 Luna。这就是漏斗的威力——**每层只管自己那窄窄一段，剩下的层层下放**。

---

## 3. 会话键构建：`dm_scope` 控制隔离粒度

路由给出 `agent_id` 后，还要决定「这条消息归入哪段对话历史」。这就是会话键。

```python
def build_session_key(agent_id, channel="", account_id="",
                      peer_id="", dm_scope="per-peer") -> str:
    aid = normalize_agent_id(agent_id)
    ch = (channel or "unknown").strip().lower()
    acc = (account_id or "default").strip().lower()
    pid = (peer_id or "").strip().lower()
    if dm_scope == "per-account-channel-peer" and pid:
        return f"agent:{aid}:{ch}:{acc}:direct:{pid}"
    if dm_scope == "per-channel-peer" and pid:
        return f"agent:{aid}:{ch}:direct:{pid}"
    if dm_scope == "per-peer" and pid:
        return f"agent:{aid}:direct:{pid}"
    return f"agent:{aid}:main"
```

`dm_scope` 是个**每 agent 可配**的策略，决定「同一个 agent 内部，不同来源的消息要不要共享上下文」：

| dm_scope | Key 格式 | 隔离效果 |
|----------|----------|----------|
| `main` | `agent:{id}:main` | **所有人共享一个会话**——任何人的消息都进同一段历史 |
| `per-peer` | `agent:{id}:direct:{peer}` | 每个用户隔离（同一用户跨平台共享） |
| `per-channel-peer` | `agent:{id}:{ch}:direct:{peer}` | 每个平台分别隔离（同一个人在 Telegram 和 Discord 是两段历史） |
| `per-account-channel-peer` | `agent:{id}:{ch}:{acc}:direct:{peer}` | 最大隔离，连 bot 账号都区分 |

为什么要把隔离粒度做成可配？因为不同 agent 有不同诉求：

- 一个「客服 FAQ」agent 可能想要 `main`：所有用户共享同一份上下文，反正答的是同一套知识。
- 一个「私人助理」agent 必须要 `per-peer`：你的对话不能串到别人那。
- 一个跨平台角色扮演 bot 可能要 `per-channel-peer`：同一个人在 Discord 上是「勇者」、在 Telegram 上是「村长」，两段故事互不干扰。

注意 `and pid` 这个守卫：私聊场景才有 `peer_id`；如果消息没有 peer_id（比如某些 webhook 场景），无论 dm_scope 是什么都落到 `agent:{id}:main`——这是「没明确归属就进公共会话」的合理兜底。

对比 s04：那里 `build_session_key` 固定产 `agent:main:direct:{channel}:{peer_id}`，只有一种隔离粒度，且 agent 永远是 `main`。s05 把「用哪个 agent」和「隔离多细」都参数化了。

---

## 4. AgentManager：多 agent 注册中心

```python
@dataclass
class AgentConfig:
    id: str
    name: str
    personality: str = ""
    model: str = ""              # 空 = 用全局 MODEL_ID
    dm_scope: str = "per-peer"

    @property
    def effective_model(self) -> str:
        return self.model or MODEL_ID

    def system_prompt(self) -> str:
        parts = [f"You are {self.name}."]
        if self.personality:
            parts.append(f"Your personality: {self.personality}")
        parts.append("Answer questions helpfully and stay in character.")
        return " ".join(parts)
```

每个 agent 是一张配置卡：名字、性格、模型、隔离策略。`system_prompt` 从配置**生成**——这是 s06「Intelligence」要展开的「提示词即磁盘文件」思想的雏形：在这里换 personality 字符串就换人格，不动代码。`effective_model` 的 fallback 让 agent 可以共用全局模型，也可以单独指定（比如让 Sage 用更强的模型）。

```python
class AgentManager:
    def __init__(self, agents_base=None):
        self._agents: dict[str, AgentConfig] = {}
        self._agents_base = agents_base or AGENTS_DIR
        self._sessions: dict[str, list[dict]] = {}

    def register(self, config: AgentConfig) -> None:
        aid = normalize_agent_id(config.id)
        config.id = aid
        self._agents[aid] = config
        agent_dir = self._agents_base / aid
        (agent_dir / "sessions").mkdir(parents=True, exist_ok=True)
        (WORKSPACE_DIR / f"workspace-{aid}").mkdir(parents=True, exist_ok=True)
```

`AgentManager` 持有两张表：`_agents`（agent 配置）和 `_sessions`（会话历史，按 session_key 存）。`register` 时除了登记配置，还会**给每个 agent 建独立的工作区目录**（`workspace/.agents/{id}/sessions/` 和 `workspace/workspace-{id}/`）——这为后面 s06 的「每个 agent 独立工作区/记忆」埋了桩。s05 本身会话还在内存里（`_sessions` dict），持久化是 s03 那一套的事。

`get_session` 是「按需创建」的懒初始化：第一次访问某 session_key 时建空列表，之后 caller 直接 `messages.append(...)` 往里加。这和 s04 的 `conversations[sk]` 模式一致，只是容器从全局 dict 挪进了 manager。

---

## 5. 路由解析函数 `resolve_route`

```python
def resolve_route(bindings, mgr, channel, peer_id,
                  account_id="", guild_id="") -> tuple[str, str]:
    agent_id, matched = bindings.resolve(
        channel=channel, account_id=account_id,
        guild_id=guild_id, peer_id=peer_id,
    )
    if not agent_id:
        agent_id = DEFAULT_AGENT_ID       # "main"
        print(f"  [route] No binding matched, default: {agent_id}")
    elif matched:
        print(f"  [route] Matched: {matched.display()}")
    agent = mgr.get_agent(agent_id)
    dm_scope = agent.dm_scope if agent else "per-peer"
    sk = build_session_key(agent_id, channel=channel, account_id=account_id,
                           peer_id=peer_id, dm_scope=dm_scope)
    return agent_id, sk
```

它是「绑定表匹配」和「会话键构建」之间的胶水。两步：

1. 调 `bindings.resolve` 得到 `agent_id`；如果**一条都没匹配上**（连 default 都没配），用硬编码的 `DEFAULT_AGENT_ID="main"` 兜底——所以即便绑定表空着，消息也能找到归宿（虽然 `main` 这个 agent 大概率没注册，后面 `run_agent` 会返回 not found）。
2. 取出该 agent 的 `dm_scope`（agent 不存在就用默认 `per-peer`），拼出 session_key。

返回 `(agent_id, session_key)` 这一对，正好是 `run_agent` 需要的两个参数。这一步把「平台维度的入站坐标」翻译成了「agent 维度的处理坐标」。

---

## 6. Agent 运行器：并发限流的工具循环

s05 把 s04 的 `run_agent_turn` 重写成异步版 `run_agent`，并引入了并发控制。

### 6.1 共享事件循环

```python
_event_loop = None
_loop_thread = None

def get_event_loop():
    global _event_loop, _loop_thread
    if _event_loop is not None and _event_loop.is_running():
        return _event_loop
    _event_loop = asyncio.new_event_loop()
    def _run():
        asyncio.set_event_loop(_event_loop)
        _event_loop.run_forever()
    _loop_thread = threading.Thread(target=_run, daemon=True)
    _loop_thread.start()
    return _event_loop

def run_async(coro):
    loop = get_event_loop()
    return asyncio.run_coroutine_threadsafe(coro, loop).result()
```

为什么要把 asyncio 循环放进一个后台 daemon 线程？因为 s05 的主入口是**同步的 REPL**（`input()` 阻塞），但 agent 执行想要并发（同时处理多条消息、WebSocket 多客户端）。解法是：主线程跑同步 REPL，后台线程跑一个常驻 asyncio 循环，两者用 `run_coroutine_threadsafe` 桥接——主线程把协程丢进后台循环，阻塞等结果。这样既保留了同步 REPL 的简洁，又拿到了 asyncio 的并发能力。`run_forever()` 让循环永不停，daemon=True 让进程退出时它自动带走。

### 6.2 信号量限流

```python
_agent_semaphore = None

async def run_agent(mgr, agent_id, session_key, user_text, on_typing=None):
    global _agent_semaphore
    if _agent_semaphore is None:
        _agent_semaphore = asyncio.Semaphore(4)
    agent = mgr.get_agent(agent_id)
    if not agent:
        return f"Error: agent '{agent_id}' not found"
    messages = mgr.get_session(session_key)
    messages.append({"role": "user", "content": user_text})
    async with _agent_semaphore:               # 最多 4 个 agent 同时跑
        if on_typing:
            on_typing(agent_id, True)
        try:
            return await _agent_loop(agent.effective_model, agent.system_prompt(), messages)
        finally:
            if on_typing(agent_id, False)
```

`asyncio.Semaphore(4)` 限制**同时只有 4 个 agent 回合在跑 LLM API**。这是必要的：网关可能同时收到很多消息（多个 WebSocket 客户端、多个平台），如果无脑全发，会把 API 打到限流，反而全慢。信号量让超出 4 个的请求排队等，保护后端。

注意信号量包住的是「调 LLM」那一段，`messages.append` 在信号量外——所以「把消息塞进历史」不会因为排队而阻塞，只有真正调 API 才受 4 并发限制。`on_typing` 回调用于在回合开始/结束时通知前端「agent 正在打字」，给用户反馈。

### 6.3 `_agent_loop`：和 s04 同构的工具循环

```python
async def _agent_loop(model, system, messages):
    for _ in range(15):                       # 最多 15 圈，防死循环
        try:
            response = await asyncio.to_thread(
                client.messages.create,
                model=model, max_tokens=4096,
                system=system, tools=TOOLS, messages=messages,
            )
        except Exception as exc:
            while messages and messages[-1]["role"] != "user":
                messages.pop()
            if messages:
                messages.pop()
            return f"API Error: {exc}"
        messages.append({"role": "assistant", "content": response.content})
        if response.stop_reason == "end_turn":
            return "".join(b.text for b in response.content if hasattr(b, "text")) or "[no text]"
        if response.stop_reason == "tool_use":
            results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                results.append({"type": "tool_result", "tool_use_id": block.id,
                                "content": process_tool_call(block.name, block.input)})
            messages.append({"role": "user", "content": results})
            continue
        return "".join(...) or f"[stop={response.stop_reason}]"
    return "[max iterations reached]"
```

逻辑和 s04 的 `run_agent_turn` 几乎一样：`end_turn` 收尾返回文本，`tool_use` 执行工具把结果作为 user 消息塞回去继续。区别有三：

1. **`asyncio.to_thread` 包住同步 API**：`client.messages.create` 是同步阻塞调用，在 asyncio 里直接调会卡住整个循环。`to_thread` 把它丢到线程池，让事件循环能继续转。
2. **15 圈上限**：`for _ in range(15)` 防止模型无限调工具，s04 没有这个护栏。超过返回 `"[max iterations reached]"`。
3. **失败回滚**：和 s04 同样的「pop 掉残留 + pop 掉 user 消息」回滚，保证会话可重入。

---

## 7. GatewayServer：WebSocket + JSON-RPC 2.0

前六节都是「内部能力」，`GatewayServer` 把这些能力**对外暴露成一个网络服务**。

### 7.1 启动与连接处理

```python
async def start(self):
    import websockets
    self._server = await websockets.serve(self._handle, self._host, self._port)

async def _handle(self, ws, path=""):
    self._clients.add(ws)
    try:
        async for raw in ws:                    # 逐条读消息
            resp = await self._dispatch(raw)
            if resp:
                await ws.send(json.dumps(resp))
    except Exception:
        pass
    finally:
        self._clients.discard(ws)
```

`websockets.serve` 起一个 ws 服务，每个连接进来的客户端套接字 `ws` 都跑一次 `_handle`。`async for raw in ws` 持续读这个客户端发来的消息，`_dispatch` 处理后把响应回写。`_clients` 是个 set，记录所有在线连接——后面 typing 广播要用。`finally discard` 保证连接断开时从 set 移除，不留死引用。

### 7.2 JSON-RPC 2.0 分派

```python
async def _dispatch(self, raw):
    try:
        req = json.loads(raw)
    except json.JSONDecodeError:
        return {"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}, "id": None}
    rid, method, params = req.get("id"), req.get("method", ""), req.get("params", {})
    methods = {
        "send": self._m_send, "bindings.set": self._m_bind_set,
        "bindings.list": self._m_bind_list, "sessions.list": self._m_sessions,
        "agents.list": self._m_agents, "status": self._m_status,
    }
    handler = methods.get(method)
    if not handler:
        return {"jsonrpc": "2.0", "error": {"code": -32601, "message": f"Unknown: {method}"}, "id": rid}
    try:
        return {"jsonrpc": "2.0", "result": await handler(params), "id": rid}
    except Exception as exc:
        return {"jsonrpc": "2.0", "error": {"code": -32000, "message": str(exc)}, "id": rid}
```

标准 JSON-RPC 2.0：请求带 `method`/`params`/`id`，响应带同 `id` 的 `result` 或 `error`。`methods` 字典是个 dispatch table（和 s02 的工具 dispatch 同构思想），把方法名映射到 handler。错误码也按规范：`-32700` 解析失败、`-32601` 方法不存在、`-32000` 业务异常。所有 handler 都是 async，统一 `await` 调用并包 `try/except`，任何异常都转成结构化错误返回——**绝不让单个请求的异常炸掉服务**。

六个方法：`send`（发消息跑 agent）、`bindings.set`/`bindings.list`（管理路由）、`sessions.list`/`agents.list`（查询状态）、`status`（心跳信息）。其中 `send` 是核心：

```python
async def _m_send(self, p):
    text = p.get("text", "")
    if not text:
        raise ValueError("text is required")
    ch, pid = p.get("channel", "websocket"), p.get("peer_id", "ws-client")
    if p.get("agent_id"):                       # 显式指定 agent，跳过路由
        aid = normalize_agent_id(p["agent_id"])
        a = self._mgr.get_agent(aid)
        sk = build_session_key(aid, channel=ch, peer_id=pid,
                               dm_scope=a.dm_scope if a else "per-peer")
    else:                                        # 走路由解析
        aid, sk = resolve_route(self._bindings, self._mgr, ch, pid)
    reply = await run_agent(self._mgr, aid, sk, text, on_typing=self._typing_cb)
    return {"agent_id": aid, "session_key": sk, "reply": reply}
```

`_m_send` 是路由系统的**对外入口**：客户端发一条 `{channel, peer_id, text}`，网关要么用显式 `agent_id`（跳过路由，相当于 s05 REPL 的 `/switch`），要么走 `resolve_route` 路由。然后 `run_agent` 跑出回复。返回里带了 `agent_id` 和 `session_key`，让客户端知道「这条消息被谁、算作哪段对话处理了」——方便客户端做对账。

### 7.3 typing 广播

```python
def _typing_cb(self, agent_id, typing):
    msg = json.dumps({"jsonrpc": "2.0", "method": "typing",
                      "params": {"agent_id": agent_id, "typing": typing}})
    for ws in list(self._clients):
        try:
            asyncio.ensure_future(ws.send(msg))
        except Exception:
            self._clients.discard(ws)
```

`run_agent` 的 `on_typing` 回调被接到这里。注意它发的是**没有 `id` 的 JSON-RPC 通知**（JSON-RPC 规定：没有 id 就是通知，不需要响应）。每当某 agent 开始/结束处理，就向**所有**在线客户端广播 `typing` 事件——这样前端能显示「Luna 正在输入…」。这是 s04 里 `send_typing`（只发回单个 Telegram chat）的网络版：这里面向的是多客户端广播。

---

## 8. REPL：把上述能力做成可玩的样子

```python
def repl():
    mgr, bindings = setup_demo()
    ...
    ch, pid = "cli", "repl-user"
    force_agent = ""
    ...
    while True:
        user_input = input("You > ").strip()
        ...
        if force_agent:                          # /switch 强制
            agent_id = force_agent
            session_key = build_session_key(agent_id, channel=ch, peer_id=pid,
                                            dm_scope=a.dm_scope if a else "per-peer")
        else:                                    # 正常路由
            agent_id, session_key = resolve_route(bindings, mgr, channel=ch, peer_id=pid)
        reply = run_async(run_agent(mgr, agent_id, session_key, user_input))
```

REPL 模拟「一个固定来源（cli / repl-user）的客户端」。命令一览：

- `/bindings`：列出所有路由绑定（`cmd_bindings`，按 tier 上色）。
- `/route <ch> <peer> [acc] [guild]`：**不真跑 agent，只看路由会解析到哪**（`cmd_route`）——纯调试用，验证绑定表逻辑。
- `/agents` / `/sessions`：查看注册的 agent 和当前会话。
- `/switch <id>`：强制用某个 agent，绕过路由（对应 `_m_send` 里带 `agent_id` 的分支）。`/switch off` 恢复。
- `/gateway`：在后台事件循环里启动 WebSocket 服务。

`run_async(run_agent(...))` 是关键调用：把异步的 `run_agent` 协程通过共享事件循环跑起来，主线程同步阻塞等结果。这就是「同步 REPL + 异步内核」的桥接点。

---

## 9. 和 s04 的对照：路由层带来了什么

| 维度 | s04 Channels | s05 Gateway |
|------|-------------|-------------|
| agent 数量 | 1 个（写死 main） | 多个（注册中心管理） |
| 「用哪个 agent」 | 不存在该问题 | 五层绑定表路由 |
| 会话键 | 固定 `agent:main:direct:{ch}:{peer}` | `dm_scope` 可配的 4 种粒度 |
| 入站来源 | Telegram 拉取 + CLI stdin | WebSocket + REPL（平台无关的 JSON-RPC） |
| 执行模型 | 同步串行 | asyncio + Semaphore(4) 限流并发 |
| 对外接口 | 无（本地进程） | WebSocket JSON-RPC 服务 |

可以看出 s05 的两个跃迁：

1. **从「单 agent」到「多 agent + 路由」**：消息不再无条件进 main，而是按 (channel, peer) 找到该找的 agent。这让一个网关能同时承载多个角色（Luna 和 Sage）。
2. **从「本地脚本」到「网络服务」**：GatewayServer 把路由 + agent 能力包成 WebSocket 协议，外部程序（前端、其他 bot 框架）可以远程调用。这是「网关」这个词的真正含义——它成了一个对外服务的枢纽。

---

## 10. 留白与后续章节

s05 把路由骨架搭起来了，但有意留了很多坑给后续：

| 留白 | 现状 | 谁来补 |
|------|------|--------|
| 会话持久化 | 内存 dict，重启即丢 | s03 的 JSONL（本节没接入） |
| 提示词工程 | 硬拼 `system_prompt` 字符串 | s06 的 8 层提示词组装 |
| 主动行为 | 只能被动响应消息 | s07 的心跳 + cron |
| 可靠投递 | 回合内同步发，失败即丢 | s08 的预写队列 |
| 重试 | API 失败只回滚不重试 | s09 的 3 层重试洋葱 |
| 并发序列化 | Semaphore(4) 简单限流，同 peer 不保证顺序 | s10 的命名车道 + FIFO |
| 配置来源 | 绑定表靠 `setup_demo` 硬编码 | 生产用配置文件（见 md 对照表） |

路由层本身是最「稳」的一层——它的逻辑（五层漏斗 + 首次匹配）从 s05 到生产代码基本不变，变的是它周围的可靠性/智能/并发设施。

---

## 11. 运行方法

```sh
cd claw0
python sessions/zh/s05_gateway_routing.py
```

需要 `.env` 里配好 `ANTHROPIC_API_KEY` 和 `MODEL_ID`。

进 REPL 后的试玩路径：

```sh
/bindings                       # 看三条演示绑定
/route cli user1                # 落到 default -> Luna
/route telegram user2           # channel 命中 -> Sage
/route discord admin-001        # peer_id 复合命中 -> Sage
/route discord user3           # 都不命中 -> Luna

/switch sage                    # 强制用 Sage（绕过路由）
Hello!                          # 和 Sage 对话
/switch off                     # 恢复路由

/gateway                        # 启动 ws://localhost:8765
```

启动 gateway 后，可以用任意 WebSocket 客户端发 JSON-RPC：

```sh
# 用 wscat 演示（需另装）
wscat -c ws://localhost:8765
> {"jsonrpc":"2.0","id":1,"method":"send","params":{"channel":"telegram","peer_id":"user2","text":"hi"}}
< {"jsonrpc":"2.0","id":1,"result":{"agent_id":"sage","session_key":"agent:sage:direct:user2","reply":"..."}}

> {"jsonrpc":"2.0","id":2,"method":"status"}
< {"jsonrpc":"2.0","id":2,"result":{"running":true,"uptime_seconds":12.3,"connected_clients":1,...}}

> {"jsonrpc":"2.0","method":"typing","params":{"agent_id":"sage","typing":true}}   # 服务端广播的通知
```

---

## 12. 一句话总结

s05 = **一张五层（peer > guild > account > channel > default）的绑定表，按「最具体的先赢」把每条入站消息路由到某个 agent，再用可配的 `dm_scope` 决定会话隔离粒度，最后通过 WebSocket + JSON-RPC 把这套「路由 + 多 agent」能力对外暴露成一个网络枢纽**。它在 s04 的「消息能进出」之上，加了「消息能找到对的 agent、对外能被远程调用」这两层，是从「单 agent 脚本」迈向「多 agent 网关」的关键一跃；而它周围所有可靠性/智能/并发能力，都留给 s06-s10 逐个补全。
