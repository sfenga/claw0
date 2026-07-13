# claw0 项目整体框架与数据流

> 一张图看懂「从用户发消息到 agent 回复」中间发生了什么——10 章机制如何层层包裹同一个 `while True` 循环。

本文是 claw0 项目的**鸟瞰图**，把 s01-s10 的所有机制串成一张完整的数据流图。读完前 10 篇分章详解后看本文，能把零散机制拼成整体；没读分章的也能从本文快速建立全局认知。

---

## 0. 项目定位

**claw0 = 「从零到一：构建 AI Agent 网关」的教学仓库**。

- 10 个渐进式章节（s01-s10），每章一个 `.py`（可直接运行）+ 一个 `.md`（配套文档）；
- 3 种语言（中/英/日），约 7000 行 Python；
- 每章只引入一个新概念，前一节代码原样保留；
- 学完全部 10 节，能顺畅阅读生产级网关 OpenClaw 的代码。

它的核心论点：**AI agent 就是一个 `while True` 循环加上一张分发表，外面包裹着持久化、路由、智能、调度、可靠性、韧性和并发控制的层层机制。** s01 那个循环，到 s10 的核心依然清晰可辨。

---

## 1. 一句话记住每一章

| 章 | 标题 | 一句话 | 机制 |
|----|------|--------|------|
| s01 | Agent Loop | `while True` + `stop_reason`——这就是一个 agent | 循环 |
| s02 | Tool Use | 工具 = schema dict + handler map，模型选名你查表执行 | 分发表 |
| s03 | Sessions | JSONL 追加写 + 重放；太大了就总结旧消息 | 持久化 |
| s04 | Channels | 每个平台都不同，但都产同一个 `InboundMessage` | 通道 |
| s05 | Gateway | 5 层绑定表把 (channel, peer) 映射到 agent | 路由 |
| s06 | Intelligence | 系统提示词 = 磁盘文件，8 层动态组装 | 智能 |
| s07 | Heartbeat & Cron | 定时线程「该不该跑」+ 和用户消息共用同一管线 | 自治 |
| s08 | Delivery | 先写磁盘再发送，崩溃也丢不了消息 | 可靠投递 |
| s09 | Resilience | 3 层重试洋葱：轮换 key → 压缩历史 → 工具循环 | 韧性 |
| s10 | Concurrency | 命名车道 + generation 追踪，序列化混沌 | 并发 |

---

## 2. 整体分层架构

10 章是**层层包裹**的结构——内核是 agent 循环，外面包着一圈圈机制：

```
+=================== claw0 分层架构 ===================+
|                                                      |
|  s10: Concurrency  (命名车道, generation 追踪)       |  ← 并发外壳
|  s09: Resilience   (3 层重试洋葱, 认证轮换)          |  ← 韧性层
|  s08: Delivery     (预写磁盘队列, 退避重试)          |  ← 投递层
|  s07: Heartbeat    (心跳巡检, cron 定点)             |  ← 自治层
|  s06: Intelligence (8 层 prompt, 记忆混合搜索)      |  ← 智能层
|  s05: Gateway      (5 级绑定, 会话隔离)              |  ← 路由层
|  s04: Channels     (Telegram/飞书/CLI 通道)          |  ← 通道层
|  s03: Sessions     (JSONL 持久化, 上下文压缩)       |  ← 持久化层
|  s02: Tools        (dispatch table, 工具)           |  ← 执行层
|  s01: Agent Loop   (while True + stop_reason)       |  ← 内核
|                                                      |
+======================================================+
```

越往外层，离「调 LLM」越远、越偏「让 agent 能在真实世界长期可靠运行」。最内层 s01 是「一次 agent 回合」，最外层 s10 是「多个回合并发有序」。

---

## 3. 核心数据结构词汇表

数据流里反复出现这几个对象，先记住它们：

| 对象 | 定义于 | 含义 |
|------|--------|------|
| `InboundMessage` | s04 | 入站消息的统一格式（text/sender_id/channel/peer_id/...）|
| `Binding` | s05 | 一条路由规则（agent_id/tier/match_key/match_value）|
| `session_key` | s05 | 会话标识（`agent:{id}:direct:{peer}` 等）|
| `messages` | s01/s03 | 对话历史列表，发给 LLM 的 `messages` 参数 |
| `system_prompt` | s06 | 系统提示词（8 层动态组装）|
| `QueuedDelivery` | s08 | 投递队列条目（channel/to/text/retry_count/...）|
| `AuthProfile` | s09 | 一个 API key + 冷却状态 |
| `LaneQueue` | s10 | 一个命名车道（FIFO + max_concurrency + generation）|
| `Future` | s10 | 异步结果容器（`concurrent.futures.Future`）|

---

## 4. 完整数据流：四种场景

数据流按「**消息从哪来**」分四种场景，机制组合不同。逐个画。

### 场景 A：用户在 Telegram 发消息 → agent 回复（最完整的链路）

```
[用户在 Telegram 群里发 "你好"]
        │
        ▼  ① 拉取(s04 拉取模型)
┌──────────────────────────────────┐
│ telegram_poll_loop (后台线程)      │  s04
│   tg.poll() → getUpdates 长轮询    │
│   三层缓冲(_seen去重/_media_groups  │
│   /_text_buf 1s) → 规范化          │
│   产 InboundMessage(               │
│     channel="telegram",            │
│     peer_id="12345",   ← 群chat_id │
│     text="你好", ...)              │
│   with q_lock:                     │
│     msg_queue.append(msg)  ← 入队  │
└──────────────┬───────────────────┘
               │
        ▼  ② 主循环取消息(s04/s10)
┌──────────────────────────────────┐
│ agent_loop 主循环 (每轮开头)        │
│   with q_lock: tg_msgs = queue[:] │  s04
│   for m in tg_msgs:                │
│     run_agent_turn(m, ...)  ↓     │
└──────────────┬───────────────────┘
               │  (s10 版本里: 入 main 车道, future.result() 等)
        ▼  ③ 路由(s05)
┌──────────────────────────────────┐
│ resolve_route(bindings, mgr,     │  s05
│   channel="telegram",            │
│   peer_id="12345")               │
│   BindingTable.resolve() 5层漏斗  │
│   → agent_id="sage"  (4层channel │
│     命中 telegram→sage)           │
│   build_session_key(dm_scope)    │
│   → "agent:sage:direct:12345"    │
└──────────────┬───────────────────┘
               │  (agent_id, session_key)
        ▼  ④ 取会话历史(s03)
┌──────────────────────────────────┐
│ mgr.get_session(session_key)     │  s03
│   → messages 列表 (从 JSONL 重放) │
│ messages.append({"role":"user",  │
│   "content":"你好"})              │
└──────────────┬───────────────────┘
               │
        ▼  ⑤ 自动召回记忆 + 组装提示词(s06)
┌──────────────────────────────────┐
│ memory_context = _auto_recall(   │  s06
│   "你好")  ← 混合搜索(关键词+向量  │
│   +时间衰减+MMR) top3            │
│ system_prompt = build_system_     │
│   prompt(8层: 身份→灵魂→工具→技能  │
│   →记忆→引导→运行时→渠道)        │
└──────────────┬───────────────────┘
               │  (system, messages, tools)
        ▼  ⑥ 三层重试洋葱调 LLM(s09)
┌──────────────────────────────────┐
│ ResilienceRunner.run()            │  s09
│  Layer1: 选未冷却的 AuthProfile    │
│  Layer2: 溢出则截断+LLM摘要压缩   │
│  Layer3: _run_attempt 工具循环 ←  │  s01/s02
│    while True:                    │
│      response = client.messages   │
│        .create(system, messages,  │
│          tools)                   │
│      if end_turn: return 回复     │
│      elif tool_use:              │
│        process_tool_call(         │  s02 dispatch
│          block.name, block.input)│
│        塞 tool_result, continue   │
│  mark_success(profile)            │
└──────────────┬───────────────────┘
               │  回复文本
        ▼  ⑦ 分片 + 入投递队列(s08)
┌──────────────────────────────────┐
│ chunks = chunk_message(回复,     │  s08
│   "telegram")  ← 按 4096 分片    │
│ for chunk in chunks:              │
│   queue.enqueue("telegram",      │
│     "12345", chunk)  ← 预写磁盘  │
│     (tmp+fsync+os.replace 原子写)│
└──────────────┬───────────────────┘
               │
        ▼  ⑧ 后台投递(s08)
┌──────────────────────────────────┐
│ DeliveryRunner (后台线程 1s 扫描)  │  s08
│   deliver_fn("telegram", "12345",│
│     chunk) → TelegramChannel.send│  s04
│       → POST /sendMessage        │
│   成功 → ack(删 .json)            │
│   失败 → fail(退避5s→10min重试,  │
│     5次移 failed/)               │
└──────────────┬───────────────────┘
               │
        ▼
[消息出现在 Telegram 群 12345]
```

这是最长的链路——一条「你好」要过 8 道工序。注意：**s01-s02 是真正调 LLM 的内核，s03-s08 是它前后的设施，s09 包裹着 s01-s02 做韧性，s10 在最外层把整个回合塞进车道并发**。

### 场景 B：心跳主动触发 → 输出（主动链路）

```
[没人发消息, 但到点了(30分钟 + 9-22点)]
        │
        ▼  ① 心跳线程检查(s07/s10)
┌──────────────────────────────────┐
│ HeartbeatRunner._loop (后台线程)  │  s07
│   should_run() 4检查              │
│   (HEARTBEAT.md存在/间隔/时段/    │
│    没在跑)                        │
└──────────────┬───────────────────┘
               │  (s10版: 查 heartbeat 车道 active>0 则跳过)
        ▼  ② 入车道(s10) / 抢锁(s07)
┌──────────────────────────────────┐
│ cmd_queue.enqueue("heartbeat",   │  s10
│   _do_heartbeat)  ← 入心跳车道    │
│ (s07版: lane_lock.acquire(       │
│   blocking=False) 抢不到让步)     │  s07
└──────────────┬───────────────────┘
               │
        ▼  ③ 组装心跳 prompt(s06 简化版)
┌──────────────────────────────────┐
│ instructions = HEARTBEAT.md 全文  │  s06
│   (巡检清单+回复纪律)             │
│ sys_prompt = SOUL.md + MEMORY.md │
│   + 当前时间                     │
└──────────────┬───────────────────┘
               │
        ▼  ④ 单轮 LLM(无工具无历史)(s07/s09)
┌──────────────────────────────────┐
│ run_agent_single_turn(           │  s07
│   instructions, sys_prompt)      │
│   (经 s09 洋葱包裹)               │  s09
│ → "提醒: 3点开会" 或 "HEARTBEAT_OK"│
└──────────────┬───────────────────┘
               │
        ▼  ⑤ 解析+去重(s07)
┌──────────────────────────────────┐
│ _parse_response:                 │  s07
│   HEARTBEAT_OK → 丢弃(没事)       │
│   有内容 → 和 _last_output 比对   │
│     重复 → 丢弃                   │
│     不重复 → output_queue.append │
└──────────────┬───────────────────┘
               │
        ▼  ⑥ 主循环 drain 输出(s07)
┌──────────────────────────────────┐
│ 主循环每轮开头:                   │
│   for msg in heartbeat.           │
│     drain_output():              │
│     print_lane("heartbeat", msg) │
└──────────────┬───────────────────┘
               │
        ▼
[REPL 显示: [heartbeat] 提醒: 3点开会]
```

对比场景 A：心跳是**单轮、无工具、无历史**的轻量调用（s07 的 `run_agent_single_turn`），不复用 s06 完整 8 层，用的是简化版。它走的是和用户对话**同一条 agent 管线**，只是触发源从「用户消息」变成「定时器」。

### 场景 C：cron 定点任务

```
[CRON.json: 每天9点 "Generate a daily summary"]
        │
        ▼  ① cron 线程 tick(s07/s10)
┌──────────────────────────────────┐
│ cron_loop (后台线程, 1s tick)      │  s07
│   CronService.tick()              │
│   for job in jobs:                │
│     enabled? 到点(next_run_at<=now)?│
│     _enqueue_job(job, now)       │
└──────────────┬───────────────────┘
               │  (s10版: 入 cron 车道, 同车道串行)
        ▼  ② 执行 payload(s07)
┌──────────────────────────────────┐
│ _do_cron():                       │  s07
│   run_agent_single_turn(          │
│     payload.message,  ← 写死的内容│
│     "You are performing a         │
│      scheduled task...")          │
│   (经 s09 洋葱)                   │  s09
└──────────────┬───────────────────┘
               │
        ▼  ③ 回调更新 job 状态(s07)
┌──────────────────────────────────┐
│ _on_done:                         │  s07
│   job.next_run_at = now+every    │
│   成功 → consecutive_errors=0    │
│   失败 → consecutive_errors++    │
│     >=5 → enabled=False (熔断)   │
│   写 cron-runs.jsonl 日志         │
│   output_queue.append            │
└──────────────┬───────────────────┘
               │
        ▼  ⑥ 主循环 drain
[REPL 显示: [cron] [Daily Summary] 今天完成了...]
```

cron 和心跳的差异：**cron 干什么是 payload 写死的**（「发日报」），心跳是 agent 自主判断（「有没有事」）；cron 有熔断+日志，心跳没有。两者都进各自车道（s10）。

### 场景 D：WebSocket 网关远程调用（s05）

```
[外部程序通过 WebSocket 连 ws://localhost:8765]
        │
        ▼  ① 连接处理(s05)
┌──────────────────────────────────┐
│ GatewayServer._handle(ws)        │  s05
│   _clients.add(ws)               │
│   async for raw in ws:  ← 收消息  │
│     resp = _dispatch(raw)        │
│     ws.send(json.dumps(resp))    │
└──────────────┬───────────────────┘
               │
        ▼  ② JSON-RPC 分派(s05)
┌──────────────────────────────────┐
│ _dispatch(raw):                   │  s05
│   json.loads → req               │
│   methods["send"] → _m_send      │
│   (分派表: send/bindings.set/     │
│    sessions.list/...)            │
└──────────────┬───────────────────┘
               │
        ▼  ③ 路由+跑 agent(s05/s09)
┌──────────────────────────────────┐
│ _m_send(params):                  │  s05
│   text/ch/peer_id 从 params      │
│   if agent_id: 显式指定(绕路由)   │
│   else: resolve_route(...)  ← 5层 │  s05
│   reply = run_agent(...)  ← 异步  │  s06/s09
│     (Semaphore(4) 限流, to_thread│
│      包同步SDK)                  │  s10 并发
│   return {agent_id, sk, reply}   │
└──────────────┬───────────────────┘
               │
        ▼  ④ 响应回客户端(s05)
[客户端收到 {"jsonrpc":"2.0","id":1,
   "result":{"agent_id":"sage","reply":"..."}}]

   期间 agent 开始处理时, _typing_cb
   广播 typing 通知给所有在线客户端(s05)
```

场景 D 把「本地脚本」变成「网络服务」——外部程序不用进 REPL，发 JSON-RPC 就能用 agent。这是 s05「网关」一词的真正含义。

---

## 5. 各章在数据流中的位置

把四种场景叠加，看每章落在数据流的哪个环节：

```
入站方向 ─────────────────────────────────────────────────► 出站方向
消息进来     路由      会话/记忆/提示词      调 LLM        投递出去

 s04 通道 → s05 路由 → s03会话 → s06智能 → s09韧性 → s01/s02循环 → s08投递 → s04通道
  (拉取/    (5层绑定)  (JSONL/  (8层prompt  (3层洋葱   (while True   (预写队列   (send)
   webhook)           重放)    +记忆召回)  包裹内核)   +分发表)       +退避)
        
s07 自治: 心跳/cron 从定时器进入, 复用同一条管线(单轮无工具)
s10 并发: 整个回合包进命名车道, 多回合/多车道并发, generation 保护重启
```

- **s01/s02**：数据流最中心，真正调 LLM + 执行工具；
- **s03/s04/s05**：入站方向——通道接消息、路由找 agent、会话取历史；
- **s06/s09**：调 LLM 前后——组装 prompt（前）、韧性重试（包裹）；
- **s08**：出站方向——回复入队、后台可靠投递；
- **s07**：旁路——定时器触发，复用主管线；
- **s10**：外层壳——把整个回合塞进车道并发。

---

## 6. 关键设计模式总结

整个项目反复用几个设计模式，理解它们就抓住了骨架：

### 6.1 dispatch table（分发表）

一dict把「名字」映射到「handler」，代替 if/elif 长链。出现了**四次**：
- s02 `TOOL_HANDLERS`：工具名 → handler；
- s05 `methods`：JSON-RPC method → handler；
- s05 `BindingTable`：绑定表匹配（按 tier）；
- s10 `CommandQueue._lanes`：车道名 → LaneQueue。

加新东西只需加一行——开闭原则。

### 6.2 边界协议（归一化 + 边界对象）

用统一对象吸收差异：
- s04 `InboundMessage`：所有平台消息归一为同一格式，agent 不碰平台细节；
- s05 `(agent_id, session_key)`：把平台坐标翻译成 agent 坐标；
- s08 `QueuedDelivery`：投递意图归一，不管发到哪都先入队。

### 6.3 预写日志（WAL）+ 原子写

「先持久化意图，再执行动作」：
- s03 JSONL 追加写（会话）；
- s08 tmp+fsync+os.replace（投递队列，崩溃安全）；
- s07 cron-runs.jsonl（运行日志）。

### 6.4 分类驱动分层

s09 的精髓：**先判断失败种类，再路由到对应层处理**——换 key 能治的换 key、换 key 治不了的压缩。不同失败不同路径，不无脑重试。

### 6.5 事件驱动自泵送

s10 `_pump`：入队和任务完成时各触发一次，没有外部调度器循环——零延迟零空转。

### 6.6 静态缓存 / 动态重建

s06 的取舍：bootstrap 文件、skills_block 启动算一次缓存（不变）；记忆召回、运行时时间每轮重算（会变）。

### 6.7 用户优先（从让步到不同道）

- s07：一把锁，后台非阻塞抢、抢不到让步；
- s10：多车道，用户进 main、后台进各自车道，根本不同道、无需让步。

---

## 7. 一张总数据流图

把所有场景、所有章节压成一张图：

```
                ┌────────────── 外部世界 ──────────────┐
                │  Telegram  飞书  CLI  WebSocket客户端  │
                └──┬──────────┬──────┬──────┬─────────┘
                   │ s04拉取   │s04推 │s04   │s05 ws
                   ▼          ▼      ▼      ▼
              ┌────────────────────────────────────┐
              │  s04 通道层: InboundMessage 归一化    │
              └─────────────────┬──────────────────┘
                                │
              ┌─────────────────▼──────────────────┐
              │  s05 路由层: 5层绑定表 → (agent_id,  │
              │           session_key)              │
              └─────────────────┬──────────────────┘
                                │
       ┌────────────────────────┼────────────────────────┐
       │                        │                        │
       ▼ 定时器旁路(s07)        ▼ 用户/WS主路           ▼ cron旁路(s07)
 ┌──────────┐           ┌──────────────────┐       ┌──────────┐
 │ heartbeat│           │ s03 会话: 取messages│      │   cron   │
 │  车道    │           │ s06 智能: 8层prompt │      │  车道    │
 │ (s10)    │           │   +记忆召回         │      │ (s10)    │
 └────┬─────┘           └──────────┬──────────┘       └────┬─────┘
      │                            │                       │
      │              ┌─────────────▼──────────────┐        │
      │              │ s09 韧性: 3层重试洋葱        │        │
      │              │  L1 轮换key → L2 压缩 →     │        │
      │              │  L3 while True+工具(s01/02) │        │
      │              └─────────────┬──────────────┘        │
      │                            │ 回复文本                │
      └──────────────┬─────────────┴──────────────────────┘
                     ▼
              ┌──────────────────────────────────────┐
              │ s08 投递: chunk分片 → 预写磁盘队列     │
              │   → DeliveryRunner 退避重试 → ack/fail│
              └─────────────────┬────────────────────┘
                                │
              ┌─────────────────▼──────────────────┐
              │  s04 通道层: Channel.send → 平台 API │
              └─────────────────┬──────────────────┘
                                │
                ┌───────────────▼────────────────┐
                │   外部世界: 消息出现在各平台    │
                └────────────────────────────────┘

   最外层 s10: 命名车道 + generation 包裹整个流程,
                多车道并发, 同车道串行, 重启安全
```

---

## 8. 一句话总结

claw0 的整体框架 = **一个 `while True`+分发表的 agent 内核（s01-s02），外面包着层层机制**：入站方向 s04 通道归一化 → s05 路由 5 层绑定 → s03 取会话 → s06 组装 8 层提示词+召回记忆；内核被 s09 三层重试洋葱包裹（轮换 key/压缩历史/工具循环）真正调 LLM；出站方向 s08 分片入预写磁盘队列、后台退避重试投递、再经 s04 通道发回平台；旁路有 s07 心跳/cron 从定时器复用同管线主动行动；最外层 s10 命名车道+generation 把整个回合包进车道、多车道并发同车道串行、重启安全。数据流按来源分四场景（用户消息/心跳/cron/WebSocket 网关），机制组合不同但都走同一条「通道→路由→会话→智能→韧性→内核→投递→通道」主管线。反复用的设计模式有 dispatch table（出现 4 次）、边界归一对象、预写日志+原子写、分类驱动分层、事件驱动自泵送、静态缓存动态重建、用户优先从让步到不同道。AI agent 就是一个循环加一张分发表，外面包着持久化/路由/智能/调度/可靠性/韧性/并发的层层机制——这就是 claw0 从零到一要教的所有。

result: 在 `docs/explain/architecture-overview.md` 写了 claw0 项目整体框架+数据流文档（8 节：项目定位、10章一句话表、分层架构图、核心数据结构词汇表、四种场景完整数据流图——用户消息/心跳/cron/WebSocket网关、各章在数据流中的位置、7 个关键设计模式、总数据流图、一句话总结），现进入工作树提交并合并回 main。
