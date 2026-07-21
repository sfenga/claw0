# Design: add-s11-integrated-gateway

## 目标与约束

- **自包含**:`s11_integrated.py` 一个文件,不跨节 import,复现各层核心算法(符合 claw0 每节独立约定)。
- **集大成**:全部 10 层都在,且按生产形态接线(不是各层并列展示,而是真正协作)。
- **可运行**:`.venv/bin/python sessions/zh/s11_integrated.py` 直接跑;飞书消息能端到端收发并经投递队列+车道。

## 架构:以车道为脊柱,各层挂载

```
   飞书(WS 长连接)──┐                              ┌── FeishuChannel.send (经投递队列)
   Telegram(长轮询,可选)──┤   InboundMessage       │
   CLI(stdin 非阻塞)──┘      │                       │
            msg_queue + q_lock                         │
                   │                                   │
           ┌───────▼───────┐    resolve_route          │
           │  drain 入站   │───► BindingTable ──► agent_id
           └───────┬───────┘    build_session_key ──► session_key
                   │                                   │
            入队 main 车道                              │
                   ▼                                   │
   ┌───────────────────────────────────┐               │
   │ LaneQueue(main)  LaneQueue(cron)  │               │
   │   LaneQueue(heartbeat)            │               │
   │  CommandQueue 路由 + generation   │               │
   └───────────┬───────────────────────┘               │
               ▼                                       │
   run_agent_turn(inbound, session, mgr):              │
     SoulSystem.build_system_prompt(+auto memory)       │
     → ResilientAgent.run(三层洋葱):                    │
         L1 ProfileManager 选 key/轮换                  │
         L2 ContextGuard 溢出压缩                      │
         L3 工具循环 (stop_reason=tool_use↔tool_result) │
     → 文本回复                                        │
               │                                       │
               ▼                                       │
   DeliveryQueue.enqueue(channel, to, text) ───────────┘
     → DeliveryRunner: chunk_message → channel.send → ack/fail(退避)
```

## 关键设计决策

### D1: 回复一律经投递队列,不直接 channel.send
s04 的 `run_agent_turn` 直接 `channel.send(peer_id, text)`。s11 改为 `DeliveryQueue.enqueue` → `DeliveryRunner` 取出 → `chunk_message`(通道感知分片)→ `channel.send` → 成功 `ack` / 失败 `fail`(指数退避 + 抖动,重试耗尽移入 `failed/`)。理由:崩溃不丢消息(预写日志),失败自动重试(s08 核心价值)。CLI 通道也走队列(统一路径,便于观察),只是 send 即 print。

### D2: 入站经车道,不直接跑 agent
s10 的 `agent_loop` 把用户输入 `enqueue(LANE_MAIN, _turn)` 并 `future.result(timeout)`。s11 复用此模式:飞书/Telegram/CLI 入站消息都 `enqueue(LANE_MAIN, turn_fn)`,串行化(避免同一会话并发改 messages 列表)。心跳入 `LANE_HEARTBEAT`,cron 入 `LANE_CRON`。generation 追踪支持重启恢复语义。

### D3: 网关五级路由 + 单 agent 默认绑定
s05 的 `BindingTable` 五级解析保留;默认装一条 tier-5 default 绑定 → 单 agent `default`。用户可通过 REPL `/bindings add ...` 加更具体绑定(如某 peer → 另一 agent)。`build_session_key(per-peer)` 做会话隔离:不同飞书用户各自独立会话。

### D4: 韧性三层洋葱直接用于 agent 回合
s09 的 `ResilienceRunner` 三层(轮换→压缩→工具循环)作为 s11 的 `ResilientAgent.run`,包裹每次 `messages.create`。多 key 从 env 读取(`ANTHROPIC_API_KEY` + `ANTHROPIC_API_KEY_2`...),单 key 也能跑(只一个 profile)。溢出时 `ContextGuard.compact_history` 用 LLM 摘要前 50%,保留近 20%。

### D5: 记忆复用 s06 完整版(含硬遗忘)
直接复现 s06 的 `MemoryStore`(evergreen + daily + 混合检索 + `memory_forget` + TTL + 留存期清理),不退化成 s10 的简化版。`memory_forget` 工具、`/forget` REPL 命令、stats 计数全保留。系统提示词组装时做 auto-recall(用最近入站文本检索 top-k 记忆注入)。

### D6: cron 用 croniter 真 cron 表达式
s10 的 cron 是 `every_seconds` 简化版。s11 按 s07 用 croniter:CRON.json 里 `schedule.cron` 字段是标准 5 段 cron 表达式(如 `0 9 * * *`),`croniter.croniter(expr, start_time)` 算 next_run。每秒 tick 检查到期则入 `LANE_CRON`。连续 5 次错误自动禁用。

### D7: 非阻塞 stdin 实时排空(沿用 s04 实时补丁)
主循环用 `select.select([sys.stdin], [], [], 0.5)` 非阻塞轮询:无输入时回到顶部排空 `msg_queue`(飞书/Telegram 入站)+ 心跳/cron 输出;有输入则处理;EOF(Ctrl-D)退出。与 s04 实时补丁一致,加 EOF 守卫。

### D8: 飞书 send 的 receive_id_type 修复一并带入
s04 的 `invalid receive_id` 修复(`ou_`→`open_id`、`oc_`→`chat_id`)是 s11 `FeishuChannel.send` 的必带逻辑,否则私聊回复会失败。

## REPL 命令集

| 命令 | 作用 |
|---|---|
| `/channels` `/accounts` | 列已注册通道/账户 |
| `/bindings` `/agents` `/sessions` | 网关:绑定表/agent/会话 |
| `/soul` `/prompt` `/bootstrap` | 智能:灵魂文件/完整提示词/启动数据 |
| `/memory` `/search <q>` `/forget date=\|category=` | 记忆:统计/检索/硬遗忘 |
| `/heartbeat` `/trigger` `/cron` | 心跳状态/强制触发/cron 作业 |
| `/lanes` `/queue` `/enqueue <lane> <msg>` `/concurrency <lane> <N>` `/generation` `/reset` | 并发车道 |
| `/delivery` `/profiles` | 投递队列状态/认证 profile 轮换状态 |
| `/help` `quit`/`exit` | 帮助/退出 |

## 非目标(明确不做)

- Telegram 媒体组合并、offset 持久化细节(s04 已有,s11 只取其长轮询与 allowed_chats 过滤)。
- s05 的 WebSocket 网关服务器(`GatewayServer`/asyncio)——s11 只取 `BindingTable` 路由表,不做对外 HTTP 服务。
- s08 的死信人工干预 UI——保留 `failed/` 目录与 `retry_failed`,但不做交互式修复。
- 多 agent 持久化路由——单 agent + 默认绑定即可演示;REPL 可加绑定但不持久化到磁盘。

## 验证策略

1. `py_compile` 通过。
2. 离线冒烟(不调 LLM):车道 enqueue→Future、`forget(category/date)`、留存期清理、`chunk_message`、`compute_backoff`、`BindingTable.resolve`、`classify_failure`、`build_session_key`。
3. 飞书连接探针(已在 s04 验证过 ws 连通)。
4. 在线(需有效 LLM key):飞书私聊发消息 → 终端打印 `[feishu/ws]` → agent 回合(可见 `[tool: ...]`)→ 回复经投递队列 → 飞书收到回复。
