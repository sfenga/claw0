# Integrated Gateway Capability Specification

> 把 claw0 的 10 个能力层(工具循环、会话、多通道、网关路由、智能记忆、心跳、cron、可靠投递、韧性、并发车道)集成为一个自包含可运行单元。本 spec 首次建立,作为各层在生产形态下如何接线的契约。

## ADDED Requirements

### Requirement: Agent Loop with Tool Use

系统 SHALL 实现以 `while True` + `stop_reason` 驱动的工具循环:模型返回 `end_turn` 时输出文本;返回 `tool_use` 时按 dispatch table 执行工具、回填 `tool_result`、继续循环。工具集 SHALL 包含 `bash`、`read_file`、`write_file`、`edit_file`、`list_directory`、`get_current_time`、`memory_write`、`memory_search`、`memory_forget`。

#### Scenario: 工具循环 end_turn
- **WHEN** 模型对一条用户消息先返回 `stop_reason=tool_use`(调用 `get_current_time`),工具回填结果后第二轮返回 `stop_reason=end_turn` 带文本
- **THEN** 系统向来源通道投递该文本,且会话历史含两轮 assistant + 中间 tool_result

#### Scenario: 未知工具不崩
- **WHEN** 模型调用 dispatch table 中不存在的工具名
- **THEN** 系统回填 `Error: Unknown tool '<name>'` 作为 tool_result 继续循环,不抛异常退出

### Requirement: Session Persistence

系统 SHALL 用 JSONL 追加写持久化每个会话,会话键由 `build_session_key(agent_id, channel, account_id, peer_id, dm_scope)` 生成(默认 `per-peer`)。重建历史时 SHALL 把 `tool_use` 归入 assistant 消息、`tool_result` 归入 user 消息、合并连续 tool_result 到同一 user 消息。

#### Scenario: 不同 peer 会话隔离
- **WHEN** 飞书用户 A(`ou_aaa`)与用户 B(`ou_bbb`)在同一 agent 上各发一条消息
- **THEN** 两者落到不同会话键(`agent:default:direct:ou_aaa` vs `...:ou_bbb`),各自历史不串扰

### Requirement: Multi-Channel Inbound

系统 SHALL 通过统一 `InboundMessage(text, sender_id, channel, account_id, peer_id, is_group, media, raw)` 归一化 CLI、飞书、Telegram 三路入站,经共享 `msg_queue` + `q_lock` 喂入同一 agent 循环。飞书 SHALL 用 WebSocket 长连接(`lark-oapi` SDK 出站连接,无需公网回调),Telegram 长轮询为可选(仅当 `TELEGRAM_BOT_TOKEN` 存在时启用)。

#### Scenario: 飞书私聊入站
- **WHEN** 飞书用户给机器人发私聊 `你好`
- **THEN** `msg_queue` 收到 `InboundMessage(channel="feishu", peer_id=<open_id>, is_group=False)`,终端打印 `[feishu/ws] <open_id>: 你好`

#### Scenario: 群聊仅 @ 时响应
- **WHEN** 设了 `FEISHU_BOT_OPEN_ID` 且群消息未 @ 机器人
- **THEN** 该事件被 `_ws_bot_mentioned` 过滤,不入 `msg_queue`

### Requirement: Gateway Routing

系统 SHALL 用 `BindingTable` 五级绑定(peer_id / guild_id / account_id / channel / default,越小越具体)解析 `(channel, peer_id, account_id, guild_id) → agent_id`,第一个匹配获胜。SHALL 预装一条 tier-5 default 绑定指向 agent `default`,使单 agent 场景开箱即用。

#### Scenario: 默认绑定兜底
- **WHEN** 未加任何自定义绑定,飞书用户发消息
- **THEN** `resolve_route` 经 tier-5 default 命中 agent `default`

#### Scenario: peer 级绑定更具体获胜
- **WHEN** 已加 tier-1 绑定 `peer_id=feishu:ou_aaa → agent:vip`,且 tier-5 default → `default`,用户 `ou_aaa` 发消息
- **THEN** 命中 tier-1,路由到 `vip` 而非 `default`

### Requirement: Intelligence Layer

系统 SHALL 用 `SoulSystem` 从 `workspace/` 的 8 个提示词文件(SOUL/IDENTITY/TOOLS/USER/AGENTS/MEMORY/HEARTBEAT/BOOTSTRAP)组装系统提示词,并在组装时做记忆 auto-recall(用最近入站文本检索 top-k daily 记忆注入)。`MemoryStore` SHALL 提供 evergreen(`MEMORY.md`)+ daily(`memory/daily/*.jsonl`)双层、混合检索、`memory_forget`、TTL 懒跳过、留存期整文件清理。

#### Scenario: auto-recall 注入记忆
- **WHEN** 飞书用户问"我之前说过偏好什么",daily 记忆里有相关条目
- **THEN** 系统提示词的记忆段含检索到的条目,模型可引用

#### Scenario: evergreen 永不被遗忘触及
- **WHEN** `forget(category=preference)` 而 `MEMORY.md` 含 preference 段落
- **THEN** daily 层匹配条目被移除,但 `MEMORY.md` 内容不变

#### Scenario: TTL 过期条目懒跳过
- **WHEN** 一条 daily entry 的 `expires_at` 早于当前时刻,`memory_search` 检索其内容
- **THEN** 该条目不出现在检索结果,`auto_expired` 计数 +1;磁盘条目保留(懒清理,不物理删除)

#### Scenario: 超留存期 daily 整文件移除
- **WHEN** `MEMORY_RETENTION_DAYS=30` 且存在 `memory/daily/2025-01-01.jsonl`,当前日期为 2026-07-21
- **THEN** 下次记忆加载时该文件被从磁盘物理移除(unlink),其所有条目不再可检索,`auto_expired` 累加该文件条目数;`MEMORY.md` 不受影响

#### Scenario: 按日期遗忘整文件
- **WHEN** `memory_forget(date="2026-07-01")` 且该日文件存在 5 条记忆
- **THEN** 系统移除 `memory/daily/2026-07-01.jsonl` 整文件,返回 `Forgot 5 entries from 2026-07-01`,`explicit_forgotten` +5

#### Scenario: 按 category 跨文件遗忘
- **WHEN** `memory_forget(category="reminder")` 且 reminder 条目分布在 3 个 daily 文件共 7 条
- **THEN** 系统以临时文件+rename 原子重写这 3 个文件,移除全部 reminder 条目(保留其余),返回 `Forgot 7 entries (category=reminder)`,`explicit_forgotten` +7

### Requirement: Heartbeat (Proactive Agent)

系统 SHALL 在 `LANE_HEARTBEAT` 车道上运行后台心跳:`HeartbeatRunner.should_run` 检查 `HEARTBEAT.md` 存在且非空、间隔已到、在 active_hours 内;满足时用 soul+memory 组装提示词,跑一轮 agent,有意义的输出入队待排空。心跳 lane 忙时 SHALL 跳过该 tick(非阻塞语义)。

#### Scenario: 缺 HEARTBEAT.md 不触发
- **WHEN** `workspace/HEARTBEAT.md` 不存在
- **THEN** `should_run` 返回 `(False, "HEARTBEAT.md not found")`,心跳不跑

### Requirement: Cron Scheduling

系统 SHALL 用 croniter 解析 `CRON.json` 中每个作业的 `schedule.cron` 字段(标准 5 段 cron 表达式),计算下次运行时间,每秒 tick 检查到期则入 `LANE_CRON` 车道执行 payload。连续 5 次执行失败 SHALL 自动禁用该作业。

#### Scenario: cron 表达式到期触发
- **WHEN** 作业 cron=`0 9 * * *`(每天 9 点),当前时刻跨过 9:00
- **THEN** 该作业被入队 `LANE_CRON` 执行,`next_run_at` 推进到次日 9:00

### Requirement: Reliable Delivery

系统 SHALL 把所有出站回复经 `DeliveryQueue` 投递(预写日志:先 tmp+`os.replace` 原子写盘,再发),而非直接 `channel.send`。失败 SHALL 指数退避(+/-20% 抖动)重试,重试耗尽移入 `failed/`。文本超通道限制 SHALL 按通道分片(`chunk_message`)。飞书 `send` SHALL 按 `receive_id` 前缀选型:`ou_`→`open_id`(私聊)、`oc_`/其他→`chat_id`(群)。

#### Scenario: 私聊回复用 open_id 类型
- **WHEN** 向飞书私聊用户(`ou_xxx`)投递回复
- **THEN** 请求 `receive_id_type=open_id`,飞书返回 code 0,用户收到回复

#### Scenario: 投递失败退避重试
- **WHEN** 一次 `channel.send` 抛异常
- **THEN** 条目 `retry_count+1`、`next_retry_at` 设为 now+退避,未超 `MAX_RETRIES` 时留队列待重试

### Requirement: Resilience (3-Layer Retry Onion)

系统 SHALL 用三层重试洋葱包裹每次 `messages.create`:Layer1 `ProfileManager` 在多 API key 间轮换(失败按 `classify_failure` 分 rate_limit/auth/timeout/billing/overflow/unknown 置冷却);Layer2 上下文溢出时 `ContextGuard` 先截断 tool_result 再 LLM 摘要压缩前 50%(保留近 20%)后用同 profile 重试;Layer3 工具循环。所有 profile 耗尽 SHALL 尝试 fallback models。

#### Scenario: auth 失败轮换到下一 key
- **WHEN** profile A 的 `messages.create` 返回 401 auth 错误,且配置了 profile B(key2)
- **THEN** A 置冷却 300s,轮换到 B 重试,统计 `total_rotations+1`

#### Scenario: 溢出压缩后重试成功
- **WHEN** `messages.create` 报 context overflow,且 compact 后消息缩短
- **THEN** 用同 profile 重试,`total_compactions+1`,最终成功则 `mark_success`

### Requirement: Concurrency (Named Lanes)

系统 SHALL 用 `LaneQueue`(命名 FIFO、`max_concurrency`、generation 追踪、`concurrent.futures.Future`)为 main/cron/heartbeat 三车道串行化。入站消息、心跳、cron 经 `CommandQueue.enqueue(lane, fn)` 路由到各自车道。`reset_all` SHALL 递增所有 generation,使旧生命周期的过期任务不重新泵送队列。

#### Scenario: main 车道串行化同会话
- **WHEN** 同一飞书用户连续快速发 2 条消息
- **THEN** 两条都入 `LANE_MAIN`,按 FIFO 串行执行(第二条等第一条完成),不会并发改同一会话的 messages 列表

#### Scenario: reset 使旧任务过期
- **WHEN** `reset_all()` 后,旧 generation 的任务完成回调触发 `_task_done`
- **THEN** 因 `gen != current_generation`,不重新泵送队列

### Requirement: Unified REPL

系统 SHALL 提供统一 REPL,命令覆盖全部能力:`/channels /accounts /bindings /agents /sessions /soul /prompt /memory /search /forget /heartbeat /trigger /cron /lanes /queue /enqueue /concurrency /generation /reset /delivery /profiles /help`。主循环 SHALL 用非阻塞 stdin 轮询(`select` 0.5s)实时排空 `msg_queue` 与心跳/cron 输出,读到 EOF(Ctrl-D)退出。

#### Scenario: 非阻塞实时排空
- **WHEN** 飞书消息到达但用户未在 CLI 敲字
- **THEN** 下一轮循环顶部排空 `msg_queue`,消息被处理,无需 CLI 输入触发

#### Scenario: EOF 退出
- **WHEN** stdin 读到 EOF(Ctrl-D 或管道结束)
- **THEN** 主循环 `break`,停止心跳/cron/投递线程后退出程序(不抛异常)

### Requirement: Configuration & Workspace

系统 SHALL 从 `.env` 读取 `ANTHROPIC_API_KEY`(及可选 `ANTHROPIC_API_KEY_2..` 用于轮换)、`MODEL_ID`、`ANTHROPIC_BASE_URL`、`FEISHU_APP_ID`/`FEISHU_APP_SECRET`/`FEISHU_IS_LARK`/`FEISHU_BOT_OPEN_ID`、可选 `TELEGRAM_BOT_TOKEN`/`TELEGRAM_ALLOWED_CHATS`、`HEARTBEAT_INTERVAL`/`HEARTBEAT_ACTIVE_START`/`HEARTBEAT_ACTIVE_END`、`MEMORY_RETENTION_DAYS`。工作区文件 SHALL 位于 `workspace/`(SOUL/IDENTITY/TOOLS/USER/AGENTS/MEMORY/HEARTBEAT/BOOTSTRAP、CRON.json、`memory/daily/`、`.sessions/`),投递队列位于 `state/delivery/`。

#### Scenario: 无 API key 拒绝启动
- **WHEN** `ANTHROPIC_API_KEY` 未设置
- **THEN** `main()` 打印错误并 `sys.exit(1)`,不进入主循环
