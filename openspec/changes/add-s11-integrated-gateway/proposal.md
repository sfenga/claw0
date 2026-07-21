## Why

claw0 的 10 个章节是**渐进式教学切片**:每节只引入一个新概念,为聚焦本节主题会简化或省略其他层。结果是——

- s04 有飞书/Telegram 通道,但没有心跳、cron、投递、韧性、并发;
- s07/s10 有心跳/cron/并发,但退回 CLI、丢了飞书通道;
- s06 有完整记忆,但和通道、并发不在一起。

**没有任何一节能同时跑通「飞书消息进来 → 工具循环 → 记忆 → 心跳/cron → 可靠投递 → 并发车道 → 回复飞书」的端到端链路。** 用户在 s04 用飞书时已能收发消息,但要体验仓库的"完整功能"必须手工跨节拼装。s11 消除这个割裂:一个自包含文件,把全部 10 层按生产形态组装,可作为 claw0 的集大成演示与 OpenClaw 生产代码的阅读桥梁。

## What Changes

- 新增 `sessions/zh/s11_integrated.py`(自包含,~1400 行)与配套 `s11_integrated.md`,作为第 11 章。
- 复现(非 import)以下各层核心算法,并按生产形态接线:
  - **s01/s02**: `while True` + `stop_reason` 工具循环;dispatch table(bash/read_file/write_file/edit_file/list_directory/get_current_time + memory_write/memory_search/memory_forget)。
  - **s03**: `SessionStore`(JSONL 追加写、历史重建)+ `ContextGuard`(token 估算、tool_result 截断、LLM 摘要压缩)。
  - **s04**: `InboundMessage` + `Channel` ABC + `CLIChannel` + `FeishuChannel`(WebSocket 长连接入站、`receive_id_type` 按 `ou_`/`oc_` 前缀选型、`parse_ws_event`、`_parse_content`、`send` 经投递队列)+ `TelegramChannel`(可选,有 token 才长轮询)+ `ChannelManager`。
  - **s05**: `Binding`/`BindingTable` 五级(peer/guild/account/channel/default)+ `build_session_key`(dm_scope)+ `AgentConfig`/`AgentManager` + `resolve_route`。
  - **s06**: `SoulSystem`(8 层提示词文件)+ `build_system_prompt` + 完整 `MemoryStore`(evergreen + daily、混合检索、`memory_forget`、TTL 懒跳过、留存期整文件清理、stats)。
  - **s07**: `HeartbeatRunner`(`should_run` 前置、soul+memory 组装提示词)+ `CronService`(**croniter 真 cron 表达式**,从 CRON.json 加载,连续错误自动禁用)。
  - **s08**: `DeliveryQueue`(预写日志:先写盘再发、tmp+os.replace 原子写)+ 指数退避(+抖动)+ ack/fail/move-to-failed/retry + 通道感知分片;**回复一律经投递队列**,不直接 `channel.send`。
  - **s09**: `ProfileManager`(多 key 轮换、冷却)+ `classify_failure`(rate_limit/auth/timeout/billing/overflow/unknown)+ 三层重试洋葱(轮换→溢出压缩→工具循环)+ fallback models。
  - **s10**: `LaneQueue`(命名 FIFO、max_concurrency、generation 追踪、Future)+ `CommandQueue`(main/cron/heartbeat 车道);入站消息、心跳、cron 全部经各自车道串行化。
- `agent_loop` 启动:飞书 ws 线程 + (可选)Telegram 轮询线程 + 心跳线程 + cron tick 线程 + 投递 runner;非阻塞 stdin 实时排空 `msg_queue`;统一 REPL(`/channels /accounts /bindings /agents /sessions /soul /memory /search /forget /prompt /heartbeat /cron /lanes /queue /delivery /profiles /help`)。
- 非破坏性:纯新增章节,不改 s01–s10。

## Capabilities

### New Capabilities

- `integrated-gateway`: 把 claw0 已有的 10 个能力层(工具循环、会话、多通道、网关路由、智能记忆、心跳、cron、可靠投递、韧性、并发车道)集成为一个自包含可运行单元。本变更首次建立该 capability 的主 spec,作为各层在生产形态下如何接线的契约。

### Modified Capabilities

<!-- specs/ 当前仅有 memory;integrated-gateway 为全新建立,不修改既有 capability。 -->

## Impact

- **代码**: 新增 `sessions/zh/s11_integrated.py`(~1400 行,自包含)+ `sessions/zh/s11_integrated.md`。复现各节算法,不跨节 import。
- **配置**: `.env.example` 增 s11 相关项(多 key 轮换 `ANTHROPIC_API_KEY_2`、`HEARTBEAT_INTERVAL`/`HEARTBEAT_ACTIVE_START`/`HEARTBEAT_ACTIVE_END`、`MEMORY_RETENTION_DAYS`、`FEISHU_*`、可选 `TELEGRAM_BOT_TOKEN`)。
- **工作区**: `workspace/` 下 SOUL/IDENTITY/TOOLS/USER/AGENTS/MEMORY/HEARTBEAT/BOOTSTRAP、CRON.json、`memory/daily/`、`.sessions/`、投递队列目录 `state/delivery/`。
- **依赖**: `lark-oapi`(飞书长连接)、`croniter`(cron)、`httpx`(投递/飞书 HTTP)、`python-telegram-bot`(可选)。均已在 requirements.txt。
- **跨章关系**: s11 是集大成章,前置依赖 s01–s10 的概念;阅读 s11 前应已学完 s01–s10。s11 指向 OpenClaw 生产代码。
- **非目标(本次不做)**: 不实现 Telegram 富媒体分组合并(s04 才有);不实现 s05 的 WebSocket 网关服务器(只取其路由表);不重写 s08 的死信人工干预 UI;不为多 agent 做持久化路由(单 agent 默认绑定即可)。
