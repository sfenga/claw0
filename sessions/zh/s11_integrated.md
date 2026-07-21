# 第11章: 集成网关(Integrated Gateway)

> 集大成——把前 10 章的每一层按生产形态接成一条可运行产线。

## 为什么有这一章

s01–s10 是**渐进式教学切片**:每节只引入一个新概念,为聚焦主题会省略其他层。结果——

- s04 有飞书/Telegram 通道,但没有心跳、cron、投递、韧性、并发;
- s07/s10 有心跳/cron/并发,但退回 CLI、丢了飞书;
- s06 有完整记忆,却和通道、并发不在一起。

**没有任何一节能同时跑通「飞书消息进来 → 工具循环 → 记忆 → 心跳/cron → 可靠投递 → 并发车道 → 回复飞书」的端到端链路。** s11 消除割裂:一个自包含文件,把全部 10 层按生产形态组装,既是 claw0 的集大成演示,也是阅读 OpenClaw 生产代码的桥梁。

## 架构

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
```

## 本章要点

- **工具循环(s01/s02)**:`while True` + `stop_reason`。`tool_use` → dispatch table 执行 → 回填 `tool_result` → 续循环;`end_turn` → 输出文本。工具:bash / read_file / write_file / edit_file / list_directory / get_current_time + memory_write / memory_search / memory_forget。
- **会话(s03)**:`SessionStore` JSONL 追加写,按 `build_session_key` 隔离(默认 per-peer:不同飞书用户各自独立会话)。`ContextGuard` 在溢出时截断 tool_result + LLM 摘要压缩前 50%(保留近 20%)。
- **通道(s04)**:`InboundMessage` 归一化 CLI + 飞书(WebSocket 长连接,`lark-oapi` 出站连接) + Telegram(长轮询,可选)。共享 `msg_queue` + `q_lock`。飞书 `send` 按 `ou_`/`oc_` 前缀选 `receive_id_type`(私聊=open_id,群=chat_id)。
- **网关(s05)**:`BindingTable` 五级绑定(peer / guild / account / channel / default,越小越具体),第一个匹配获胜。预装 tier-5 default → 单 agent 开箱即用;REPL 可加更具体绑定。
- **智能(s06)**:`BootstrapLoader` 装 8 个提示词文件(SOUL/IDENTITY/TOOLS/USER/AGENTS/MEMORY/HEARTBEAT/BOOTSTRAP)→ `build_system_prompt` 8 层组装;组装时做记忆 **auto-recall**(用入站文本检索 top-k 注入)。`MemoryStore` 完整:evergreen + daily、混合检索(keyword+vector+时间衰减+MMR)、`memory_forget`、TTL 懒跳过、留存期整文件清理。
- **心跳(s07)**:`HeartbeatRunner` 跑在 `LANE_HEARTBEAT`。`should_run` 检查 `HEARTBEAT.md` 存在且非空、间隔已到、在 active_hours 内;用 soul+memory 组装提示词跑一轮 agent。lane 忙则跳过该 tick(非阻塞)。
- **cron(s07)**:`CronService` 用 **croniter 真 cron 表达式**(如 `0 9 * * *`)从 `CRON.json` 加载,每秒 tick 检查到期则入 `LANE_CRON`。连续 5 次错误自动禁用。无 croniter 时退化为 `every_seconds`。
- **投递(s08)**:回复**一律经 `DeliveryQueue`**(预写日志:tmp+`os.replace` 原子写盘→再发),不直接 `channel.send`。失败指数退避(+/-20% 抖动),重试耗尽移入 `failed/`。`chunk_message` 按通道限制分片。
- **韧性(s09)**:`ResilientAgent` 三层重试洋葱包裹每次 `messages.create`:L1 `ProfileManager` 多 key 轮换(按 `classify_failure` 分 rate_limit/auth/timeout/billing/overflow/unknown 置冷却);L2 溢出时 `ContextGuard` 截断+压缩后用同 profile 重试;L3 工具循环。所有 profile 耗尽尝试 fallback models。
- **并发(s10)**:`LaneQueue`(命名 FIFO、`max_concurrency`、generation 追踪、`Future`)三车道 main/cron/heartbeat。入站、心跳、cron 经 `CommandQueue.enqueue(lane, fn)` 路由,各自串行化。`reset_all` 递增 generation,使旧任务不重新泵送。

## 代码要点

### run_agent_turn —— 集成接线核心

一条飞书/CLI 消息的生命周期:

```python
def run_agent_turn(inbound, mgr, bindings, soul_text, bootstrap,
                   memory_store, resilient, delivery, tool_handlers):
    # 1. 网关路由: (channel, peer) -> agent_id + session_key
    agent_id, session_key = resolve_route(bindings, mgr, inbound.channel, inbound.peer_id, ...)
    messages = mgr.get_session(session_key)
    messages.append({"role": "user", "content": inbound.text})

    # 2. 智能: auto-recall + 8 层系统提示词
    recall = memory_store.hybrid_search(inbound.text, top_k=3)
    system_prompt = build_system_prompt(mode="full", bootstrap=bootstrap,
                                       memory_context=..., agent_id=agent_id, channel=inbound.channel)

    # 3. 韧性三层洋葱: 轮换/溢出压缩/工具循环
    response, messages = resilient.run(system_prompt, messages, TOOLS)

    # 4. 投递: 经投递队列(崩溃不丢, 失败退避)
    text = "".join(b.text for b in response.content if hasattr(b, "text"))
    delivery.enqueue(inbound.channel, inbound.peer_id, text)
```

### 韧性三层洋葱

```python
# L1 轮换: 多 key 间切换, 失败按类别置冷却
for _ in range(len(profiles)):
    profile = pm.select_profile()
    api_client = Anthropic(api_key=profile.api_key, base_url=profile.base_url)
    # L2 溢出压缩: 截断 tool_result + LLM 摘要, 用同 profile 重试
    for attempt in range(MAX_OVERFLOW_COMPACTION):
        try:
            # L3 工具循环
            return self._run_attempt(api_client, model, system, messages, tools)
        except Exception as exc:
            reason = classify_failure(exc)
            if reason == FailoverReason.overflow: ...  # 压缩重试
            else: pm.mark_failure(profile, reason, ...); break  # 换下一个 profile
```

### 非阻塞主循环

```python
while True:
    for m in heartbeat.drain_output(): print_lane("heartbeat", m)
    for m in cron.drain_output():     print_lane("cron", m)
    # 排空入站 -> 入队 LANE_MAIN (不必等 CLI 敲字)
    with q_lock:
        for inbound in list(msg_queue):
            cq.enqueue(LANE_MAIN, lambda ib=inbound: run_agent_turn(ib, ...))
    # 非阻塞 stdin; EOF(Ctrl-D)退出
    if not select.select([sys.stdin], [], [], 0.5)[0]:
        continue
    line = sys.stdin.readline()
    if line == "": break
    ...
```

## 试一试

```sh
# CLI + 飞书(在 .env 配 ANTHROPIC_API_KEY, MODEL_ID, FEISHU_APP_ID, FEISHU_APP_SECRET)
.venv/bin/python sessions/zh/s11_integrated.py

# 多 key 轮换(韧性): 在 .env 加 ANTHROPIC_API_KEY_2=...
# 心跳: 在 workspace/HEARTBEAT.md 写指令; 设 HEARTBEAT_INTERVAL / HEARTBEAT_ACTIVE_START / _END
# cron: 在 workspace/CRON.json 写 jobs (schedule.cron = "0 9 * * *")
# 记忆硬遗忘: /forget date=2026-07-01 | /forget category=reminder

# REPL 命令
# You > /channels                 (已注册通道: 应含 feishu)
# You > /lanes                    (main/cron/heartbeat 三车道状态)
# You > /heartbeat                (心跳状态)
# You > /cron                     (cron 作业)
# You > /memory                   (记忆统计, 含 auto_expired/explicit_forgotten)
# You > /profiles                 (认证 profile 轮换状态)
# You > /delivery                 (投递队列 pending/failed)
```

## 配置(.env)

| 变量 | 必需 | 说明 |
|------|------|------|
| `ANTHROPIC_API_KEY` | 是 | 主 LLM key(韧性 Layer1 主 profile) |
| `MODEL_ID` | 是 | 模型 id |
| `ANTHROPIC_BASE_URL` | 否 | 兼容服务商 base url |
| `ANTHROPIC_API_KEY_2`/`_3` | 否 | 额外 key,用于轮换故障转移 |
| `FEISHU_APP_ID`/`APP_SECRET` | 否 | 飞书自建应用凭证(长连接入站) |
| `FEISHU_IS_LARK` | 否 | true=国际版 Lark,否则国内飞书 |
| `FEISHU_BOT_OPEN_ID` | 否 | 群聊仅 @ 时响应 |
| `TELEGRAM_BOT_TOKEN` | 否 | 启用 Telegram 长轮询 |
| `TELEGRAM_ALLOWED_CHATS` | 否 | TG 白名单 |
| `HEARTBEAT_INTERVAL`/`ACTIVE_START`/`ACTIVE_END` | 否 | 心跳间隔与活跃时段 |
| `MEMORY_RETENTION_DAYS` | 否 | 记忆留存期(默认 30 天整文件清理) |

## 工作区(workspace/)

```
SOUL.md  IDENTITY.md  TOOLS.md  USER.md  AGENTS.md
MEMORY.md  HEARTBEAT.md  BOOTSTRAP.md  CRON.json
memory/daily/*.jsonl      # daily 记忆(TTL + 留存期清理)
.sessions/                # 会话 JSONL
state/delivery/           # 投递队列(pending + failed/)
state/telegram/offset-*   # Telegram offset 持久化
```

## 与 OpenClaw 的对照

| 观点 | claw0 s11 | OpenClaw 生产 |
|------|-----------|---------------|
| 通道 | CLI + 飞书 + Telegram | 10+ 平台 + 生命周期钩子 |
| 路由 | BindingTable 五级(内存) | 持久化绑定表 + 多 agent |
| 记忆 | hybrid_search 纯 Python | 向量库 + 真实 embedding |
| 投递 | 单机 WAL + 退避 | 分布式队列 + 死信干预 UI |
| 韧性 | 3 层洋葱(多 key) | 多 provider + 多区域 |
| 并发 | 命名车道(线程 + Future) | 异步网关 + 车道池 |
