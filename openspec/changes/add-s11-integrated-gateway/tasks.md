# Tasks: add-s11-integrated-gateway

> 实现清单。spec-driven 工作流,apply 阶段照此勾选。s11 为 zh 单语言版本(仓库已精简为仅中文)。

## 1. 骨架与配置

- [ ] 1.1 新建 `sessions/zh/s11_integrated.py`:文件头 docstring + imports + `load_dotenv(..., override=True)` + 常量(MODEL_ID、WORKSPACE_DIR、STATE_DIR、dirs)
- [ ] 1.2 ANSI + print helpers(assistant/tool/info/warn/error/lane/channel/heartbeat/cron/delivery/resilience/session)
- [ ] 1.3 `colored_prompt()` 用 `\001/\002` 包裹(与 s04/s10 一致,修复退格)

## 2. 通道层 (s04)

- [ ] 2.1 `InboundMessage` dataclass + `Channel` ABC(receive/send/close)
- [ ] 2.2 `CLIChannel`(input/print)
- [ ] 2.3 `FeishuChannel`:httpx + tenant token 刷新;`_parse_content`(text/post/image);`parse_ws_event`;`_ws_bot_mentioned`;`start_long_connection`(lark.ws.Client 守护线程,HAS_LARK 守卫);`send` 按 `ou_`/`oc_` 前缀选 receive_id_type
- [ ] 2.4 `TelegramChannel`(可选,HAS_TG 守卫;长轮询 getUpdates + offset 持久化 + allowed_chats 过滤)推 InboundMessage 入 msg_queue
- [ ] 2.5 `ChannelManager`(register/get/list/close_all)

## 3. 工具层 (s01/s02/s03)

- [ ] 3.1 `safe_path`(阻止逃逸 WORKSPACE_DIR)
- [ ] 3.2 工具实现:bash / read_file / write_file / edit_file / list_directory / get_current_time
- [ ] 3.3 `TOOLS` schema 列表(含 memory_write/search/forget)+ `TOOL_HANDLERS` 分发 + `process_tool_call`

## 4. 会话与上下文 (s03)

- [ ] 4.1 `SessionStore`(create/load/save_turn/save_tool_result/append_transcript/_rebuild_history/list_sessions)
- [ ] 4.2 `ContextGuard`(estimate_tokens/truncate_tool_results/compact_history)

## 5. 智能与记忆 (s06)

- [ ] 5.1 `SoulSystem`(load SOUL 等 8 文件)+ `build_system_prompt`
- [ ] 5.2 `MemoryStore` 完整版:evergreen + daily + `write_memory(ttl_hours)` + `_purge_over_retention` + `_load_all_chunks` + 混合检索 `hybrid_search` + `forget(category,date)` + `get_stats`(含 auto_expired/explicit_forgotten)
- [ ] 5.3 记忆工具 handler:`tool_memory_write` / `tool_memory_search` / `tool_memory_forget`

## 6. 网关路由 (s05)

- [ ] 6.1 `Binding`/`BindingTable`(add/remove/resolve 五级)
- [ ] 6.2 `build_session_key`(dm_scope=per-peer)
- [ ] 6.3 `AgentConfig`/`AgentManager`(register/get_agent/get_session/list_sessions)
- [ ] 6.4 `resolve_route(bindings, mgr, channel, peer_id, ...)`
- [ ] 6.5 预装 tier-5 default 绑定 → agent `default`

## 7. 韧性 (s09)

- [ ] 7.1 `FailoverReason` + `classify_failure`
- [ ] 7.2 `AuthProfile` + `ProfileManager`(select/mark_failure/mark_success/list_profiles)
- [ ] 7.3 `ResilientAgent.run`:三层洋葱(轮换→溢出压缩→工具循环)+ fallback models + 统计;从 env 读多 key 构造 profiles

## 8. 可靠投递 (s08)

- [ ] 8.1 `QueuedDelivery` + `compute_backoff_ms`(指数+抖动)
- [ ] 8.2 `DeliveryQueue`(enqueue/原子写/ack/fail/move_to_failed/load_pending/load_failed/retry_failed)
- [ ] 8.3 `chunk_message`(通道限制)+ `DeliveryRunner`(后台线程:取 pending→chunk→deliver_fn→ack/fail)
- [ ] 8.4 出站回复走 `DeliveryQueue.enqueue`,deliver_fn = channel.send

## 9. 并发车道 (s10)

- [ ] 9.1 `LaneQueue`(enqueue/_pump/_run_task/_task_done/wait_for_idle/stats/generation)
- [ ] 9.2 `CommandQueue`(get_or_create_lane/enqueue/reset_all/wait_for_all/stats)
- [ ] 9.3 三车道 main/cron/heartbeat 初始化

## 10. 心跳与 cron (s07)

- [ ] 10.1 `HeartbeatRunner`(should_run/_build_heartbeat_prompt/heartbeat_tick/start/stop/drain_output/status),tick 入 LANE_HEARTBEAT
- [ ] 10.2 `CronService`(croniter 解析 CRON.json schedule.cron/cron_tick 入 LANE_CRON/list_jobs/连续5错禁用)

## 11. agent 回合与主循环接线

- [ ] 11.1 `run_agent_turn(inbound, sessions, mgr, ...)`:resolve_route→session_key→build_system_prompt(+auto recall)→ResilientAgent.run(tools)→DeliveryQueue.enqueue 回复
- [ ] 11.2 `agent_loop`:建 ChannelManager/CommandQueue/heartbeat/cron/delivery;启动飞书 ws + (可选)tg 轮询 + heartbeat + cron tick + delivery runner 线程
- [ ] 11.3 主循环:非阻塞 stdin(select 0.5s)+ 排空 msg_queue(入队 LANE_MAIN)+ 排空 heartbeat/cron 输出 + REPL 命令分发
- [ ] 11.4 REPL 命令全实现(/channels /accounts /bindings /agents /sessions /soul /prompt /memory /search /forget /heartbeat /trigger /cron /lanes /queue /enqueue /concurrency /generation /reset /delivery /profiles /help)
- [ ] 11.5 `main()`:无 key 拒启;调 agent_loop

## 12. 文档与验证

- [ ] 12.1 `sessions/zh/s11_integrated.md`(架构图 + 各层要点 + 试一试 + REPL 命令 + .env 项)
- [ ] 12.2 `.env.example` 增 s11 相关项(多 key 轮换、HEARTBEAT_*、MEMORY_RETENTION_DAYS 注释)
- [ ] 12.3 `py_compile` 通过
- [ ] 12.4 离线冒烟(车道/forget/留存期清理/chunk/backoff/resolve/classify/build_session_key)
- [ ] 12.5 `openspec validate add-s11-integrated-gateway` 通过
- [ ] 12.6 在线验证(需有效 LLM key):飞书私聊 → agent 回合 → 回复经投递队列 → 飞书收到
