## Context

s06 的 `MemoryStore` 采用双层结构:evergreen 常驻层(`MEMORY.md`,手动维护)与 daily 日志层(`memory/daily/{date}.jsonl`,append-only JSONL)。检索管线 `_temporal_decay` 已实现**软遗忘**——在排序层给旧条目降权,但不触及存储。问题:daily 层只增不减,长期运行无界膨胀,过期信息(临时提醒、已过期偏好)持续占用检索带宽。

约束:claw0 是教学仓库,s03(会话)与 s08(投递)都布道"追加写 + WAL、不原地改文件"的哲学;三语(en/zh/ja)版本需同步实现同一行为。本变更为 `memory` capability 首次建立主 spec,既固化现有行为,也追加硬遗忘。

## Goals / Non-Goals

**Goals:**
- 真正从存储层移除过期记忆(硬遗忘),与现有软遗忘区分清晰。
- 与 claw0 追加写哲学一致:文件级过期为主,不破坏 JSONL append-only 性质。
- 自动过期(TTL + 留存期)与显式遗忘(`memory_forget`)两条路径并存。
- 非破坏性:现有 `memory_write` / `memory_search` 行为不变。
- 三语同步、可观测(遗忘计数)。

**Non-Goals:**
- 条目级 TTL 过期(单条重写 JSONL 文件)——破坏 append-only,与 s03/s08 哲学冲突;留作 Future。
- 墓碑(tombstone)软删或归档移走——增加复杂度且与"遗忘应干净"相悖;不做。
- 跨语言等价性的自动校验脚手架——属另一变更。
- 将遗忘接入 s07 cron 后台清扫——属跨章扩展,本次不实现(见 Open Questions)。

## Decisions

### D1: 文件级过期为主,条目级列为 Future

**选择**: 留存期过期以**整个 daily 文件**为粒度(`{date}.jsonl` 旧于 `MEMORY_RETENTION_DAYS` 即移除);`expires_at` 的单条级过期仅做**懒跳过**(读时跳过该条,不重写文件)。

**理由**: claw0 在 s03/s08 反复强调"追加写、崩溃不丢消息、不原地改文件"。条目级硬删除需重写整个 JSONL,直接违背这一哲学,且对教学仓库而言复杂度与教学收益不匹配。文件级删除是整文件移除(原子、简单),与 WAL 思路一致;单条 `expires_at` 走懒跳过,既有单条 TTL 的教学价值,又不破坏 append-only。

**备选**: 条目级硬删除(重写文件)——更真实但复杂、与哲学冲突,列为 Future。

### D2: 触发模型 = TTL 懒清理 + 留存期(自动) + memory_forget(显式)

**选择**: 双触发路径。
- 自动:读时懒清理(跳过 `expires_at` 过期条目;移除超留存期的整文件)。
- 显式:`memory_forget(category?, date?)` 工具,agent 主动遗忘。

**理由**: 覆盖"该忘的自动忘"+"用户/agent 知道该忘什么"两类场景。容量上限(N 条)列为 Future,避免本次 spec 过载;留存期(天数)已能控制膨胀。

**备选**: 容量上限裁剪(保留最新 N 条)——真实但需排序+重写,列入 Future;后台 cron 清扫——见 D3。

### D3: 不接 s07 cron(本次),标注 Future

**选择**: 本次仅做读时懒清理,不实现后台 cron 清扫作业。

**理由**: 接 cron 是跨章变更(s06+s07),虽是"遗忘作为定时维护"的最美教学联动,但会显著扩大 scope、拖慢闭环。在 design 显式记录该方向,作为后续自然延伸。

### D4: 显式遗忘按 category 需重写文件(唯一例外)

**选择**: `memory_forget(category=...)` 必须从多个 daily 文件中删除匹配条目并重写剩余条目回文件。

**理由**: 这是 append-only 哲学的**有意例外**——显式遗忘是 agent 的主动、有意识行为(非崩溃路径),其语义就是"删掉这些条目"。与自动过期(只删整文件、不重写)形成清晰对照:自动路径保持 append-only,显式路径才允许重写。这种区分本身是教学点。

**备选**: 仅支持 `memory_forget(date=...)`(整文件),不支持 category——更纯粹但实用性差,放弃。

### D5: evergreen 层绝对隔离

**选择**: 任何自动/显式遗忘均不得触及 `MEMORY.md`。

**理由**: evergreen 是用户手动维护的常驻事实,语义即"永不过期"。自动机制碰它是设计错误。直接写为 spec 的硬约束(见 `Requirement: 自动遗忘边界`)。

## Risks / Trade-offs

- **[条目级 TTL 仅懒跳过、不主动删]** → 文件不会因单条过期而缩小,只会在留存期到点时整文件删除。可接受:留存期是最终的体积控制阀。
- **[显式按 category 遗忘重写文件,中途崩溃可能丢条目]** → 缓解:先写临时文件再原子 rename(与 s08 预写思路一致);本设计要求实现遵循此模式。
- **[默认留存期 30 天可能过短/过长]** → 缓解:走环境变量 `MEMORY_RETENTION_DAYS`,用户可调;默认值在教学场景偏保守即可。
- **[三语实现可能漂移]** → 缓解:spec 即契约,apply 阶段三语对照同一 spec 实现;但本设计不引入自动等价校验(Non-Goal)。

## Migration Plan

非破坏性叠加:现有 daily 文件无 `expires_at` 字段时按"永不过期"处理(懒跳过逻辑对缺失字段视为未过期),留存期到期才整文件删除。无需迁移历史数据。回滚:移除新工具、还原 `MemoryStore`,现有记忆文件不受影响。

## Open Questions

- 遗忘接入 s07 cron 的具体形态(cron payload kind? 复用 `agent_turn` 还是新增 `memory_gc`?留待该后续变更解决)。
- 容量上限(N 条)是否需要在 cron 联动变更中一并引入。
