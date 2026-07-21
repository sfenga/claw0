# Memory Capability Specification

> 双层记忆存储 + 混合检索 + 硬遗忘。本 spec 首次建立:既固化 s06 现有行为,也追加本次变更的遗忘需求。

## ADDED Requirements

### Requirement: 双层记忆存储

系统 SHALL 维护两层记忆:evergreen 常驻层(`MEMORY.md`,手动维护的长期事实)与 daily 日志层(`memory/daily/{YYYY-MM-DD}.jsonl`,agent 通过 `memory_write` 工具自动追加写入)。daily 日志层 SHALL 为追加写(append-only),单条 entry 格式为 JSON `{ts, category, content, expires_at?}`,其中 `expires_at` 为可选 ISO-8601 时间戳。

#### Scenario: 写入一条无 TTL 的记忆
- **WHEN** agent 调用 `memory_write(content="用户偏好简洁回答", category="preference")` 且当日文件不存在
- **THEN** 系统在 `memory/daily/{今日}.jsonl` 追加一行 JSON,包含 `ts`、`category`、`content`,且**不含** `expires_at` 字段

#### Scenario: 写入一条带 TTL 的记忆
- **WHEN** agent 调用 `memory_write(content="临时提醒:周三开会", category="reminder", ttl_hours=48)`
- **THEN** 系统追加的 entry 包含 `expires_at` 字段,其值为写入时刻 + 48 小时的 ISO-8601 时间戳

### Requirement: 混合检索带软遗忘

系统 SHALL 提供混合检索(`memory_search`),管线为 keyword + vector → merge → 时间衰减 → MMR,返回 top-k。时间衰减 SHALL 在**排序**层给旧条目降权,但 SHALL NOT 修改或删除任何存储条目。

#### Scenario: 软遗忘不影响存储
- **WHEN** 检索一条 90 天前的记忆
- **THEN** 该条目因时间衰减在结果中排名靠后或被挤出 top-k,但其 JSONL 文件与条目内容保持原样未变

### Requirement: 自动过期清理(TTL 与留存期)

系统 SHALL 在读取 daily 记忆时进行懒清理:单条 entry 的 `expires_at` 早于当前时刻时,该条目 SHALL 被跳过且不计入检索。系统 SHALL 移除文件名日期早于当前日期减去 `MEMORY_RETENTION_DAYS`(环境变量,默认 30)的整个 daily 文件。自动过期清理 SHALL 仅作用于 `memory/daily/` 目录。

#### Scenario: 懒跳过已过期条目
- **WHEN** 某条 entry 的 `expires_at` 已过,agent 调用 `memory_search` 检索其内容
- **THEN** 该条目不出现在检索结果中

#### Scenario: 超期 daily 文件被移除
- **WHEN** `MEMORY_RETENTION_DAYS=30` 且存在文件 `memory/daily/2025-01-01.jsonl`,当前日期为 2026-07-15
- **THEN** 该文件在下次记忆加载时被从磁盘移除,其所有条目不再可检索

### Requirement: 自动遗忘边界——永不触及 evergreen 层

系统 SHALL NOT 对 `MEMORY.md` evergreen 层执行任何自动过期、移除或裁剪。任何自动过期机制 SHALL 仅限于 `memory/daily/` 目录下的文件。

#### Scenario: evergreen 在留存期外仍保留
- **WHEN** `MEMORY.md` 存在内容且所有 daily 文件均已超期被移除
- **THEN** `load_evergreen()` 仍返回完整的 `MEMORY.md` 内容,且 `memory_search` 仍能在 evergreen 段落中检索

### Requirement: 显式遗忘工具 memory_forget

系统 SHALL 提供 `memory_forget` 工具,允许 agent 显式遗忘记忆,接受 `category`(可选)与 `date`(可选,YYYY-MM-DD)参数,返回被移除的条目数。`memory_forget` 作用域为 daily 日志层;当提供 `date` 时 SHALL 移除对应的整个 daily 文件;当仅提供 `category` 时 SHALL 从所有 daily 文件中移除匹配该 category 的条目(以重写文件方式)。`memory_forget` SHALL NOT 触及 evergreen 层。

#### Scenario: 按日期遗忘整文件
- **WHEN** agent 调用 `memory_forget(date="2026-07-01")` 且该日文件存在 5 条记忆
- **THEN** 系统移除 `memory/daily/2026-07-01.jsonl` 并返回 "Forgot 5 entries from 2026-07-01"

#### Scenario: 按 category 遗忘跨文件条目
- **WHEN** agent 调用 `memory_forget(category="reminder")` 且 reminder 条目分布在 3 个 daily 文件共 7 条
- **THEN** 系统从这 3 个文件中移除全部 reminder 条目(保留其余条目重写回文件)并返回 "Forgot 7 entries (category=reminder)"

#### Scenario: 显式遗忘不触及 evergreen
- **WHEN** agent 调用 `memory_forget(category="preference")` 而 `MEMORY.md` 含 preference 段落
- **THEN** daily 层匹配条目被移除,但 `MEMORY.md` 的 preference 段落保持不变

### Requirement: 遗忘可观测

系统的 `get_stats` SHALL 输出遗忘计数,至少包含:本次进程内自动过期移除的条目数、显式 `memory_forget` 移除的条目数,以及当前 daily 文件数与总条目数。

#### Scenario: stats 反映遗忘活动
- **WHEN** 自启动以来自动过期移除了 12 条、显式遗忘了 3 条
- **THEN** `get_stats` 返回的字典中 `auto_expired=12`、`explicit_forgotten=3`、`daily_files` 与 `total_entries` 反映当前在留存期内的余量
