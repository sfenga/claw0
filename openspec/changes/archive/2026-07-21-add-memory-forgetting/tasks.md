# Tasks: add-memory-forgetting

> 实现清单。注意:这是 spec-driven 工作流,apply 阶段照此勾选;三语版本(en/zh/ja)需同步实现同一 spec 的行为。

## 1. 配置与数据模型

- [x] 1.1 在 `.env.example` 新增 `MEMORY_RETENTION_DAYS=30` 及注释
- [x] 1.2 `MemoryStore.__init__` 读取 `MEMORY_RETENTION_DAYS`(int,默认 30),初始化遗忘计数器 `auto_expired=0`、`explicit_forgotten=0`

## 2. 写入路径(TTL 字段)

- [x] 2.1 `write_memory` 新增可选 `ttl_hours: float | None` 参数;非空时计算 `expires_at = now + ttl_hours` 并写入 entry
- [x] 2.2 `memory_write` 工具 schema 增加 `ttl_hours` 可选参数,透传给 `write_memory`
- [x] 2.3 更新 `memory_write` 工具 description,说明可用 TTL 写入会过期的临时记忆

## 3. 自动过期(懒清理)

- [x] 3.1 `_load_all_chunks` 加载前移除文件名日期早于 `today - MEMORY_RETENTION_DAYS` 的整个 daily 文件(文件级),累加 `auto_expired`
- [x] 3.2 加载单条 entry 时,若 `expires_at` 存在且早于 now,跳过该条(不计入 chunk),累加 `auto_expired`
- [x] 3.3 确保自动过期路径**不读不写** `MEMORY.md`(边界)

## 4. 显式遗忘工具 memory_forget

- [x] 4.1 实现 `MemoryStore.forget(category=None, date=None) -> int`:
  - `date` 给定 → 移除整个 `memory/daily/{date}.jsonl`,返回其条目数
  - `category` 给定 → 遍历所有 daily 文件,移除匹配 category 的条目,原子重写(临时文件 + rename),返回移除数
  - 二者均不给 → 抛错或返回 0(按 spec 场景约定)
- [x] 4.2 计数累加 `explicit_forgotten`;**不触及** `MEMORY.md`
- [x] 4.3 定义 `memory_forget` 工具 schema(`category`、`date` 均可选 string)与 handler `tool_memory_forget`
- [x] 4.4 将 `memory_forget` 注册进 `TOOLS` 分发表

## 5. 可观测性

- [x] 5.1 `get_stats` 输出增加 `auto_expired`、`explicit_forgotten` 字段
- [x] 5.2 现有 `daily_files`、`total_entries` 反映留存期内的余量(自动过期已移除超期文件后)

## 6. 多语言同步

> 仓库已精简为仅保留中文版本(en/ja 两套 sessions 已删除),故 en/ja 同步项不再适用。

- [x] 6.1 ~~将上述 1–5 的改动同步到 `sessions/en/s06_intelligence.py`~~ (N/A: en 已删除)
- [x] 6.2 ~~同步到 `sessions/ja/s06_intelligence.py`~~ (N/A: ja 已删除)
- [x] 6.3 对照 spec 场景自测(`sessions/zh`):写入带 TTL 的记忆→检索→过期后懒跳过;`memory_forget(date=)`、`memory_forget(category=)`;evergreen 不被触及;stats 反映计数

## 7. 验证(apply 后)

- [x] 7.1 `openspec validate add-memory-forgetting` 通过
- [x] 7.2 运行各语言 s06,手动触发场景验证行为
- [x] 7.3 退出探索模式后,经 `/opsx:apply` 勾选本清单实现
