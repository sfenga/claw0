## Why

s06 的记忆系统只增不减:`memory/daily/*.jsonl` 永远追加,从不删除。已有的 `_temporal_decay` 只是**软遗忘**——在检索排序里给旧条目降权,但存储层什么都没移除。长期运行下记忆目录无界膨胀,且过期信息(如一次性提醒、已过期的偏好)永远占用检索带宽。本变更新增**硬遗忘**:真正从存储层移除过期记忆,让记忆具备留存控制能力。

## What Changes

- 给 daily 日志条目新增 `expires_at` 可选字段,写记忆时可指定 TTL。
- 新增自动过期清理:旧于配置天数 `MEMORY_RETENTION_DAYS` 的 daily 文件被移除(文件级,与 s03/s08 的追加写哲学一致)。
- 新增 `memory_forget` 工具:agent 可显式按 `category` 或 `date` 遗忘记忆,返回被移除的条目数。
- `memory_search` / `_load_all_chunks` 跳过已过期条目(懒清理)。
- 自动过期**永不触及** `MEMORY.md` evergreen 层。
- `get_stats` 输出遗忘计数(过期移除数、显式遗忘数)。

非破坏性变更;现有 `memory_write` / `memory_search` 行为保持不变,遗忘是叠加能力。

## Capabilities

### New Capabilities

- `memory`: 双层记忆存储(daily 日志 + evergreen 常驻事实)、混合检索(TF-IDF + 向量 + 时间衰减 + MMR),以及本次新增的硬遗忘能力(TTL 过期 + 显式遗忘 + 边界约束)。本变更首次建立该 capability 的主 spec,既固化现有行为,也追加遗忘需求。

### Modified Capabilities

<!-- specs/ 当前为空,无已有 capability 可修改;memory 为全新建立。 -->

## Impact

- **代码**: `sessions/{en,zh,ja}/s06_intelligence.py` 的 `MemoryStore` 类(写入、加载、检索、stats);新增 `memory_forget` 工具定义与 handler。
- **配置**: `.env.example` 新增 `MEMORY_RETENTION_DAYS`(默认 30)。
- **运行时数据**: `workspace/memory/daily/`(读时懒清理 + 可选后台清扫)。
- **跨章联动(不在本次 scope)**: 遗忘作为 s07 cron 作业是自然延伸,本次仅在 design 标注为 Future,不实现。
- **三语一致性**: 三种语言版本同步实现同一 spec;本变更的 spec 即跨语言等价契约。
