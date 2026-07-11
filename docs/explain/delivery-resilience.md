# 第 08、09 节详解：投递（Delivery）与韧性（Resilience）

> s08：先写磁盘，再尝试发送——崩溃也丢不了消息。
> s09：一次调用失败，轮换重试——三层洋葱包裹每次 agent 执行。

本文是对 `sessions/zh/s08_delivery.py`（869 行）、`s09_resilience.py`（1126 行）及两份配套 `.md` 的逐层深读。这两节是 claw0 的**可靠性层**，放在一起讲是因为它们解决同一类问题——「**失败时怎么办**」——但分别针对**出站投递**（s08）和**入站调用**（s09）。

---

## 0. 一句话定位

| 节 | 解决什么 | 核心机制 |
|----|---------|---------|
| **s08 投递** | agent 的回复/心跳/cron 输出**发出去时**失败或进程崩溃，消息不丢 | 预写磁盘队列 + 退避重试 + 启动恢复 |
| **s09 韧性** | agent **调 LLM 时**失败（限流/鉴权/超时/溢出），自动恢复 | 三层重试洋葱（轮换 key → 压缩历史 → 工具循环） |

它们都围绕「失败」，但方向相反：

```
s08 出站方向:  agent 产出 ──► [队列] ──► 投递到平台(可能失败 → 重试)
s09 入站方向:  用户消息 ──► [洋葱] ──► 调 LLM(可能失败 → 轮换/压缩/重试)
```

一句话：**s08 保证「agent 说的话不丢」，s09 保证「agent 能把话说完」**。

---

# 第一部分：s08 投递（Delivery）

## s08-1. 架构总览

```
Agent 回复 / 心跳 / Cron
          |
    chunk_message()          ← 按平台限制分片(telegram=4096, discord=2000...)
          |
    DeliveryQueue.enqueue()  ← 写入磁盘(预写日志)
       1. 生成唯一 id
       2. 写 .tmp.{pid}.{id}.json
       3. fsync()             ← 数据落盘
       4. os.replace() → {id}.json   ← 原子换名(WRITE-AHEAD)
          |
    DeliveryRunner (后台线程, 1s 扫描)
          |
    deliver_fn(channel, to, text)
       /          \
    success      failure
      |             |
    ack()         fail()
    (删 .json)    (retry_count++, 算退避, 更新磁盘)
                      |
                retry_count >= 5?
                   |yes
                 移到 failed/

退避: [5s, 25s, 2min, 10min] 带 +/-20% 抖动
```

核心组件：

| 组件 | 代码位置 | 职责 |
|------|---------|------|
| `QueuedDelivery` | `:122-156` | 单条投递条目的数据结构 |
| `DeliveryQueue` | `:176-303` | 磁盘持久化队列：enqueue/ack/fail/load_pending |
| `compute_backoff_ms` | `:159-166` | 指数退避 + 抖动 |
| `chunk_message` | `:319-336` | 按平台限制分片 |
| `DeliveryRunner` | `:343-435` | 后台投递线程 + 启动恢复 |
| `MockDeliveryChannel` | `:442-459` | 模拟投递渠道（可调失败率，教学用） |
| `HeartbeatRunner` | `:552-616` | s07 心跳的简化版，输出入队而非直接 print |

## s08-2. 核心原则：预写日志（WAL）

整个 s08 建立在一个原则上：**先写磁盘，再尝试发送**。

为什么不直接发？因为发送可能失败、进程可能崩溃。如果「agent 生成回复 → 直接发送」之间进程崩了，这条回复就丢了——用户问完没等到回复，agent 重启也不会重发（它不知道有这条没发出去的）。

预写日志的解法：**在真正发送之前，先把「这条消息要发给谁、内容是什么」写进磁盘队列**。这样：

- 写入成功后，无论何时崩，消息都在磁盘上；
- 崩后重启，扫描磁盘队列，把没发完的重新发。
- 真正发送在后台线程慢慢做，失败就重试。

这和数据库的 WAL（Write-Ahead Log）思想完全一致——**先持久化意图，再执行动作**，动作失败可重试，意图不会丢。s08 把这个原则用到「消息投递」上。

## s08-3. 原子写入：三步保证崩溃安全

`DeliveryQueue._write_entry`（`:198-207`）是 s08 最精细的部分：

```python
def _write_entry(self, entry: QueuedDelivery) -> None:
    final_path = self.queue_dir / f"{entry.id}.json"
    tmp_path = self.queue_dir / f".tmp.{os.getpid()}.{entry.id}.json"

    data = json.dumps(entry.to_dict(), indent=2, ensure_ascii=False)
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())        # 第2步: 数据落盘

    os.replace(str(tmp_path), str(final_path))  # 第3步: 原子换名
```

**为什么这么绕？直接 `open(final_path, "w").write(data)` 不行吗？** 因为那会**半写文件**——如果写到一半进程崩了，`{id}.json` 就是个残缺的 JSON，后续读它会解析失败，消息既没发出去也无法重试（坏数据）。

三步设计避开这个坑：

**第 1 步：写临时文件 `.tmp.{pid}.{id}.json`**
- 文件名带 pid + id，避免多进程/多线程并发写时撞名。
- 此时 `final_path`（`{id}.json`）还不存在或还是旧的——崩溃的话，留下个孤立的 tmp 文件，**无害**（不会假装成功）。

**第 2 步：`f.flush()` + `os.fsync(f.fileno())`**
- `flush()` 把 Python 缓冲区刷到操作系统。
- `fsync()` 强制操作系统把页缓存**真正写到磁盘硬件**——`flush` 之后数据可能还在 OS 缓存里（断电会丢），`fsync` 才保证落盘。
- 这两步分开是因为它们作用于不同层：Python 缓冲 → OS 缓存 → 磁盘硬件，要逐层强制。

**第 3 步：`os.replace(tmp, final)`**
- 这是 POSIX 的**原子操作**（`rename(2)`）——要么整个换名成功，要么没换，**不存在「换名一半」**。
- 崩在这步之前：`final` 还是旧的（或不存在），tmp 是新的完整文件——下次启动看到 tmp 孤立、`final` 没这条，知道这条没「提交」，重新处理。
- 崩在这步之后：`final` 是新文件，完整——已提交。

**核心保证**：`final_path` 要么不存在，要么是**完整的** JSON——绝不会有半写的残缺文件。这就是「崩溃安全」。

> 注意 `load_pending`（`:255`）只 `glob("*.json")`，不读 `.tmp.*`——所以孤立的 tmp 文件会被忽略，不污染队列。这是配套设计。

## s08-4. QueuedDelivery：条目数据结构

```python
@dataclass
class QueuedDelivery:
    id: str               # uuid 前 12 位, 文件名也用它
    channel: str          # 发到哪个渠道(telegram/discord/console)
    to: str               # 发给谁(peer_id)
    text: str             # 内容
    retry_count: int = 0  # 已重试次数
    last_error: str | None = None   # 上次失败原因
    enqueued_at: float = field(default_factory=time.time)   # 入队时间
    next_retry_at: float = 0.0     # 下次重试时间戳(0=立即)
```

带 `to_dict`/`from_dict` 做序列化——磁盘存的就是这些字段。`next_retry_at` 是关键：失败后算退避、设它为「现在+退避秒数」，后台线程只处理「到了重试时间」的条目。`enqueued_at` 用于排序（FIFO）。

## s08-5. 投递生命周期：ack / fail

```python
def ack(self, delivery_id):          # 成功
    (self.queue_dir / f"{delivery_id}.json").unlink()   # 删文件=完成

def fail(self, delivery_id, error):  # 失败
    entry = self._read_entry(delivery_id)
    entry.retry_count += 1
    entry.last_error = error
    if entry.retry_count >= MAX_RETRIES:     # 5 次
        self.move_to_failed(delivery_id)     # 移到 failed/ 目录(留档, 不再重试)
        return
    backoff_ms = compute_backoff_ms(entry.retry_count)
    entry.next_retry_at = time.time() + backoff_ms / 1000.0   # 设下次重试时间
    self._write_entry(entry)        # 把新状态写回磁盘(原子写)
```

- **ack**：成功就删文件——文件不存在 = 这条发完了，队列自然变短。简单干净。
- **fail**：递增重试计数、记错误、算退避、更新 `next_retry_at`、**原子写回磁盘**（同样三步）。超过 5 次就 `move_to_failed`——`os.replace` 把文件从 `queue/` 挪到 `queue/failed/`，不再参与重试但**留档**（可 `/retry` 手动重新入队）。

## s08-6. 退避：指数增长 + 抖动防惊群

```python
BACKOFF_MS = [5_000, 25_000, 120_000, 600_000]   # [5s, 25s, 2min, 10min]
MAX_RETRIES = 5

def compute_backoff_ms(retry_count):
    if retry_count <= 0:
        return 0
    idx = min(retry_count - 1, len(BACKOFF_MS) - 1)   # 超出数组取最后一个
    base = BACKOFF_MS[idx]
    jitter = random.randint(-base // 5, base // 5)    # +/- 20%
    return max(0, base + jitter)
```

两个要点：

**① 指数增长**：5s → 25s → 2min → 10min。失败越多等越久——给故障恢复时间，不狂试。`min(idx, len-1)` 让第 5 次（idx=4）也用 10min（数组只到 idx=3）。

**② 抖动（jitter）**：`+/-20%` 随机偏移。**防惊群效应（thundering herd）**——如果多个失败任务同时入队，没抖动的话它们会在完全相同的时刻重试（都等 5s），瞬间又把下游打挂；抖动让重试时间分散开，错峰重试。这是分布式系统重试的标准技巧。

`max(0, ...)` 兜底，防止抖动让等待变负。

## s08-7. chunk_message：按平台限制分片

```python
CHANNEL_LIMITS = {"telegram": 4096, "telegram_caption": 1024,
                  "discord": 2000, "whatsapp": 4096, "default": 4096}

def chunk_message(text, channel="default"):
    if len(text) <= limit:
        return [text]
    chunks = []
    for para in text.split("\n\n"):          # 先按段落
        if chunks and len(chunks[-1]) + len(para) + 2 <= limit:
            chunks[-1] += "\n\n" + para        # 还能塞就合并进上一块
        else:
            while len(para) > limit:          # 单段超长就硬切
                chunks.append(para[:limit])
                para = para[limit:]
            if para:
                chunks.append(para)
    return chunks or [text[:limit]]
```

每个平台对单条消息长度有限制（Telegram 4096、Discord 2000）。超长回复要先分片再逐条入队投递。

分片策略**两级**：先按段落（`\n\n`）切，尽量在段落边界合并/拆分，不劈断段落；单段超长才硬切。这保证可读性——不会把一句话从中间劈开。

这和 s04 TelegramChannel 的 `_chunk` 同构——s08 把分片抽成独立函数、按渠道表配置化，更通用。

## s08-8. DeliveryRunner：后台投递线程

```python
class DeliveryRunner:
    def start(self):
        self._recovery_scan()                    # 启动时先扫描恢复
        self._thread = threading.Thread(target=self._background_loop, daemon=True, ...)
        self._thread.start()

    def _background_loop(self):
        while not self._stop_event.is_set():
            try:
                self._process_pending()
            except Exception as exc:
                print_error(...)                 # 单轮异常不杀线程
            self._stop_event.wait(timeout=1.0)   # 每秒扫一次

    def _process_pending(self):
        pending = self.queue.load_pending()      # 扫磁盘队列目录
        now = time.time()
        for entry in pending:
            if entry.next_retry_at > now:        # 没到重试时间, 跳过
                continue
            self.total_attempted += 1
            try:
                self.deliver_fn(entry.channel, entry.to, entry.text)
                self.queue.ack(entry.id)         # 成功 → 删
                self.total_succeeded += 1
            except Exception as exc:
                self.queue.fail(entry.id, str(exc))   # 失败 → 退避+计数
                self.total_failed += 1
```

三个要点：

**① 启动恢复扫描** `_recovery_scan`：进程启动时先 `load_pending` + `load_failed`，统计「上次崩溃遗留了多少没发/失败的」。这是 s08 的崩溃恢复核心——**重启后磁盘队列还在，自动接着发**。打印「Recovery: 3 pending」让你知道恢复了什么。

**② 每秒扫描 + 到期才发**：`load_pending` 扫所有 `.json`，但只处理 `next_retry_at <= now` 的——没到重试时间的跳过。这样退避机制生效：刚失败的要等退避时间到才再试。

**③ 单轮异常不杀线程**：`_background_loop` 的 `try/except` 兜住 `_process_pending` 的任何异常——投递循环必须**永生**，一个坏条目不能搞停整个投递。

**`deliver_fn` 是注入的**：`agent_loop` 里定义 `deliver_fn = lambda ch,to,text: mock_channel.send(to, text)`（`:731`），把具体怎么发外置。生产里会换成真正的 `TelegramChannel.send` / `FeishuChannel.send`。s08 用 `MockDeliveryChannel` 模拟（可调 50% 失败率），让你能观察重试而不需要真连平台。

## s08-9. agent_loop 集成：所有出站都走队列

```python
# 心跳输出 → 入队
class HeartbeatRunner:
    def trigger(self):
        heartbeat_text = f"[Heartbeat #{self.run_count}] ..."
        chunks = chunk_message(heartbeat_text, self.channel)
        for chunk in chunks:
            self.queue.enqueue(self.channel, self.to, chunk)   # 入队, 不直接 print

# 用户对话回复 → 入队
if response.stop_reason == "end_turn":
    assistant_text = "".join(b.text for b in response.content if hasattr(b, "text"))
    print_assistant(assistant_text)                             # 本地立刻显示
    chunks = chunk_message(assistant_text, default_channel)
    for chunk in chunks:
        queue.enqueue(default_channel, default_to, chunk)       # 同时入队投递
```

关键变化：**所有出站消息（agent 回复、心跳、cron）都不直接发，而是 `queue.enqueue` 入队**，由 `DeliveryRunner` 后台投递。这是 s08 相对 s07 的核心升级——s07 的心跳输出只进内存队列 print 到 REPL，s08 让它进**磁盘持久化队列**，崩溃不丢、失败重试。

注意 s08 同时 `print_assistant`（本地立刻显示）和 `enqueue`（持久投递）——本地交互即时、远端投递可靠，两路并存。

## s08-10. REPL 命令

```sh
/queue             # 看待投递条目(id/重试次数/等待秒数/预览)
/failed            # 看失败条目(已超 5 次重试, 移到 failed/)
/retry             # 把 failed/ 全部重置重试计数移回队列
/simulate-failure  # 切换 50% 投递失败率(观察重试)
/heartbeat         # 心跳状态
/trigger           # 手动触发心跳
/stats             # 投递统计(待投/失败/尝试/成功/出错)
```

`/simulate-failure` 是教学利器——开启 50% 失败率后发消息，能直观看到「入队 → 投递失败 → 退避重试 → 最终成功或移到 failed/」的完整生命周期，而不需要真的断网。

## s08-11. 留白与对照

| 方面 | s08 教学 | 生产 |
|------|---------|------|
| 队列存储 | 目录里 JSON 文件 | 相同模式 |
| 原子写入 | tmp+fsync+replace | 相同 |
| 退避 | [5s,25s,2min,10min]+抖动 | 相同 |
| 分片 | 段落边界 | +代码围栏感知 |
| 投递渠道 | MockDeliveryChannel | 真 Telegram/飞书 send |

s08 把「出站可靠性」跑通：预写日志 + 原子写 + 退避重试 + 启动恢复。它和 s07 的内存输出队列的区别是**持久化**——s07 进程崩就丢，s08 崩了重启接着发。这是从「能跑」到「可靠」的跃迁。

---

# 第二部分：s09 韧性（Resilience）

## s09-1. 架构总览：三层重试洋葱

```
Profiles: [main-key, backup-key, emergency-key]
     |
for each non-cooldown profile:              LAYER 1: 认证轮换
     |
create client(profile.api_key)
     |
for compact_attempt in 0..2:                LAYER 2: 溢出恢复
     |
_run_attempt(client, model, ...)            LAYER 3: 工具调用循环
     |              |
   success       exception
     |              |
mark_success    classify_failure()
return result       |
               overflow?  --> 截断+LLM摘要压缩, 重试 Layer 2
               auth/rate? -> mark_failure(冷却), break 到 Layer 1 换 key
               timeout?  --> mark_failure(60s), break 到 Layer 1
               billing?  --> mark_failure(300s), break 到 Layer 1
                    |
               所有 profile 耗尽?
                    |
               try fallback models (换更小模型)
                    |
               全失败?
                    |
               raise RuntimeError
```

核心组件：

| 组件 | 代码位置 | 职责 |
|------|---------|------|
| `FailoverReason` + `classify_failure` | `:131-165` | 把异常分类成 6 种原因 |
| `AuthProfile` | `:173-190` | 单个 API key + 冷却状态 |
| `ProfileManager` | `:198-261` | 选可用 key、标记失败/成功 |
| `ContextGuard` | `:272-423` | token 估算 + 工具结果截断 + LLM 摘要压缩 |
| `ResilienceRunner` | `:620-887` | 三层洋葱核心 |
| `SimulatedFailure` | `:561-599` | 教学用故障注入 |

## s09-2. 核心概念：为什么需要三层

agent 调 LLM 会遇到**不同种类**的失败，它们需要**不同处理**：

| 失败种类 | 例子 | 正确处理 |
|---------|------|---------|
| 鉴权失败 | 401 无效 key | 换个 key（这个 key 坏了，等也没用） |
| 限流 | 429 太频繁 | 换 key 或等一会（这个 key 被限了） |
| 超时 | 请求超时 | 短等后换 key（瞬态故障） |
| 账单 | 402 配额用完 | 换 key（这个 key 没钱了） |
| 上下文溢出 | token 太多 | **不能换 key（换了还是溢出）**→ 压缩历史 |

关键洞察：**前四类是「key 的问题」，换 key 能解决；溢出是「请求的问题」，换 key 没用**——必须压缩消息。所以 s09 用「分类 + 分层」：分类器判断失败种类，不同种类路由到不同层处理。

如果只用「无脑重试」（失败就重试 N 次），既治不了溢出（重试还是溢出），也浪费 key（坏 key 重试还是坏）。s09 的三层结构精确对应不同失败的处理路径。

## s09-3. classify_failure：异常分类器

```python
class FailoverReason(Enum):
    rate_limit = "rate_limit"
    auth = "auth"
    timeout = "timeout"
    billing = "billing"
    overflow = "overflow"
    unknown = "unknown"

def classify_failure(exc):
    msg = str(exc).lower()
    if "rate" in msg or "429" in msg: return FailoverReason.rate_limit
    if "auth" in msg or "401" in msg or "key" in msg: return FailoverReason.auth
    if "timeout" in msg or "timed out" in msg: return FailoverReason.timeout
    if "billing" in msg or "quota" in msg or "402" in msg: return FailoverReason.billing
    if "context" in msg or "token" in msg or "overflow" in msg: return FailoverReason.overflow
    return FailoverReason.unknown
```

朴素做法：把异常转字符串，**关键字匹配**。`429` → 限流、`401` → 鉴权、`402` → 账单、`context/token` → 溢出。不依赖结构化异常类型——因为不同 SDK/不同错误来源的错误对象千差万别，但错误消息文本相对稳定。这是「**字符串匹配做路由**」的实用主义（生产会加 HTTP 状态码检查更准）。

分类结果驱动后续：每类对应不同冷却时长和不同层处理（见 s09-4、s09-5）。

## s09-4. AuthProfile + ProfileManager：key 池与冷却

```python
@dataclass
class AuthProfile:
    name: str                          # "main-key" 等
    provider: str                     # "anthropic"
    api_key: str
    cooldown_until: float = 0.0        # 冷却到何时(此时间前跳过)
    failure_reason: str | None = None  # 上次失败原因
    last_good_at: float = 0.0          # 上次成功时间

class ProfileManager:
    def select_profile(self):          # 选第一个未冷却的
        now = time.time()
        for profile in self.profiles:
            if now >= profile.cooldown_until:
                return profile
        return None                    # 全在冷却 → None

    def mark_failure(self, profile, reason, cooldown_seconds=300.0):
        profile.cooldown_until = time.time() + cooldown_seconds   # 设冷却
        profile.failure_reason = reason.value

    def mark_success(self, profile):
        profile.failure_reason = None
        profile.last_good_at = time.time()
```

**key 池**：一组 API key（main/backup/emergency），按顺序用。`select_profile` 取第一个冷却已过的——坏 key 冷却期间跳过、用下一个。`mark_failure` 给坏 key 设冷却时长（不同原因不同时长）、`mark_success` 清除失败状态。

冷却时长按失败原因分（这是 s09 的精细之处）：

| 原因 | 冷却 | 理由 |
|------|------|------|
| auth | 300s | 坏 key 不会自愈，长冷 |
| billing | 300s | 没钱不会马上有钱，长冷 |
| rate_limit | 120s | 限流窗口一般几分钟，中冷 |
| timeout | 60s | 瞬态故障，短冷 |
| unknown | 120s | 不确定，中冷 |
| overflow | 不冷 | 不冷却 key，而是压缩消息（见 s09-5） |

注意 `overflow` **不冷却 key**——因为溢出不是 key 的错，冷却 key 没意义，应该压缩消息后用**同一个 key** 重试。这是分类驱动行为的关键体现。

s09 教学版用**同一个 key 给三个 profile**（注释明说，演示用），生产是三个真不同 key。

## s09-5. 三层洋葱：run()

`s09` 的灵魂函数。逐层拆：

### Layer 1：认证轮换（最外层）

```python
for _rotation in range(len(self.profile_manager.profiles)):
    profile = self.profile_manager.select_profile()
    if profile is None: break                       # 全冷却 → 退出
    if profile.name in profiles_tried: break        # 都试过了 → 退出
    profiles_tried.add(profile.name)

    api_client = Anthropic(api_key=profile.api_key, ...)   # 用这个 key 建客户端

    # ↓ 进入 Layer 2
    layer2_messages = list(current_messages)
    for compact_attempt in range(MAX_OVERFLOW_COMPACTION):  # 3 次
        try:
            result, layer2_messages = self._run_attempt(...)   # Layer 3
            self.profile_manager.mark_success(profile)
            return result, layer2_messages                      # 成功 → 返回
        except Exception as exc:
            reason = classify_failure(exc)
            if reason == FailoverReason.overflow:
                # 压缩, 重试 Layer 2 (见下)
            elif reason in (auth, billing):
                self.profile_manager.mark_failure(profile, reason, 300)
                break          # 换下一个 profile (回 Layer 1)
            elif reason == rate_limit:
                mark_failure(120); break
            elif reason == timeout:
                mark_failure(60); break
            else:
                mark_failure(120); break
```

Layer 1 是「**轮换 key**」的循环：遍历所有 profile，每个 key 试一轮。**关键决策点在 except 里**——根据 `classify_failure` 的结果决定：

- **overflow** → 不换 key，压缩消息后**重试 Layer 2**（同一个 key）；
- **auth/rate/billing/timeout/unknown** → 给这个 key 设冷却，`break` 退出 Layer 2、回到 Layer 1 换下一个 key。

这就是「分类驱动分层」：换 key 能治的 → 换 key；换 key 治不了的（溢出）→ 压缩。

### Layer 2：溢出恢复（中间层）

```python
if reason == FailoverReason.overflow:
    if compact_attempt < MAX_OVERFLOW_COMPACTION - 1:
        # Stage 1: 截断过大的 tool_result
        layer2_messages = self.guard.truncate_tool_results(layer2_messages)
        # Stage 2: LLM 摘要压缩历史
        layer2_messages = self.guard.compact_history(layer2_messages, api_client, self.model_id)
        continue          # 用压缩后的消息重试 Layer 3
    else:
        # 压缩 3 次还溢出 → 放弃这个 key
        self.profile_manager.mark_failure(profile, reason, 600)
        break              # 回 Layer 1 换 key
```

溢出时，**两阶段压缩**：

**Stage 1 `truncate_tool_results`**（`:308-332`）：把超长的 `tool_result` 块截断。工具返回（比如读了个 10 万字的文件）往往是上下文膨胀的元凶——截掉它，保留头部 + 「[truncated]」标注。这是**无损优先**的压缩：先去最大的冗余。

**Stage 2 `compact_history`**（`:334-423`）：把前 50% 的消息用 **LLM 摘要**压缩成一段「[Previous conversation summary]」，保留最后 20%（至少 4 条）不动。这是**有损压缩**：老对话变摘要，近对话原样保留。

两阶段的设计思想：**先无损（截工具结果）、后有损（摘要历史）**，逐步降低上下文体积。压缩后用**同一个 key** 重试 Layer 3——因为溢出是消息太长不是 key 坏。

### Layer 3：工具调用循环（最内层）

```python
def _run_attempt(self, api_client, model, system, messages, tools):
    current_messages = list(messages)
    iteration = 0
    while iteration < self.max_iterations:
        iteration += 1
        response = api_client.messages.create(...)   # 调 LLM
        current_messages.append({"role": "assistant", "content": response.content})
        if response.stop_reason == "end_turn":
            return response, current_messages        # 完成 → 返回
        elif response.stop_reason == "tool_use":
            # 执行工具, 塞结果, continue
            ...
        else:
            return response, current_messages        # 兜底当 end_turn
    raise RuntimeError(f"exceeded {self.max_iterations} iterations")
```

Layer 3 就是 s01/s02 教的**标准工具循环**——`while True + stop_reason` 分发。这是和前几节同构的核心。区别是：**任何 API 异常都向外层传播**（不在这里 try/except）——让 Layer 2 的 except 接住、分类、决定怎么重试。Layer 3 只管「正常跑通」，失败交给外层洋葱。

### 兜底：fallback models

```python
# 所有 profile 耗尽后
for fallback_model in self.fallback_models:    # ["claude-haiku-4-..."]
    profile = self.profile_manager.select_profile()
    if profile is None:
        # 重置 rate_limit/timeout 的冷却(它们可能过期了)
        for p in self.profile_manager.profiles:
            if p.failure_reason in (rate_limit, timeout):
                p.cooldown_until = 0.0
        profile = self.profile_manager.select_profile()
    if profile is None: continue
    try:
        result, updated = self._run_attempt(api_client, fallback_model, ...)  # 换小模型
        ...
        return result, updated
    except: continue

raise RuntimeError("All profiles and fallback models exhausted")
```

主 model + 所有 key 都失败后，**换更小/更便宜的模型**（fallback，如 haiku）再试——小模型 token 上限高、便宜、可能正好能跑通。这层的精明之处：重试时**重置 rate_limit/timeout 的冷却**（因为重试期间时间已过，那些瞬态限制可能已解除），给 key 第二次机会。

全失败才 `raise RuntimeError`——这是「**尽力而为后明确失败**」：所有手段用尽才报错，调用方知道是真不行了。

## s09-6. 重试上限：动态公式

```python
num_profiles = len(profile_manager.profiles)    # 3
self.max_iterations = min(
    max(BASE_RETRY + PER_PROFILE * num_profiles, 32),   # max(24+8*3, 32)=56
    160,                                                  # 上限 160
)
```

`max_iterations` 是 Layer 3 工具循环的迭代上限（防死循环）。公式：

- `BASE_RETRY(24) + PER_PROFILE(8) × key 数(3)` = 48——key 越多，允许的循环越多（因为可能轮换多次）。
- 下限 32、上限 160——保底够用、又不会无限。

这是个**自适应**的限额：key 多就放宽、但封顶防爆。

## s09-7. ContextGuard：token 估算与压缩

```python
class ContextGuard:
    def __init__(self, max_tokens=CONTEXT_SAFE_LIMIT):  # 180000
        self.max_tokens = max_tokens

    @staticmethod
    def estimate_tokens(text):
        return len(text) // 4          # 粗估: 4 字符 ≈ 1 token

    def estimate_messages_tokens(self, messages):
        # 遍历所有消息的所有 block(text/tool_result/tool_use)累加估算
        ...
```

`estimate_tokens` 用「4 字符 ≈ 1 token」粗估——不调 tokenizer，够用来判断「是不是快溢出了」。`estimate_messages_tokens` 遍历消息列表所有 block 累加——能处理 string content、list content（text/tool_result/tool_use block）各种形态。

`CONTEXT_SAFE_LIMIT=180000` 是个软上限——超过就该压缩。`MAX_TOOL_OUTPUT=50000` 限制单次工具返回。

`compact_history` 的细节（`:334-423`）：

```python
total = len(messages)
keep_count = max(4, int(total * 0.2))            # 保留最后 20%(至少 4 条)
compress_count = max(2, int(total * 0.5))        # 压缩前 50%
compress_count = min(compress_count, total - keep_count)
old_messages = messages[:compress_count]
recent_messages = messages[compress_count:]
# 把 old 展平成文本, 调 LLM 摘要
summary = api_client.messages.create(system="You are a summarizer...", messages=[summary_prompt])
# 用 [Previous conversation summary] + 一条 assistant ack 替换 old
compacted = [
    {"role": "user", "content": "[Previous conversation summary]\n" + summary},
    {"role": "assistant", "content": [{"type":"text","text":"Understood, I have the context..."}]},
] + recent_messages
```

注意压缩本身要调 LLM——如果摘要调用失败，降级为「直接丢老消息」(`return recent_messages`)，保底不卡死。这是「**恢复机制本身也要容错**」的体现。

摘要后用一对「user 摘要 + assistant 确认」消息替换原本几十条历史——体积骤降，但关键事实保留在摘要里。这是 s06 智能层提到的「上下文溢出处理」的真实实现（s06 只提了概念，s09 给了代码）。

## s09-8. SimulatedFailure：教学故障注入

```python
class SimulatedFailure:
    TEMPLATES = {
        "rate_limit": "Error code: 429 -- rate limit exceeded",
        "auth": "Error code: 401 -- authentication failed, invalid API key",
        "timeout": "Request timed out after 30s",
        "billing": "Error code: 402 -- billing quota exceeded",
        "overflow": "Error: context window token overflow, too many tokens",
        "unknown": "Error: unexpected internal server error",
    }
    def arm(self, reason): self._pending = reason; return "Armed: ..."
    def check_and_fire(self):
        if self._pending is not None:
            reason = self._pending
            self._pending = None                      # 一次性
            raise RuntimeError(self.TEMPLATES[reason])   # 抛模拟错
```

`/simulate-failure rate_limit` 给下次 API 调用「装一个故障」——`run` 在调真 API 前 `check_and_fire()`，如果有 armed 故障就抛对应错误。这让你能**不真断网/不真坏 key** 就观察三层洋葱怎么处理各类失败。一次性：触发后解除，下次正常调真 API。

这是教学设计的精髓——把罕见、难复现的生产故障（限流、坏 key、溢出）变成可按需触发的演示，让学习者看见洋葱的每一层在干活。

## s09-9. agent_loop 集成

```python
runner = ResilienceRunner(profile_manager, model_id, fallback_models, guard, sim_failure)
...
while True:
    user_input = input(...)
    ...
    messages.append({"role": "user", "content": user_input})
    try:
        response, messages = runner.run(system=SYSTEM_PROMPT, messages=messages, tools=TOOLS)
        # 打印回复
    except RuntimeError as exc:
        print_error(str(exc))                        # 全失败
        # 回滚 user 消息(和 s04 同构)
        while messages and messages[-1]["role"] != "user": messages.pop()
        if messages: messages.pop()
```

主循环很简洁——因为所有重试复杂性都封装在 `runner.run` 里了。`run` 要么返回成功结果，要么抛 `RuntimeError`（全失败）。抛错时回滚 `messages`（和 s04 失败回滚同构），保证会话可重入。

对比 s04/s06 的内联工具循环，s09 把工具循环包进三层洋葱——**调用方不感知重试/轮换/压缩**，只看到「成功或全失败」。这是「把可靠性封装成黑盒」的工程化。

## s09-10. REPL 命令

```sh
/profiles               # 各 key 状态(available/cooldown) + 上次成功时间 + 失败原因
/cooldowns              # 当前活跃冷却(剩余秒数)
/simulate-failure <r>   # 装一个模拟故障(rate_limit/auth/timeout/billing/overflow/unknown)
/fallback               # 备选模型链
/stats                  # 弹性统计(尝试/成功/失败/压缩次数/轮换次数/最大迭代)
```

`/simulate-failure rate_limit` → 发消息 → 观察：profile 标记冷却 → 轮换到 backup → backup 成功 → 返回。`/simulate-failure overflow` → 观察：截断 + LLM 摘要压缩 → 同 key 重试。直接看见三层洋葱的运作。

## s09-11. 留白与对照

| 方面 | s09 教学 | 生产 |
|------|---------|------|
| key | 同一个 key 给三个 profile | 跨提供商多真 key |
| 分类器 | 字符串匹配 | + HTTP 状态码 |
| 压缩 | 截断 + LLM 摘要 | 同两阶段 |
| 备选模型 | haiku | 同链，更小更便宜 |
| 重试上限 | 24+8×N，封 160 | 同公式 |

---

# 第三部分：s08 + s09 对照与总结

## 两节的对照

| 维度 | s08 投递 | s09 韧性 |
|------|---------|---------|
| 方向 | 出站（agent → 平台） | 入站（agent → LLM） |
| 失败对象 | 投递动作（发消息） | API 调用（调 LLM） |
| 持久化 | 磁盘队列（WAL） | 无（重试在内存） |
| 重试方式 | 退避（5s→10min）+ 抖动 | 分类驱动（换 key/压缩） |
| 崩溃恢复 | 有（启动扫描） | 无（重试不跨进程） |
| 终态 | ack（删）/ failed/（留档） | RuntimeError（全失败） |
| 后台 | DeliveryRunner 线程 | 同步（在主循环里） |

它们正交：s08 管「消息发出去」的可靠，s09 管「调 LLM」的可靠。一个 agent 可以两者都有——调 LLM 用 s09 洋葱保住、回复用 s08 队列可靠投递。

## 共同的设计哲学

两节都体现**「先保命、再恢复」**的可靠性思想：

- **s08**：先写盘（保命，崩了不丢），后台慢慢发，失败退避重试，启动恢复。
- **s09**：先分类（判明是什么失败），对症下药（换 key/压缩），多层兜底（fallback model），全不行才报错。

核心都是**「失败是常态，要分类、要分层、要可恢复」**——不是假装不会失败，而是把每种失败都安排好出路。这是从「能跑的 demo」到「可靠的系统」的质变。

## 在 10 章地图里的位置

```
s01-s02: 循环 + 工具        (能跑)
s03-s04: 持久化 + 通道       (能存能连)
s05-s06: 路由 + 智能         (能找能想)
s07:    心跳 + cron          (能主动)
s08:    投递队列             (出站可靠)  ← 本文上半
s09:    重试洋葱             (入站可靠)  ← 本文下半
s10:    命名车道             (并发有序)
```

s08/s09 是可靠性双子星——s07 让 agent 主动了，但主动产生的输出可能丢（s08 解决）、主动调 LLM 可能失败（s09 解决）。它们让 agent 从「会跑」到「**长期可靠地跑**」。

## 运行方法

```sh
# s08
cd claw0 && python sessions/zh/s08_delivery.py
/queue /failed /simulate-failure /stats   # 观察投递生命周期

# s09
cd claw0 && python sessions/zh/s09_resilience.py
/profiles /simulate-failure rate_limit /cooldowns /stats   # 观察三层洋葱
```

## 一句话总结

s08+s09 是 claw0 的**可靠性双子层**：**s08 投递**用「预写磁盘队列（tmp+fsync+os.replace 原子写保证崩溃安全）+ 退避重试（[5s,25s,2min,10min] 带 ±20% 抖动防惊群）+ 启动恢复扫描」让 agent 的所有出站消息（回复/心跳/cron 输出）先写盘再发、失败可重试、崩溃重启接着发、5 次失败移到 failed/ 留档，把「能跑」变「出站可靠」；**s09 韧性**用「三层重试洋葱」包裹每次调 LLM——Layer 1 在 API key 池里轮换（坏 key 按原因设不同冷却 60-300s 跳过）、Layer 2 在溢出时两阶段压缩（先截断 tool_result 无损、后 LLM 摘要历史有损，用同 key 重试，因为溢出不是 key 的错）、Layer 3 是标准工具循环（异常向外传播不内捕），全 profile 耗尽换 fallback 小模型再试、全失败才抛 RuntimeError；`classify_failure` 把异常字符串分类成 rate_limit/auth/timeout/billing/overflow/unknown 六种，分类驱动「换 key 能治的换 key、换 key 治不了的压缩」。两节正交（s08 出站、s09 入站），共同哲学是「**失败是常态，要分类、要分层、要可恢复**」——s08 靠持久化+退避让出站不丢，s09 靠分类+轮换+压缩让入站能跑完，把 agent 从「会跑」推到「长期可靠地跑」。

result: 在 `docs/explain/delivery-resilience.md` 写了第 8、9 节合并详解文档（约 13 主节，分三部分：s08 投递含预写日志原则/原子写入三步崩溃安全/退避抖动防惊群/分片/后台线程启动恢复/REPL命令/留白；s09 韧性含三层洋葱必要性/异常分类器/key池冷却/两阶段压缩/重试公式/故障注入/REPL命令/留白；s08+s09对照与共同可靠性哲学及10章地图位置/运行方法），现进入工作树提交并合并回 main。
