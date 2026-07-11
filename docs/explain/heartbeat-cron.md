# 第 07 节详解：心跳与 Cron（Heartbeat & Cron）

> 不再只是被动响应——agent 学会主动出击。

本文是对 `sessions/zh/s07_heartbeat_cron.py`（659 行）和 `s07_heartbeat_cron.md` 的逐层深读。s01-s06 的 agent 全是「**被动**」的：你不发消息它就静默。**s07 让 agent 变「主动」**——有个后台定时器周期性唤醒它，自己检查「有没有该做的事」，到点主动报告或执行任务。这是从「响应式工具」到「自主智能体」的关键一跃。

---

## 0. 一句话定位

s07 = **主动层**。它回答一个问题：**「agent 没人理的时候，凭什么还能自己动起来？」**

答案是一个**后台定时器 + 车道互斥**机制：

```
被动(s01-s06):  用户发消息 ──► agent 响应 ──► 回复      (没人发就不动)

主动(s07):
  ① 心跳车道:    定时器(每秒检查) ──► 该跑吗? ──► 抢车道 ──► 跑 agent ──► 排队输出
  ② cron 车道:   CRON.json 定点 ──► 到点? ──► 跑 agent ──► 排队输出
  ③ 主车道:      用户消息 ──► 阻塞抢车道 ──► 跑 agent ──► 打印
                                                    ↑
                                       三条车道共享 lane_lock, 用户永远优先
```

核心思想：**主动行为复用被动行为的同一条 agent 管线**（都是 `client.messages.create`），只是触发源从「用户消息」变成「定时器」，且通过车道锁保证「用户在说话时，后台主动让步」。

---

## 1. 架构总览

```
Main Lane (用户输入):
    User Input --> lane_lock.acquire() (阻塞) --> LLM --> Print
                   (阻塞获取: 用户总是赢)

Heartbeat Lane (后台线程, 1s 轮询):
    should_run()?
        |no --> sleep 1s
        |yes
    _execute():
        lane_lock.acquire(blocking=False)   ← 非阻塞
            |fail --> 让步 (用户优先)
            |success
        build prompt from HEARTBEAT.md + SOUL.md + MEMORY.md
        run_agent_single_turn()
        parse: "HEARTBEAT_OK"? --> 丢弃
               有内容? --> 和上次重复? --> 丢弃
                            |不重复
                       output_queue.append()

Cron Service (后台线程, 1s tick):
    CRON.json --> load_jobs --> tick() 每秒
        for each job: enabled? --> due? --> _run_job()
        error? --> consecutive_errors++ --> >=5? --> auto-disable
        |ok
    consecutive_errors=0 --> 写 cron-runs.jsonl
```

三个核心组件 + 一个共享锁：

| 组件 | 代码位置 | 职责 |
|------|---------|------|
| `HeartbeatRunner` | `:148-304` | 周期性主动检查（心跳），用 `HEARTBEAT.md` 当指令 |
| `CronService` + `CronJob` | `:312-481` | 定点任务调度（at/every/cron），错误自动禁用 |
| `lane_lock` | `agent_loop:499` | 共享锁，保证用户消息优先 |
| `run_agent_single_turn` | `:132-142` | 单轮 LLM 调用，心跳和 cron 共用 |

外加两个后台线程（heartbeat 线程 + cron-tick 线程）和一个输出队列机制。下面逐个拆。

---

## 2. 先理解核心概念：被动 vs 主动

| | 被动（reactive） | 主动（proactive） |
|---|---|---|
| 触发源 | 用户发消息 | 定时器到点 |
| 没人时 | 静默 | 自己跑 |
| 输入 | 用户文字 | 心跳指令 / cron payload |
| 典型输出 | 回答用户 | 主动提醒 / 日报 / `HEARTBEAT_OK` |
| s01-s06 | ✅ 全是被动 | —— |
| s07 | —— | ✅ 引入主动 |

s07 的两种「主动」：

- **心跳（Heartbeat）**：**周期性**自检。每隔 N 秒（默认 1800s=30分钟）+ 在活跃时段（9-22点）内，自动唤醒 agent 按 `HEARTBEAT.md` 检查「有没有该报告的」。像心脏规律跳动。
- **Cron**：**定点**任务。按 `CRON.json` 配置的时间（某时刻 / 每隔多久 / cron 表达式）执行特定任务。像闹钟定点响。

两者都是「主动」，区别是触发节奏：心跳是「定期巡检」，cron 是「定点干活」。

---

## 3. Lane 互斥：用户永远优先（最重要的设计）

这是 s07 最核心的设计原则。问题是：**后台心跳/cron 随时可能跑 agent，如果它跑的时候用户正好发消息，怎么办？** 不能让后台任务霸占 LLM、让用户干等。

答案：**用一个共享锁（`lane_lock`），两条车道抢同一把锁，但抢法不同——用户阻塞抢（总能抢到），后台非阻塞抢（抢不到就退让）。**

### 3.1 用户的抢法：阻塞获取

```python
# agent_loop 里处理用户消息:
lane_lock.acquire()                    # 阻塞: 锁被后台占着就等, 直到拿到
try:
    messages.append({"role": "user", "content": user_input})
    ...跑 agent...
finally:
    lane_lock.release()
```

`acquire()` 不带参数 = **阻塞获取**：锁被别人占着，就**死等**，直到对方释放、自己拿到。所以用户消息**最终一定能进**——哪怕后台正在跑，用户会等它跑完（或后台让步后）再进。用户**始终能进入**。

### 3.2 后台的抢法：非阻塞获取

```python
# HeartbeatRunner._execute:
acquired = self.lane_lock.acquire(blocking=False)   # 非阻塞: 抢不到立刻返回 False
if not acquired:
    return        # 用户占着锁, 跳过本次心跳, 下次再来
```

`acquire(blocking=False)` = **非阻塞获取**：锁被占着，**立刻返回 False**，不等。所以后台心跳**抢不到就让步**——绝不等用户、绝不和用户抢。这一轮跳过，下一秒再试。

### 3.3 为什么这样设计

| | 用户车道 | 后台车道 |
|---|---|---|
| 抢法 | 阻塞 `acquire()` | 非阻塞 `acquire(blocking=False)` |
| 锁被占时 | 等 | 立刻退让 |
| 谁总赢 | 用户（总能进） | 抢不到就跳过 |
| 延迟 | 可能等后台跑完 | 无延迟（立刻让步） |

效果：**用户在说话时，后台心跳/cron 全部让步**；用户空闲时，后台才占锁跑主动任务。交互体验永远是「用户优先」——后台任务绝不会让用户卡顿。

这是「**读写优先级**」思想的简化版：用户=高优先级写者（阻塞等），后台=低优先级读者（试一下不行就走）。s10 会把它升级成「命名车道」系统，s07 先用单锁把「优先级」这个核心概念跑通。

### 3.4 TOCTOU 竞态——为什么锁检测不放 `should_run`

注意 `should_run()` 里有 `if self.running: return False`（`:185`），但它**不检测 `lane_lock`**。锁的检测在 `_execute()` 里单独做。为什么？

```python
def should_run(self):
    ...
    if self.running: return False, "already running"   # 只查 running 标志
    return True, "all checks passed"
    # 注意: 这里没查 lane_lock

def _execute(self):
    acquired = self.lane_lock.acquire(blocking=False)   # 锁检测在这!
    if not acquired: return
    ...
```

如果 `should_run()` 里检测锁、`_execute()` 里再获取锁，会有 **TOCTOU（Time-Of-Check-To-Time-Of-Use）竞态**：

```
t1: should_run() 查锁 → 没人占 → 返回 True
t2: 用户线程恰好 acquire 了锁
t3: _execute() acquire(blocking=False) → 失败!  ← 但 should_run 已经说 True 了
```

「检查时」和「使用时」之间状态变了。s07 的解法：**把锁检测和锁获取合并成同一个原子动作**——`acquire(blocking=False)` 本身就是「检测+获取」一体的（成功=没被占且我拿到了，失败=被占）。`should_run` 只做轻量前置检查（文件存在、间隔、时段、running），重锁在 `_execute` 里原子获取。这是并发编程的精细设计。

---

## 4. HeartbeatRunner：周期性自检

### 4.1 `should_run()`：4 项前置检查

```python
def should_run(self) -> tuple[bool, str]:
    if not self.heartbeat_path.exists():
        return False, "HEARTBEAT.md not found"
    if not self.heartbeat_path.read_text(encoding="utf-8").strip():
        return False, "HEARTBEAT.md is empty"

    elapsed = time.time() - self.last_run_at
    if elapsed < self.interval:
        return False, f"interval not elapsed ({self.interval - elapsed:.0f}s remaining)"

    hour = datetime.now().hour
    s, e = self.active_hours
    in_hours = (s <= hour < e) if s <= e else not (e <= hour < s)
    if not in_hours:
        return False, f"outside active hours ({s}:00-{e}:00)"

    if self.running:
        return False, "already running"
    return True, "all checks passed"
```

四个检查**全过**才跑：

1. **HEARTBEAT.md 存在**：没这个文件就不跑心跳（心跳是可选的，没配就不开）。
2. **HEARTBEAT.md 非空**：空文件没意义，跳过。
3. **间隔已过**：距上次跑不足 `interval`（默认 1800s）就不跑——节流，别频繁打扰。`time.time() - last_run_at` 是单调流逝时间。
4. **在活跃时段内**：默认 9-22 点。半夜别主动跳出来烦人。注意 `in_hours` 的跨午夜处理：`active_hours=(22,9)` 表示晚10到早9（跨午夜），用 `not (e <= hour < s)` 算——这是处理「活跃时段跨过 0 点」的正确逻辑。
5. **没在跑**：上一轮还没结束就不重叠跑。

返回 `(bool, reason)`——带原因，`status` 命令能显示「为什么没跑」（剩余多少秒 / 不在时段）。这对调试很有用。

注意这里**不查 `lane_lock`**（见 3.4 TOCTOU）。

### 4.2 `_execute()`：跑一次心跳

```python
def _execute(self) -> None:
    acquired = self.lane_lock.acquire(blocking=False)   # 非阻塞抢锁
    if not acquired:
        return                                            # 用户在用, 让步
    self.running = True
    try:
        instructions, sys_prompt = self._build_heartbeat_prompt()
        if not instructions:
            return
        response = run_agent_single_turn(instructions, sys_prompt)  # 单轮 LLM
        meaningful = self._parse_response(response)
        if meaningful is None:                           # HEARTBEAT_OK → 丢弃
            return
        if meaningful.strip() == self._last_output:       # 和上次重复 → 丢弃
            return
        self._last_output = meaningful.strip()
        with self._queue_lock:
            self._output_queue.append(meaningful)        # 排队, 等主线程取
    except Exception as exc:
        with self._queue_lock:
            self._output_queue.append(f"[heartbeat error: {exc}]")
    finally:
        self.running = False
        self.last_run_at = time.time()
        self.lane_lock.release()
```

四道关卡，层层过滤：

1. **抢锁**：非阻塞，抢不到立刻 return（让步用户）。
2. **跑 agent**：用 `_build_heartbeat_prompt` 构造的指令调 `run_agent_single_turn`。
3. **解析**：`_parse_response` 把 `HEARTBEAT_OK`（没事）过滤掉。
4. **去重**：和 `_last_output` 比，重复就不报（避免每隔 30 分钟说同样的话）。

通过这四关的「有意义的新内容」才进输出队列。`finally` 保证无论成功失败都 `running=False` + 更新 `last_run_at` + 释放锁——状态必复位，不会卡死在「running」。

### 4.3 `_build_heartbeat_prompt`：构造心跳指令

```python
def _build_heartbeat_prompt(self) -> tuple[str, str]:
    instructions = self.heartbeat_path.read_text(encoding="utf-8").strip()  # HEARTBEAT.md
    mem = self._memory.load_evergreen()
    extra = ""
    if mem:
        extra = f"## Known Context\n\n{mem}\n\n"                            # MEMORY.md
    extra += f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    return instructions, self._soul.build_system_prompt(extra)              # SOUL.md + extra
```

心跳的 prompt 由三部分拼：
- **指令**（user 消息）：`HEARTBEAT.md` 全文——「检查这些项，没事回 HEARTBEAT_OK」。
- **系统提示词**：`SOUL.md`（人格）+ `Known Context`（`MEMORY.md` 长期记忆）+ 当前时间。

注意 s07 复用了 s06 的概念（SOUL/MEMORY），但是**简化版**——s07 的 `SoulSystem`/`MemoryStore`（`:76-112`）是 s06 对应类的精简：没有 8 层组装、没有混合搜索，只有最朴素的加载/写入/按行匹配搜索。这是教学取舍：s07 的重点是「主动触发机制」而非「提示词工程」，所以智能层用简化版，避免和 s06 重复一大坨代码。生产代码会直接复用 s06 的完整版。

### 4.4 `_parse_response`：HEARTBEAT_OK 约定 + 去重

```python
def _parse_response(self, response: str) -> str | None:
    if "HEARTBEAT_OK" in response:
        stripped = response.replace("HEARTBEAT_OK", "").strip()
        return stripped if len(stripped) > 5 else None
    return response.strip() or None
```

`HEARTBEAT_OK` 是个**约定信号**——agent 在 `HEARTBEAT.md` 指示下，没事时回这个词。`_parse_response` 看到 `HEARTBEAT_OK` 就知道「没东西要报告」：
- 纯 `HEARTBEAT_OK` → 返回 `None`（丢弃，不进队列）；
- 如果 `HEARTBEAT_OK` 后还跟了 >5 字符的内容（比如「HEARTBEAT_OK，但顺便提一句明天开会」），把 `HEARTBEAT_OK` 去掉、保留剩余内容——**约定信号 + 附带内容**的情况。

这是个很实用的设计：用一个「哨兵词」让 agent 表达「没事」，程序据此过滤掉无意义的空跑，不烦用户。

### 4.5 `_loop` + 后台线程

```python
def _loop(self) -> None:
    while not self._stopped:
        try:
            ok, _ = self.should_run()
            if ok:
                self._execute()
        except Exception:
            pass
        time.sleep(1.0)

def start(self) -> None:
    self._thread = threading.Thread(target=self._loop, daemon=True, name="heartbeat")
    self._thread.start()
```

daemon 线程，每秒检查一次 `should_run`，过了就 `_execute`。`time.sleep(1.0)` 是轮询节流——不忙转。`daemon=True` 进程退出自动带走。`_stopped` 是退出开关。

这是典型的「**轮询线程 + 节流检查**」模式——和 s04 的 `telegram_poll_loop` 同构（while + sleep + 检查条件 + 干活）。

### 4.6 输出队列：后台→主线程的安全输送

```python
self._output_queue: list[str] = []
self._queue_lock = threading.Lock()

# 后台线程写:
with self._queue_lock:
    self._output_queue.append(meaningful)

# 主线程读(drain):
def drain_output(self) -> list[str]:
    with self._queue_lock:
        items = list(self._output_queue)
        self._output_queue.clear()
        return items
```

后台心跳产生的内容不能直接 `print`——因为主线程可能在 `input()` 阻塞、或在跑用户对话，直接 print 会和用户输出交错乱套。所以用**队列缓冲**：后台往队列塞，主线程在 REPL 循环每轮开头 `drain_output` 取出来打印。

这和 s04 的 `msg_queue` 完全同构——`list + Lock` 手搓的线程安全队列，生产者（后台线程）写、消费者（主线程）批取。s08 会把它升级成「可靠投递队列」（先写盘再发），s07 先用内存版把「后台输出怎么到前台」跑通。

### 4.7 `trigger`：手动触发

```python
def trigger(self) -> str:
    acquired = self.lane_lock.acquire(blocking=False)
    if not acquired:
        return "main lane occupied, cannot trigger"
    ...同 _execute 的逻辑, 但返回字符串报告结果...
```

`/trigger` 命令调它——**绕过间隔检查**（`should_run` 不查），立刻跑一次心跳。但仍守车道锁（用户在用就拒绝）。这是调试/演示用：不想等 30 分钟，敲 `/trigger` 立刻看心跳效果。

---

## 5. run_agent_single_turn：单轮 LLM（心跳/cron 共用）

```python
def run_agent_single_turn(prompt: str, system_prompt: str | None = None) -> str:
    sys_prompt = system_prompt or "You are a helpful assistant performing a background check."
    try:
        response = client.messages.create(
            model=MODEL_ID, max_tokens=2048, system=sys_prompt,
            messages=[{"role": "user", "content": prompt}],
        )
        return "".join(b.text for b in response.content if hasattr(b, "text")).strip()
    except Exception as exc:
        return f"[agent error: {exc}]"
```

和 s04/s06 的 `_agent_loop`/`run_agent_turn` 的关键区别：**单轮、不用工具**。

- `messages=[{"role":"user",...}]`：只传一条 user 消息，**不带历史**——每次心跳/cron 是独立的单轮调用，不延续之前对话。这合理：心跳是「自检」，不需要记住刚才聊了什么（要记忆的话 `MEMORY.md` 已注入 system prompt）。
- 不传 `tools=`：**不让心跳/cron 调工具**。这是个克制设计——后台任务应该「轻、快、安全」，给它工具等于让它能改文件/写记忆，风险高。心跳只该「检查+报告」，不该「动手」。需要动手的任务用 cron 的 `agent_turn` payload 显式安排。
- `max_tokens=2048`：限长，防后台任务写一堆。
- 异常包成字符串返回，不抛——后台任务失败不该崩线程。

这是个「**最小化后台 agent**」的范例：能调 LLM、能拿文本，但不给工具、不给历史、限长、容错。够用且安全。

---

## 6. CronService：定点任务调度

cron 是「定点」主动——按配置的时间表执行特定任务，比心跳更精确、更任务化。

### 6.1 三种调度类型

```python
@dataclass
class CronJob:
    id: str
    name: str
    enabled: bool
    schedule_kind: str       # "at" | "every" | "cron"
    schedule_config: dict
    payload: dict            # {"kind": "agent_turn", "message": "..."}
    consecutive_errors: int = 0
    last_run_at: float = 0.0
    next_run_at: float = 0.0
```

`schedule_kind` 三种：

| kind | 含义 | 例子 | 适用 |
|------|------|------|------|
| `at` | 一次性定点 | `{"at": "2026-07-11T15:00"}` | 「下午3点提醒开会」——跑一次 |
| `every` | 固定间隔 | `{"every_seconds": 3600, "anchor": "..."}` | 「每小时巡检」 |
| `cron` | cron 表达式 | `{"expr": "0 9 * * *"}` | 「每天9点发日报」——最灵活 |

### 6.2 `_compute_next`：算下次运行时间

```python
def _compute_next(self, job, now):
    cfg = job.schedule_config
    if job.schedule_kind == "at":
        ts = datetime.fromisoformat(cfg.get("at", "")).timestamp()
        return ts if ts > now else 0.0          # 过了就 0(不再跑)
    if job.schedule_kind == "every":
        every = cfg.get("every_seconds", 3600)
        anchor = datetime.fromisoformat(cfg.get("anchor", "")).timestamp()
        steps = int((now - anchor) / every) + 1
        return anchor + steps * every           # 对齐到锚点
    if job.schedule_kind == "cron":
        return croniter(expr, datetime.fromtimestamp(now)).get_next(datetime).timestamp()
    return 0.0
```

每种类型算下次时间戳的逻辑不同：

- **at**：解析时刻，没到就返回它，过了返回 0（一次性，过期作废）。
- **every**：**对齐到锚点**——`anchor + steps*every`。关键设计：不是「上次跑完+every」（那样每次会漂移、累积误差），而是**对齐到固定的锚点时刻**。比如锚点是 9:00、间隔 1 小时，现在 10:30，下次是 11:00（不是 11:30）。这样触发时间**可预测、不漂移**，多个任务不会因为前一个慢而连串推迟。
- **cron**：用 `croniter` 库算下次匹配——标准 5 字段 cron（分 时 日 月 周）。`0 9 * * *` = 每天 9:00。

返回 0.0 表示「没有下次」（一次性已过期 / 配置错）。

### 6.3 `tick`：每秒检查到期任务

```python
def tick(self):
    now = time.time()
    remove_ids = []
    for job in self.jobs:
        if not job.enabled or job.next_run_at <= 0 or now < job.next_run_at:
            continue                              # 没启用 / 没下次 / 还没到
        self._run_job(job, now)
        if job.delete_after_run and job.schedule_kind == "at":
            remove_ids.append(job.id)             # 一次性跑完删除
    if remove_ids:
        self.jobs = [j for j in self.jobs if j.id not in remove_ids]
```

`tick` 由 cron-tick 后台线程每秒调一次。遍历所有任务：启用 + 有下次时间 + 到点了 → 跑。`delete_after_run` + `at` 类型跑完从列表删——一次性任务的自清理。

### 6.4 `_run_job`：执行 + 错误自禁用 + 日志

```python
def _run_job(self, job, now):
    payload = job.payload
    kind = payload.get("kind", "")
    output, status, error = "", "ok", ""
    try:
        if kind == "agent_turn":                   # 跑 agent
            msg = payload.get("message", "")
            sys_prompt = "You are performing a scheduled background task..."
            output = run_agent_single_turn(msg, sys_prompt)
        elif kind == "system_event":              # 系统事件(不调 LLM)
            output = payload.get("text", "")
        else:
            output, status, error = f"[unknown kind]", "error", "unknown"
    except Exception as exc:
        status, error, output = "error", str(exc), f"[cron error: {exc}]"

    job.last_run_at = now
    if status == "error":
        job.consecutive_errors += 1
        if job.consecutive_errors >= 5:            # 连续5次错 → 自动禁用
            job.enabled = False
            ...通知...
    else:
        job.consecutive_errors = 0                 # 成功就清零
    job.next_run_at = self._compute_next(job, now) # 重算下次
    # 写日志 cron-runs.jsonl
    entry = {"job_id": ..., "run_at": ..., "status": status, "output_preview": output[:200]}
    with open(self._run_log, "a") as f:
        f.write(json.dumps(entry) + "\n")
    if output and status != "skipped":
        self._output_queue.append(f"[{job.name}] {output}")  # 进队列
```

三个要点：

**① payload 的 kind**：`agent_turn`（调 LLM 跑）或 `system_event`（直接用配置的文本，不调 LLM）。后者用于「纯通知」类任务——比如定时往队列塞一句「该喝水了」，不需要 LLM 生成。

**② 自动禁用（熔断）**：
```python
if status == "error":
    job.consecutive_errors += 1
    if job.consecutive_errors >= 5:
        job.enabled = False   # 熔断!
else:
    job.consecutive_errors = 0   # 成功清零
```
连续 5 次失败就 `enabled=False`——**熔断**，停止再跑这个坏任务。这是「**故障隔离**」：一个坏配置（比如 payload 写错、message 空）的任务，不能因为每分钟跑一次就每分钟报一次错、刷屏 + 烧 token。5 次后自动停，并通知。成功一次就清零——是「连续」错误，不是「累计」。

这是生产系统的重要自保机制：**定时任务的失败要能自我了断**，否则一个坏任务能把系统拖垮。

**③ 日志**：每次跑写一条 JSONL 到 `cron-runs.jsonl`——和 s03 会话持久化同构（追加写）。记录 job_id/时间/状态/输出预览/错误，用于审计「这个任务到底跑没跑、结果如何」。这是「可观测性」的基础。

### 6.5 加载任务 + 手动触发

```python
def load_jobs(self):
    raw = json.loads(self.cron_file.read_text())
    for jd in raw.get("jobs", []):
        kind = jd.get("schedule", {}).get("kind", "")
        if kind not in ("at", "every", "cron"):
            continue                              # 跳过无效 kind
        job = CronJob(...)
        job.next_run_at = self._compute_next(job, now)
        self.jobs.append(job)
```

从 `CRON.json` 加载，过滤掉 kind 不合法的。`trigger_job` 供 `/cron-trigger <id>` 手动触发某任务（绕过时间，调试用）。

---

## 7. agent_loop：三条车道集成

```python
def agent_loop():
    lane_lock = threading.Lock()                  # 共享锁
    heartbeat = HeartbeatRunner(workspace=..., lane_lock=lane_lock, ...)
    cron_svc = CronService(WORKSPACE_DIR / "CRON.json")
    heartbeat.start()                             # 起心跳线程

    cron_stop = threading.Event()
    def cron_loop():
        while not cron_stop.is_set():
            cron_svc.tick()
            cron_stop.wait(timeout=1.0)           # 每秒 tick
    threading.Thread(target=cron_loop, daemon=True, name="cron-tick").start()

    while True:                                   # 主 REPL 循环
        for msg in heartbeat.drain_output():     # ① 取心跳输出
            print_heartbeat(msg)
        for msg in cron_svc.drain_output():       # ② 取 cron 输出
            print_cron(msg)

        user_input = input(...)                   # ③ 读用户输入
        ...
        if user_input.startswith("/"):            # ④ 命令分派
            ...
            continue
        lane_lock.acquire()                       # ⑤ 阻塞抢锁
        try:
            ...跑用户对话...
        finally:
            lane_lock.release()
```

集成要点：

**①② 主循环每轮开头先 drain 两个队列**——把后台心跳/cron 产生的内容取出来打印。这样后台输出会在用户下次回车时（或 input 间隙）显示，不会和用户对话交错。`cron_stop.wait(timeout=1.0)` 既当 sleep 又能被 `set()` 唤醒——比 `time.sleep` 更好退出。

**⑤ 用户对话阻塞抢锁**——用户消息处理时持有 `lane_lock`，期间后台心跳/cron 全部让步。对比后台的非阻塞抢法（3.2），这里阻塞抢保证用户**最终必进**。

三个线程并发：
- 主线程（REPL + 用户对话）
- heartbeat 线程（1s 轮询 + 心跳执行）
- cron-tick 线程（1s tick + cron 执行）

共享 `lane_lock`。用户在线程里跑对话时占锁，后台两个都让步；用户在 `input()` 等待时（没占锁，因为锁在 try 块释放了），后台能跑。所以**用户「敲字时」后台可以跑，「对话处理时」后台让步**——精细的优先级。

---

## 8. REPL 命令

```sh
/heartbeat         # 心跳状态(启用/运行中/should_run/原因/上次跑/下次/间隔/活跃时段/队列)
/trigger           # 手动触发心跳(绕过间隔, 仍守锁)
/cron              # 列 cron 任务(ON/OFF 状态/错误数/下次)
/cron-trigger <id> # 手动触发某 cron 任务
/lanes             # 车道锁状态(main_locked / heartbeat_running)
/help              # 帮助
```

`/lanes` 用 `lane_lock.acquire(blocking=False)` 探测锁状态——抢到了说明没被占（立刻 release），抢不到说明被用户占着。这是个「非侵入探测」：用「试抢+立刻放」来查锁在不持有它。

`/heartbeat` 显示的 `reason` 字段尤其有用——告诉你「为什么心跳没跑」（剩余多少秒 / 不在时段 / already running），把 `should_run` 的判断透明化。

---

## 9. 和 s06 对照：主动层带来了什么

| 维度 | s06 Intelligence | s07 Heartbeat & Cron |
|------|-------------------|---------------------|
| agent 行为模式 | 纯被动 | 被动 + 主动 |
| 触发源 | 用户消息 | 用户消息 + 定时器/cron |
| 智能层 | 完整 8 层组装 | 简化版（SOUL+MEMORY） |
| 后台输出 | 无 | 队列 → 主线程 drain |
| 并发控制 | 无 | lane_lock 车道互斥 |
| 故障自保 | 无 | cron 连续 5 错熔断 |
| 可观测 | 无 | cron-runs.jsonl 日志 |

s07 的跃迁：从「**被动响应**」到「**主动自检 + 定点任务**」。agent 不再只是「被问到才答」，而是「会自己定期检查、到点干活、主动报告」。这是从「工具」到「助理」的质变——真正的助理会主动提醒你，而不是等你问。

技术上，s07 引入三个新东西：①车道互斥（用户优先的并发协调）；②后台输出队列（线程间安全输送）；③熔断+日志（定时任务的故障自保与可观测）。这些是「让 agent 在后台长期自主运行」的基础设施。

注意 s07 的智能层是**简化版**——它没复用 s06 的完整 8 层和混合搜索，因为 s07 的重点是「主动触发机制」而非「提示词工程」。生产代码会让心跳/cron 复用 s06 的完整智能层。

---

## 10. 留白与后续

| 留白 | 现状 | 谁来补 |
|------|------|--------|
| 输出投递 | 内存队列，进程崩就丢 | s08 的可靠投递队列（先写盘再发） |
| 并发序列化 | 单 lane_lock，所有后台共享一把锁 | s10 的命名车道（不同会话各自车道） |
| 心跳智能层 | 简化版 SOUL/MEMORY | 生产复用 s06 完整版 |
| 任务持久化 | CRON.json 启动加载，运行时改不持久 | 生产加运行时增删任务持久化 |
| 失败重试 | cron 失败只计数熔断，不重试 | s09 的重试洋葱 |
| 主动→外发 | 心跳/cron 输出只到 REPL | s08 让主动输出能发到 Telegram/飞书 |

s07 把「主动触发」跑通了，但主动产生的输出还只在本地 REPL 显示、还可能丢——这些可靠性问题留给 s08（Delivery：可靠投递）和 s10（Concurrency：命名车道）。s07 本身的「车道互斥 + 熔断 + 日志」三个机制，是让 agent 长期自主运行的最低保障。

---

## 11. 运行方法

```sh
cd claw0
python sessions/zh/s07_heartbeat_cron.py
```

需要 `.env` 配 `ANTHROPIC_API_KEY` 和 `MODEL_ID`，可选环境变量：

```sh
HEARTBEAT_INTERVAL=1800       # 心跳间隔(秒), 默认 30 分钟
HEARTBEAT_ACTIVE_START=9      # 活跃时段起, 默认 9
HEARTBEAT_ACTIVE_END=22       # 活跃时段止, 默认 22
```

工作区文件：
- `workspace/HEARTBEAT.md`：心跳指令（没有就不开心跳）。
- `workspace/CRON.json`：cron 任务（没有就空）。

试玩路径：

```sh
# 看心跳状态
/heartbeat        # enabled/should_run/reason/next_in...
/lanes            # main_locked / heartbeat_running

# 手动触发心跳(不等 30 分钟)
/trigger          # 立刻跑一次, 看输出

# 配 cron(workspace/CRON.json 写入):
{
  "jobs": [
    {"id": "daily", "name": "Daily Summary",
     "enabled": true,
     "schedule": {"kind": "cron", "expr": "0 9 * * *"},
     "payload": {"kind": "agent_turn", "message": "Generate a daily summary."}},
    {"id": "once", "name": "Reminder",
     "enabled": true, "delete_after_run": true,
     "schedule": {"kind": "at", "at": "2026-07-11T15:00"},
     "payload": {"kind": "system_event", "text": "该开会了"}}
  ]
}

/cron             # 列出任务
/cron-trigger daily   # 手动触发
```

观察后台输出会以 `[heartbeat]` / `[cron]` 前缀出现在 REPL——那是后台线程产生、经队列 drain 到主线程打印的。

---

## 12. 一句话总结

s07 = **主动层，让 agent 从「被动响应」变「主动出击」**：用**车道互斥**机制（用户 `lane_lock.acquire()` 阻塞抢锁总赢、后台心跳/cron `acquire(blocking=False)` 非阻塞抢不到就让步）保证用户消息永远优先；`HeartbeatRunner` 是 1 秒轮询的 daemon 线程，过 `should_run` 四项检查（HEARTBEAT.md 存在+非空 / 间隔已过 / 在活跃时段 / 没在跑，锁检测单独在 `_execute` 做`以避 TOCTOU）后抢锁跑 `run_agent_single_turn` 单轮 LLM（不带历史、不给工具、限长、容错），用 `HEARTBEAT_OK` 哨兵词过滤「没事」+ 和上次输出去重，有意义新内容经 `list+Lock` 输出队列线程安全地送到主线程 drain 打印；`CronService` 支持 at(一次性)/every(对齐锚点不漂移)/cron(表达式) 三种调度，每秒 `tick` 检查到期任务执行，连续 5 次错误自动熔断禁用、成功清零，每次跑写 `cron-runs.jsonl` 日志。三条车道（主线程对话 / 心跳线程 / cron-tick 线程）共享 `lane_lock`——用户敲字时后台可跑、用户处理对话时后台让步。它引入车道互斥、后台输出队列、熔断+日志三个基础设施，是 agent 长期自主运行的最低保障；输出投递可靠性、命名车道序列化分别留给 s08/s10，智能层用 s06 简化版（重点在触发机制而非提示词工程）。

result: 在 `docs/explain/heartbeat-cron.md` 写了第 7 节（心跳与 Cron）详细解释文档（约 13 节，含被动vs主动概念、车道互斥用户优先机制及 TOCTOU 竞态规避、HeartbeatRunner 四项检查+单轮LLM+HEARTBEAT_OK哨兵+去重+输出队列、run_agent_single_turn 单轮无工具无历史的克制设计、CronService 三种调度类型+对齐锚点+熔断+日志、agent_loop 三车道集成、s06 对照、留白、运行方法），现进入工作树提交并合并回 main。
