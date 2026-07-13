# 第 10 节详解：并发（Concurrency）

> 命名车道序列化混沌——用「按名分车道」替换单把锁，让并发有序、可观测、可重启。

本文是对 `sessions/zh/s10_concurrency.py`（900 行）和 `s10_concurrency.md` 的逐层深读。这是 claw0 的**终章**——把 s07 那把单 `lane_lock` 升级成**命名车道系统**。s07 用一把锁让用户优先，但所有后台任务挤同一条道；s10 给每种工作**单独的车道**，各自 FIFO 排队、各自并发上限、互不阻塞。

---

## 0. 一句话定位

s10 = **并发层**。它回答一个问题：**「多个来源的工作（用户对话 / 心跳 / cron / 自定义任务）同时涌来时，怎么让它们有序、不互相阻塞、又能保证该串行的串行？」**

答案是**命名车道（named lanes）**：

```
Incoming Work
    |
CommandQueue.enqueue(lane_name, fn)        ← 按名分车道
    |
+--------+    +--------+    +-----------+
| main   |    |  cron  |    | heartbeat |   ← 三条标准车道(各自独立)
| max=1  |    | max=1  |    |   max=1   |
| FIFO   |    | FIFO   |    |   FIFO    |
+---+----+    +---+----+    +-----+-----+
    |             |               |
[active]      [active]        [active]       ← 各自的活跃任务计数
    |             |               |
_task_done   _task_done      _task_done      ← 完成后自泵送下一个
    |             |               |
_pump()       _pump()         _pump()
```

每个车道是一个**独立 FIFO 队列 + 独立并发上限**。不同来源的工作进不同车道、互不阻塞；同一车道内按 `max_concurrency` 控制并行度（默认 1 = 串行，保证同车道任务有序）。

---

## 1. 架构总览

核心组件：

| 组件 | 代码位置 | 职责 |
|------|---------|------|
| `LaneQueue` | `:100-203` | 单个命名车道：FIFO deque + Condition + 活跃计数 + generation |
| `CommandQueue` | `:211-267` | 中央调度器：按名路由、惰性创建车道、reset_all |
| `HeartbeatRunner` | `:351-491` | s07 心跳的 lane 化版：进 heartbeat lane 而非抢锁 |
| `CronService` | `:499-603` | s07 cron 的 lane 化版：进 cron lane |
| `run_agent_single_turn` | `:333-343` | 单轮 LLM 调用（心跳/cron/自定义 enqueue 共用） |

三条标准车道（`:91-93`）：

```python
LANE_MAIN = "main"          # 用户对话
LANE_CRON = "cron"          # 定点任务
LANE_HEARTBEAT = "heartbeat"  # 周期巡检
```

用户还可 `/enqueue <自定义lane名>` 动态创建车道。

---

## 2. 先理解 s07 单锁的问题，才知道 s10 为什么需要车道

s07 用一把共享 `lane_lock`：用户阻塞抢（总赢）、后台非阻塞抢（抢不到让步）。它解决了「用户优先」，但有三个局限：

**① 所有后台工作挤同一条道**
心跳和 cron 都抢同一把锁——如果心跳在跑，cron 到点了也抢不到、只能让步。两种本该独立的工作互相阻塞。

**② 同一来源的多条消息无法并行，也无法可控地串行**
多个 cron 任务同时到点，s07 没有机制说「cron 任务串行跑、别互相干扰」——它们要么抢锁乱序、要么一个个让步。

**③ 没有可观测性**
s07 的 `/lanes` 只能查一把锁的状态。你想知道「heartbeat 车道排了几条、cron 车道有几个在跑」——没有，因为只有一条道。

s10 的解法：**给每种工作单独的车道**——
- main lane：用户对话，max=1（一次处理一条，保证对话有序）；
- cron lane：cron 任务，max=1（cron 任务串行，不互相干扰）；
- heartbeat lane：心跳，max=1。

三条车道**独立**：心跳在跑不影响 cron 到点执行、cron 在跑不影响用户对话。同车道内 max=1 串行、可改成更大值允许并行。这就是「**命名车道序列化混沌**」——把乱七八糟的并发工作按名分流、各自有序。

---

## 3. LaneQueue：单个车道的核心原语

```python
class LaneQueue:
    def __init__(self, name, max_concurrency=1):
        self.name = name
        self.max_concurrency = max(1, max_concurrency)
        self._deque = deque()                 # [(fn, future, generation), ...]
        self._condition = threading.Condition()
        self._active_count = 0                # 当前在跑几个
        self._generation = 0                  # 代号(重启恢复用)
```

一个车道 = **deque（FIFO 队列）+ Condition（同步）+ 活跃计数 + generation**。四个字段各司其职，下面逐个讲。

### 3.1 deque：FIFO 队列

```python
self._deque: deque[tuple[Callable, concurrent.futures.Future, int]] = deque()
```

队列里每项是三元组 `(fn, future, gen)`：
- `fn`：要执行的 callable（无参，返回结果）；
- `future`：`concurrent.futures.Future`，结果/异常的载体；
- `gen`：入队时的 generation 快照（重启恢复用，见第 7 节）。

`deque` 是双端队列，`popleft()` 从头取——**FIFO**，先入先出，保证同车道任务按顺序执行。这是「序列化」的基础。

### 3.2 Condition：比 Lock 更强的同步原语

```python
self._condition = threading.Condition()
```

s07 用 `threading.Lock`，s10 升级成 `threading.Condition`。区别：`Lock` 只能互斥（保护临界区），`Condition` 在互斥之上还能**等待/通知**——`wait()` 挂起等条件满足、`notify_all()` 唤醒等待者。

s10 用 Condition 是因为需要 `wait_for_idle()`（`:179`）：等待「车道空了」这件事。用 Lock 只能轮询（反复查 active_count），低效；用 Condition 能挂起睡眠、等任务完成的 `notify_all` 唤醒，零 CPU 开销。这是优雅关停的基础（第 9 节）。

### 3.3 active_count + max_concurrency：并发上限

```python
self._active_count = 0
self.max_concurrency = max(1, max_concurrency)   # 至少 1
```

`active_count` 是「当前这个车道有几个任务在跑」。`max_concurrency` 是上限。`_pump` 的核心判断就是 `active_count < max_concurrency`——还有空闲槽位才启动新任务。

- `max=1`：串行，同一车道一次只跑一个（保证顺序）；
- `max=3`：最多 3 个并行（适合无状态、可并行的任务）。

默认 1 是刻意的：**大多数车道要的是顺序保证**（用户对话要按顺序、cron 任务要串行不互相干扰），并行是特例。要并行显式调大。

---

## 4. _pump 自泵送：无外部调度器的引擎

这是 s10 最精巧的设计。看 `enqueue` 和 `_pump`：

```python
def enqueue(self, fn, generation=None):
    future = concurrent.futures.Future()
    with self._condition:
        gen = generation if generation is not None else self._generation
        self._deque.append((fn, future, gen))
        self._pump()                    # ← 入队立刻尝试泵
    return future

def _pump(self):
    """从 deque 弹出任务并启动, 直到 active >= max 或 deque 空.
    调用时必须持有 _condition."""
    while self._active_count < self.max_concurrency and self._deque:
        fn, future, gen = self._deque.popleft()
        self._active_count += 1
        t = threading.Thread(target=self._run_task, args=(fn, future, gen),
                             daemon=True, name=f"lane-{self.name}")
        t.start()
```

`_pump` 干的事：**「只要还有空闲槽位（active < max）且队列非空，就弹出下一个任务、起线程跑它」**。

关键是**谁来调 `_pump`**——s10 的答案是「**没有外部调度器，泵送是事件驱动的**」：

1. **入队时** `enqueue` 调一次 `_pump`：新任务来了，看能不能立刻跑。
2. **任务完成时** `_task_done` 调一次 `_pump`：腾出一个槽位，看队列里有没有等着的可以接着跑。

这两点构成「**自泵送（self-pumping）**」——不需要一个后台线程不停扫描「有没有任务该跑」，而是**入队和完成这两个事件自然触发泵送**。没有外部调度器循环，省 CPU、反应快。

对比「外部调度器」方案（一个线程每秒扫所有车道看哪个该跑）：那样有延迟（最多 1 秒才反应）、有空转开销。s10 的事件驱动泵送是「来了就泵、完成就泵」，零延迟零空转。

### 4.1 _run_task + _task_done：执行与收尾

```python
def _run_task(self, fn, future, gen):
    try:
        result = fn()             # 执行 callable
        future.set_result(result) # 结果塞进 Future
    except Exception as exc:
        future.set_exception(exc) # 异常塞进 Future(不抛, 让调用方取)
    finally:
        self._task_done(gen)     # 收尾: 必调

def _task_done(self, gen):
    with self._condition:
        self._active_count -= 1
        if gen == self._generation:   # 当前代才泵(见第7节)
            self._pump()
        self._condition.notify_all()  # 唤醒 wait_for_idle 的等待者
```

`_run_task` 在**自己的线程**里跑 `fn`——所以同车道的多个任务（如果 max>1）是真并行（各自线程）。`fn()` 的返回值/异常塞进 `Future`，调用方通过 `future.result()` 取。`finally` 保证 `_task_done` 必调——无论成功失败都要收尾（腾槽位 + 泵下一个 + 唤醒等待者）。

---

## 5. Future：结果返回机制

```python
def enqueue(self, fn, generation=None) -> concurrent.futures.Future:
    future = concurrent.futures.Future()
    ...
    return future
```

每次 `enqueue` 返回一个 `concurrent.futures.Future`——这是 Python 标准库的「**未来结果容器**」。它代表「一个异步进行中的计算，结果将来会有」。

调用方两种用法：

**① 阻塞等结果**：
```python
future = cmd_queue.enqueue(LANE_MAIN, user_turn_fn)
result_text = future.result(timeout=120)   # 阻塞等, 最多 120s
```
`future.result()` 阻塞当前线程直到任务完成、返回结果（或抛异常）。s10 的用户对话就这么用——主线程把用户回合塞 main lane、然后 `future.result()` 等。这和「同步调用」体验一样，但底下任务在车道线程里跑、和别的车道并发。

**② 回调通知**：
```python
future.add_done_callback(_on_done)   # 任务完成时异步调 _on_done
```
不阻塞、注册回调，任务完成时自动调。s10 的心跳/cron 用这个——后台任务完成不用人盯着、完成后回调把结果塞进输出队列。

`Future` 是「**调用方和执行方解耦**」的桥梁：入队方拿 Future、执行方填 Future、双方不用直接打交道。这是 s10 相对 s07（直接调函数/抢锁）的抽象升级。

---

## 6. 三条标准车道：分流与隔离

```python
# agent_loop, :628-631
cmd_queue = CommandQueue()
cmd_queue.get_or_create_lane(LANE_MAIN, max_concurrency=1)
cmd_queue.get_or_create_lane(LANE_CRON, max_concurrency=1)
cmd_queue.get_or_create_lane(LANE_HEARTBEAT, max_concurrency=1)
```

三条车道都 max=1（串行）。分流后各干各的：

- **main lane**：用户对话。一次一条、串行处理——保证用户消息按顺序、不丢。主线程入队后 `future.result()` 阻塞等。
- **cron lane**：cron 任务。串行——多个 cron 同时到点也一个个跑、不互相干扰。
- **heartbeat lane**：心跳巡检。串行——上一轮没完不会再来挤。

**隔离的价值**：心跳在跑（占 heartbeat lane）**不影响**用户对话（main lane）——两者不同车道、各跑各的。对比 s07：心跳抢锁时用户得让步（因为同一把锁）。s10 让「用户优先」从「心跳让步」升级成「根本不同道、无需让步」。

---

## 7. Generation 追踪：重启恢复的精妙

这是 s10 最微妙的设计。问题场景：**任务正在跑，系统重启了，旧任务的完成回调回来「泵送」队列——会怎样？**

假设没有 generation：

```
t1: main lane 入队任务A、B, A 开始跑
t2: 系统重启(进程崩了或 reset), 队列被清空, 重新开始
t3: 新代码入队任务C、D, C 开始跑
t4: 旧的 A 终于完成了(它的线程还在), 回调 _task_done
    → active_count -= 1 (但这是新世界的 active_count!)
    → _pump() → 把 D 弹出来跑  ← 旧任务污染了新队列!
```

旧任务的完成回调**污染了重启后的状态**——它减的是新 active_count、泵的是新队列，把不该启动的任务启动了。这种「僵尸任务」在重启后捣乱，很难排查。

s10 的解法：**每个车道有个 generation 计数器，入队时快照当前 gen、完成时比对**。

```python
def enqueue(self, fn, generation=None):
    gen = generation if generation is not None else self._generation   # 快照当前 gen
    self._deque.append((fn, future, gen))

def _task_done(self, gen):
    with self._condition:
        self._active_count -= 1
        if gen == self._generation:    # 同代才泵
            self._pump()
        # else: 过期任务, 静默死去, 不泵
```

`reset_all`（重启模拟）递增所有车道的 generation：

```python
def reset_all(self):
    with self._lock:
        for name, lane in self._lanes.items():
            with lane._condition:
                lane._generation += 1     # 代号 +1
```

重启后 gen 从 0 变 1。旧任务（gen=0）完成时 `gen(0) != self._generation(1)` → **不泵**、只默默减 active_count（即使减错了也无害，因为不触发泵送）。新任务（gen=1）正常泵送。

效果：**旧 generation 的任务完成回调变成无害的空操作**——不影响新队列。这就是「过期任务不会在重启后排空队列」的保证。

`/reset` 命令演示这个：敲 `/reset` 递增所有 gen，之后旧任务完成不会泵新队列。这是生产级「重启安全」的教学版。

---

## 8. wait_for_idle + Condition.wait：高效空闲检测

```python
def wait_for_idle(self, timeout=None):
    deadline = (time.monotonic() + timeout) if timeout is not None else None
    with self._condition:
        while self._active_count > 0 or len(self._deque) > 0:
            remaining = None
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
            self._condition.wait(timeout=remaining)   # 挂起, 不轮询
        return True
```

「等车道空」。`while` 循环检查「还有活跃任务或队列非空」——是就 `self._condition.wait()` 挂起，等被唤醒再查。

关键：`wait()` 释放锁、挂起线程、零 CPU，等 `notify_all`（在 `_task_done` 里调）唤醒它。这是 Condition 相对 Lock 的优势——Lock 只能 `while True: sleep(0.1)` 轮询（忙等、有延迟），Condition 是真睡眠、零开销、即时唤醒。

`CommandQueue.wait_for_all`（`:247`）聚合所有车道：等每条都 idle。s10 在退出时调它（`:883` `cmd_queue.wait_for_all(timeout=3.0)`）——优雅关停，给在跑的任务 3 秒收尾，不会把任务砍在半截。

---

## 9. CommandQueue：中央调度器

```python
class CommandQueue:
    def __init__(self):
        self._lanes: dict[str, LaneQueue] = {}
        self._lock = threading.Lock()

    def get_or_create_lane(self, name, max_concurrency=1):
        with self._lock:
            if name not in self._lanes:                    # 惰性创建
                self._lanes[name] = LaneQueue(name, max_concurrency)
            return self._lanes[name]

    def enqueue(self, lane_name, fn):
        lane = self.get_or_create_lane(lane_name)          # 按名找/建车道
        return lane.enqueue(fn)                            # 委托给 LaneQueue

    def reset_all(self):
        ...递增所有 gen...

    def stats(self):
        return {name: lane.stats() for name, lane in self._lanes.items()}  # 全车道统计
```

`CommandQueue` 是个**路由器**：`enqueue(lane_name, fn)` 按名找到/建对应 `LaneQueue`，把 `fn` 塞进去。它自己不调度、不执行——只是「按名分流的入口」。

**惰性创建**：车道第一次被引用时才建（`if name not in self._lanes`）。所以你 `/enqueue research xxx` 会当场创建一个叫 `research` 的新车道——动态扩展，不用预先声明。

`self._lock` 只保护「车道字典的增删查」（短临界区），不保护车道内部——车道内部用各自的 `_condition`。这是**分层锁**：外层锁护字典、内层锁护车道，减少争用。

---

## 10. HeartbeatRunner：lane 感知的跳过

s07 心跳用 `lock.acquire(blocking=False)` 抢不到就让步。s10 改成「检查 heartbeat 车道忙不忙」：

```python
def heartbeat_tick(self):
    ok, reason = self.should_run()          # s07 同款 4 检查
    if not ok:
        return

    lane_stats = self.command_queue.get_or_create_lane(LANE_HEARTBEAT).stats()
    if lane_stats["active"] > 0:             # 车道有活跃任务 → 跳过本轮
        return                              # (上一轮还没完, 不重叠)

    future = self.command_queue.enqueue(LANE_HEARTBEAT, _do_heartbeat)
    future.add_done_callback(_on_done)      # 完成回调: 设 last_run_at、去重、入输出队列
```

「lane 忙就跳过」在功能上等同 s07 的「锁抢不到就让步」——都是「上一轮没完就不重叠跑」。但用**车道抽象**表达：查 heartbeat 车道的 `active` 计数，而不是抢全局锁。

`_on_done` 回调（`:431`）做 s07 同款的事：更新 `last_run_at`、去重（和 `_last_output` 比）、有意义新内容进输出队列。区别只是「在车道任务的完成回调里做」而非「`_execute` 里同步做」——因为现在心跳是车道里的一个异步任务。

---

## 11. CronService：进 cron lane

```python
def _enqueue_job(self, job, now):
    def _do_cron():
        sys_prompt = "You are performing a scheduled background task..."
        return run_agent_single_turn(message, sys_prompt)

    future = self.command_queue.enqueue(LANE_CRON, _do_cron)   # 进 cron 车道

    def _on_done(f, j=job, n=job_name):
        j["last_run_at"] = time.time()
        j["next_run_at"] = time.time() + j["every_seconds"]
        try:
            result = f.result()
            j["consecutive_errors"] = 0
            if result:
                self._output_queue.append(f"[{n}] {result}")
        except Exception as exc:
            j["consecutive_errors"] += 1
            if j["consecutive_errors"] >= 5:    # 连续5错熔断
                j["enabled"] = False

    future.add_done_callback(_on_done)
```

和 s07 cron 同款（连续 5 错熔断、完成回调更新 job 状态），区别是任务**进 cron 车道**而非直接跑。多个 cron 同时到点时，cron 车道 max=1 让它们**串行**——一个个跑、不互相干扰。这是 s10 相对 s07 的并发改进：cron 任务有了自己的道、且有序。

---

## 12. 用户对话：进 main lane + 阻塞等 Future

```python
# agent_loop, :813-879
def _make_user_turn(user_msg, msgs, sys_prompt, tool_handler):
    def _turn():
        msgs.append({"role": "user", "content": user_msg})
        while True:
            response = client.messages.create(...)        # 工具循环(和 s04/s06 同构)
            msgs.append({"role": "assistant", ...})
            if response.stop_reason == "end_turn":
                return final_text
            elif response.stop_reason == "tool_use":
                ...塞 tool_result, continue
    return _turn

print_lane(LANE_MAIN, "processing...")
future = cmd_queue.enqueue(LANE_MAIN, _make_user_turn(...))   # 入 main 车道
try:
    result_text = future.result(timeout=120)                  # 阻塞等结果
    print_assistant(result_text)
except concurrent.futures.TimeoutError:
    print("Request timed out.")
```

用户消息被包成一个 callable `_turn`（内部是标准工具循环），塞进 **main 车道**，主线程 `future.result(timeout=120)` 阻塞等。

关键变化对比 s07：s07 主线程直接 `client.messages.create` 跑（持 lane_lock）；s10 主线程**把整个回合委托给 main 车道**，自己只等 Future。这看起来绕，但好处是：
- main 车道 max=1 自动保证用户消息**串行**（不会两个用户回合并发改 messages）；
- 用户回合在车道线程跑、和后台车道并发，但 main 车道独立、后台不抢 main 的资源；
- `timeout=120` 给了超时保护（s07 没有超时）。

主线程在 `future.result()` 阻塞期间，后台心跳/cron 车道照样跑——因为是不同车道、不同线程、不共享 main 的 messages。这是 s10 「用户和后台真正并行」的实现。

---

## 13. agent_loop 集成

```python
def agent_loop():
    cmd_queue = CommandQueue()
    cmd_queue.get_or_create_lane(LANE_MAIN, max_concurrency=1)
    cmd_queue.get_or_create_lane(LANE_CRON, max_concurrency=1)
    cmd_queue.get_or_create_lane(LANE_HEARTBEAT, max_concurrency=1)

    heartbeat = HeartbeatRunner(workspace=..., command_queue=cmd_queue, ...)
    cron_svc = CronService(..., cmd_queue)
    heartbeat.start()                         # 心跳后台线程(每秒 tick, 入 heartbeat 车道)

    cron_stop = threading.Event()
    def cron_loop():
        while not cron_stop.is_set():
            cron_svc.cron_tick()              # cron 后台线程(每秒 tick, 入 cron 车道)
            cron_stop.wait(timeout=1.0)
    threading.Thread(target=cron_loop, daemon=True).start()

    while True:
        for msg in heartbeat.drain_output():  # 主循环开头 drain 两个后台输出队列
            print_lane(LANE_HEARTBEAT, msg)
        for msg in cron_svc.drain_output():
            print_lane(LANE_CRON, msg)

        user_input = input(...)
        ...
        future = cmd_queue.enqueue(LANE_MAIN, _make_user_turn(...))
        result_text = future.result(timeout=120)   # 主线程阻塞等用户回合

    heartbeat.stop()
    cron_stop.set()
    cmd_queue.wait_for_all(timeout=3.0)       # 优雅关停: 等所有车道 idle
```

三个执行来源 + 三条车道：
- 主线程：用户消息 → main 车道 → 阻塞等 Future；
- heartbeat-timer 线程：每秒 tick → 入 heartbeat 车道；
- cron-tick 线程：每秒 tick → 入 cron 车道。

每条车道有自己的线程池（每个任务一个 daemon 线程），三套独立。主循环开头 drain 心跳/cron 输出（和 s07 同款机制）。退出时 `wait_for_all` 等所有车道 idle，优雅关停。

---

## 14. REPL 命令

```sh
/lanes                    # 所有车道状态(name/active条/max/queued/gen) — 可观测性
/queue                    # 各车道待处理条目
/enqueue <lane> <msg>     # 手动入队到任意车道(可创建新车道)
/concurrency <lane> <N>   # 改某车道 max_concurrency(动态调并行度)
/generation               # 看各车道 generation 计数器
/reset                    # 模拟重启(reset_all, 递增所有 gen)
/heartbeat                # 心跳状态
/trigger                  # 手动触发心跳
/cron                     # 列 cron 任务
```

`/lanes` 是 s10 的可观测性核心——一条命令看清所有车道的活跃/排队/上限/代号。`/enqueue` 让你手动往任意车道塞任务（包括自定义名，会惰性建新车道）。`/concurrency` 动态调并行度——比如把 research 车道从 1 调到 3，让研究任务并行。`/reset` 演示 generation 机制。

这些命令把「车道系统」从黑盒变白盒——你能看见、能操作并发结构，这是 s10 相对 s07（只有一把锁、看不清）的工程价值。

---

## 15. 和 s07 对照：车道系统带来了什么

| 维度 | s07 单锁 | s10 命名车道 |
|------|---------|------------|
| 并发协调 | 一把 `lane_lock`，所有工作抢同把 | 多车道，各干各的 |
| 用户优先 | 心跳/cron 让步（抢不到锁） | 根本不同道、无需让步 |
| 同源任务有序 | 无（cron 任务乱抢） | 同车道 max=1 串行 |
| 并行度控制 | 无 | 每车道 `max_concurrency` 可配 |
| 可观测 | 只有锁状态 | 每车道 active/queued/gen |
| 结果返回 | 直接调/输出队列 | `Future`（阻塞等或回调） |
| 重启安全 | 无 | generation 追踪过期任务 |
| 空闲等待 | 无 | `wait_for_idle`（Condition） |
| 动态扩展 | 无 | 惰性创建自定义车道 |

s10 的跃迁：从「**一把锁的让步**」到「**多车道的分流**」。用户优先不再靠「后台让步」实现，而是靠「根本不在同一条道」；并发不再是一锅粥，而是按名分流、各自有序、可配并行度、可观测、可重启恢复。这是从「能并发」到「**有序可控的并发**」的质变。

---

## 16. 留白（教学版简化）

| 方面 | s10 教学 | 生产 |
|------|---------|------|
| 任务执行 | 每任务一个 `threading.Thread` | 线程池 + 有界 worker（防线程爆炸） |
| 车道配置 | 三条标准 + 动态自定义 | + 插件定义车道、配置文件 |
| 指标 | `stats()` 基础计数 | + 完成时间、延迟、吞吐量指标采集 |
| 重启恢复 | generation 内存计数 | + 持久化队列状态（跨进程重启） |
| 背压 | 无界 deque | + 有界队列 + 拒绝策略 |
| 优先级 | 车道间平等 | + 车道间优先级调度 |

s10 是教学版的「车道骨架」——核心模式（命名车道 + FIFO + max + Future + generation + Condition）从教学到生产基本不变，但生产要加线程池（防每任务一线程爆炸）、有界队列（防内存爆）、指标采集、持久化恢复等。

---

## 17. 运行方法

```sh
cd claw0
python sessions/zh/s10_concurrency.py
```

需要 `.env` 配 `ANTHROPIC_API_KEY`、`MODEL_ID`，可选环境变量 `HEARTBEAT_INTERVAL`、`HEARTBEAT_ACTIVE_START/END`。

试玩路径：

```sh
/lanes                # 看三条标准车道状态
/enqueue main 法国首都?         # 手动往 main 车道塞任务
/enqueue research 总结最新AI进展   # 创建并塞进 research 新车道
/concurrency research 3       # 把 research 车道并行度调到 3
/lanes                # 看 research 车道 max=3
/queue                # 看各车道排队
/generation           # 看 gen 计数器(都 0)
/reset                # 模拟重启, gen 都 +1
/generation           # 再看(都 1)——旧任务完成不会泵新队列
/heartbeat / /trigger / /cron   # 心跳/cron 状态
```

观察输出会以 `[main]`/`[cron]`/`[heartbeat]`/`[research]` 前缀带颜色出现——每个车道一种颜色，直观看到「不同车道并发、同车道串行」。

---

## 18. 10 章总图收尾

s10 是 claw0 的终章。把 10 章连起来看：

```
s01: while True + stop_reason            (循环)        ← agent 的心跳
s02: TOOLS + TOOL_HANDLERS              (执行)        ← 分发表
s03: JSONL + ContextGuard              (持久化)      ← 会话不丢
s04: Channel ABC + InboundMessage      (通道)        ← 多平台接入
s05: BindingTable + session_key        (路由)        ← 找到对的 agent
s06: 8 层 prompt + hybrid search       (智能)        ← 灵魂与记忆
s07: Heartbeat + Cron                  (自治)        ← 主动行为
s08: DeliveryQueue + backoff           (可靠投递)    ← 出站不丢
s09: 3 层重试洋葱 + profiles           (韧性)        ← 入站能跑完
s10: Named lanes + generation          (并发)        ← 有序可控的并发
```

s01 那个 `while True` 循环，到了 s10 的核心依然清晰可辨——只是它现在跑在一个命名车道里、被 Future 包裹、和别的车道并发、有 generation 保护。**AI agent 就是一个 `while True` 循环加上一张分发表，外面包裹着持久化、路由、智能、调度、可靠性、韧性和并发控制的层层机制**——这就是 claw0 从零到一要教的所有东西。

---

## 19. 一句话总结

s10 = **并发层，用「命名车道（LaneQueue）」替换单把锁**：每条车道是独立的 FIFO deque + `threading.Condition` + active_count + max_concurrency + generation 计数器，`CommandQueue` 按名路由、惰性创建车道；任务以 callable 入队、返回 `concurrent.futures.Future`（调用方可 `result()` 阻塞等或 `add_done_callback` 回调）；**自泵送机制**——入队和任务完成时各调一次 `_pump`，没有外部调度器、事件驱动零延迟；同车道 `max_concurrency`（默认 1 串行、可调大并行）控制并行度；**generation 追踪**——入队快照 gen、完成时比对，`reset_all` 递增所有 gen 让旧任务的完成回调变无害空操作、不污染重启后的队列；`wait_for_idle` 用 Condition.wait 高效睡眠等空闲（优雅关停）；三条标准车道 main/cron/heartbeat 各自独立、用户优先从「后台让步」升级成「根本不同道」、后台与用户真正并行。它把 s07 的「一把锁让步」升级成「多车道分流」，从「能并发」到「**有序可控、可观测、可重启恢复的并发**」——核心模式（命名车道+FIFO+max+Future+generation+Condition）从教学到生产基本不变，生产加线程池/有界队列/指标/持久化。作为 claw0 终章，它把 s01 的 `while True` 循环包进了车道和 Future——AI agent 就是一个循环加一张分发表，外面包着持久化/路由/智能/调度/可靠性/韧性/并发层层机制。

result: 在 `docs/explain/concurrency.md` 写了第 10 节（并发）详细解释文档（约 19 节，含命名车道替换单锁的动机、LaneQueue 核心原语 deque/Condition/active_count/generation、_pump 自泵送无外部调度器、max_concurrency 并行度、Future 结果返回、generation 追踪重启恢复、wait_for_idle+Condition 高效空闲、CommandQueue 惰性创建、三条标准车道分流、HeartbeatRunner/CronService lane 化、用户对话进 main 车道阻塞等 Future、agent_loop 集成、REPL 命令、s07 对照、留白、运行方法、10 章总图收尾），现进入工作树提交并合并回 main。
