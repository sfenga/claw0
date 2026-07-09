# Python 异步并发知识详解

> 从「为什么要并发」到 asyncio 实战陷阱，一篇打通。文中的例子尽量和 claw0 s05 的真实用法（共享事件循环、`Semaphore`、`asyncio.to_thread`、`run_coroutine_threadsafe`）对应。

---

## 0. 先搞清三个词：并发 / 并行 / 异步

很多人混着用，但它们是三件事：

| 词 | 含义 | Python 里的体现 |
|----|------|----------------|
| **并发 (concurrency)** | 「同时处理」多个任务，但**不一定同时执行**——可以是交替进行 | 多线程、asyncio 都算并发 |
| **并行 (parallel)** | 多个任务**在同一时刻真正同时执行**，需要多核 | 多进程 `multiprocessing` |
| **异步 (async)** | 一种**实现并发**的方式：用事件循环调度协程，任务等 IO 时让出 CPU 给别的任务 | `asyncio` |

一句话区分：**并发是「能处理多件事」的能力，并行是「同时在算多件事」的能力，异步是「靠事件循环 + 协程来达成并发」的一种具体手段。** asyncio 是「并发但不并行」——单线程里交替跑。

---

## 1. 为什么要并发：IO 密集 vs CPU 密集

程序做的事分两类：

- **IO 密集（IO-bound）**：大部分时间在**等**——等网络响应、等磁盘读写、等用户输入。CPU 其实闲着。
  - 典型：调 API、爬网页、读写文件、数据库查询、聊天机器人（claw0 就是）。
- **CPU 密集（CPU-bound）**：大部分时间在**算**——大量计算占满 CPU。
  - 典型：图像处理、数值模拟、压缩加密。

并发的价值几乎全在 IO 密集场景：**等 IO 的时候 CPU 闲着也是闲着，不如去干别的任务。** 比如 agent 调 LLM API 要等 5 秒，这 5 秒里完全可以处理另一条用户消息。这正是 s05 引入 asyncio 的理由——网关要同时伺候多个客户端，每个都在等 API。

CPU 密集场景并发帮不上（CPU 已经满了，让出也给不出算力），要靠**并行**（多进程）才真快。

> 结论：**IO 密集 → asyncio/多线程；CPU 密集 → 多进程。** claw0 全是网络 IO，所以用 asyncio。

---

## 2. Python 并发的三套马车

| 方案 | 模块 | 是否真并行 | 是否受 GIL 限制 | 适合 |
|------|------|----------|----------------|------|
| 多线程 | `threading` | 否（伪并行） | 是 | IO 密集（兼容旧同步库） |
| 多进程 | `multiprocessing` | 是 | 否（各自独立解释器） | CPU 密集 |
| 异步 | `asyncio` | 否（单线程调度） | 是但影响小（await 让出） | IO 密集（高并发、可控） |

### 2.1 GIL：绕不开的前提

CPython（最常见的 Python 实现）有 **GIL（全局解释器锁）**：同一时刻**只有一个线程在执行 Python 字节码**。所以多线程在 Python 里**不能真并行算 CPU 任务**——两个线程算数学，还是轮流用一个核，甚至更慢（锁争抢开销）。

但 GIL 在 **IO 等待时会释放**——线程调 `recv()`/`read()` 等系统调用等数据时，GIL 放开，别的线程能趁机跑。所以多线程对 IO 密集**有用**，只是和 asyncio 相比开销大（每线程占内存、切换成本高），高并发时（几百上千连接）扛不住。

asyncio 用「单线程 + 协程」绕开线程开销：**一个线程，几万个协程，靠事件循环调度**，谁等 IO 就挂起谁、谁就绪就跑谁。没有线程切换成本，没有 GIL 争抢（只有一个线程）。

### 2.2 为什么 claw0 两种都用

s05 的设计是「**同步 REPL 主线程 + 后台 asyncio 线程**」混用：

- 主线程要 `input()`（同步阻塞）→ 用同步循环最简单；
- 后台要并发跑多 agent → 用 asyncio；
- 两边用 `run_coroutine_threadsafe` 桥接。

这是「**异步不是全有或全无**」的典型——你可以在一个程序里既保留同步部分（不想改的老代码、阻塞交互），又在需要并发的地方用 asyncio。关键就是知道怎么桥接（见第 8 节）。

---

## 3. asyncio 的三块基石：协程 / 事件循环 / await

### 3.1 协程（coroutine）：能暂停的函数

```python
async def greet(name):
    await asyncio.sleep(1)        # ← 这里「暂停」，把控制权还给事件循环
    return f"hello, {name}"
```

`async def` 定义的是**协程函数**，调用它**不会立刻执行**，而是返回一个**协程对象**：

```python
c = greet("world")   # 此时啥都没跑，只是造了个协程对象
print(c)              # <coroutine object greet at 0x...>
```

协程对象必须被**事件循环调度**才会跑（`await` 它、或 `create_task`、或 `asyncio.run`）。这是新手最大的坑：**写了 `async def` 就以为它跑了——其实没有。**（见第 10 节陷阱 1）

### 3.2 事件循环（event loop）：总调度

事件循环是一个不停转的对象，负责「监视等待中的协程，谁就绪了就恢复谁」。asyncio 的核心就是它。**没有事件循环，协程就是一坨死代码。**

三件事必须搞懂：

- **`asyncio.run(coro)`**：便捷入口。建一个临时事件循环，跑完 `coro`，关掉循环。适合「顶层入口跑一次」的场景（比如脚本的 `main()`）。**一个进程里通常只调一次。**
- **`asyncio.new_event_loop()` + `loop.run_forever()`**：手动建一个**常驻**循环，永远转着等任务。适合「循环要长期存在、由别的线程往里塞任务」的场景——**s05 的后台线程就这么干的**。
- **`asyncio.get_event_loop()`**：拿到「当前线程的」事件循环。Python 3.10+ 在没有运行循环的线程里调它会警告/报错，所以新代码少用，改用 `new_event_loop()` 显式建。

s05 选了第二种（`new_event_loop` + `run_forever` + daemon 线程），因为循环要长期常驻、等主线程随时往里塞协程。

### 3.3 `await`：让出控制权

`await` 是协程里最关键的关键字，它的语义是：**「我在等这个异步操作，期间我把 CPU 让给事件循环去跑别的协程，等它好了再回来继续。」**

```python
async def fetch():
    data = await network_request()   # 等网络时，事件循环可以去跑别的协程
    return data
```

如果没有 `await`，协程里哪怕调了耗时的同步函数，**也不会让出**——整个事件循环被它霸占，别的协程全停摆。这是第 10 节陷阱 2 的根源。

> 一句话理解 asyncio：**协程是能 `await` 暂停的函数，事件循环是负责在协程暂停时去跑别人、在就绪时恢复它的总调度，`await` 是「我让出，你先跑」的信号。**

---

## 4. 调度多任务：`create_task` / `gather` / `wait` / `as_completed`

这是异步并发真正「并发」起来的地方——多个协程同时被循环调度。

### 4.1 `asyncio.create_task(coro)`：把协程变成可被调度的任务

```python
task = asyncio.create_task(do_work())   # 立即排入事件循环，开始跑（遇到 await 才真正并发）
```

`create_task` 把协程**包装成 Task** 排进循环，循环会在合适的时机跑它。注意：**你必须把返回的 Task 存住**（赋值给变量、放进集合），否则它可能被垃圾回收掉、被中途取消——这是著名陷阱（第 10 节陷阱 4）。

### 4.2 `asyncio.gather(*coros)`：等一批协程全跑完

```python
results = await asyncio.gather(fetch_a(), fetch_b(), fetch_c())
# results = [a结果, b结果, c结果]，顺序和传入一致
```

并发跑三个，等全部完成，按**传入顺序**返回结果（不是完成顺序）。这是最常用的「并发等一群」。

### 4.3 `asyncio.wait(coros)`：更细的控制

```python
done, pending = await asyncio.wait(tasks, return_when=FIRST_COMPLETED)
```

返回「已完成的」和「未完成的」两个集合，可以指定「第一个完成就返回」(`FIRST_COMPLETED`)、「全部完成」(`ALL_COMPLETED`)、「任意一个异常」(`FIRST_EXCEPTION`)。比 `gather` 灵活，但返回值结构也乱，日常用 `gather` 更多。

### 4.4 `asyncio.as_completed(coros)`：谁完成就先处理谁

```python
for coro in asyncio.as_completed([fetch_a(), fetch_b(), fetch_c()]):
    result = await coro    # 先完成的先拿到
    print(result)
```

按**完成顺序**迭代——谁先回来先处理谁。适合「谁快用谁」的场景（比如多源竞速取最快响应）。

---

## 5. 同步原语：协程之间的协调

多个协程并发跑时，需要协调（互斥、限流、通知、传数据），asyncio 提供了和 `threading` 几乎对应的异步版：

### 5.1 `asyncio.Lock`：互斥

```python
lock = asyncio.Lock()
async with lock:          # 同一时刻只有一个协程能进入
    shared_state += 1
```

保护共享资源。**注意**：因为 asyncio 是单线程协作式调度，很多情况下其实不需要锁（只要临界区里没有 `await`，就不会被打断）。只有临界区**含 `await`**（可能中途被切走）时才真正需要锁。这是和 threading 的重要区别——threading 是抢占式，任何时刻都可能被打断。

### 5.2 `asyncio.Semaphore`：限流（claw0 用的就是这个）

```python
sem = asyncio.Semaphore(4)         # 同时最多 4 个进入
async with sem:
    await api_call()
```

限制并发数。s05 用 `Semaphore(4)` 限制最多 4 个 agent 回合同时调 LLM，避免打爆 API 限流。这是网关类程序最常见的用法——上游有并发上限，就用信号量在本地排成队。

### 5.3 `asyncio.Event`：通知

```python
event = asyncio.Event()
# 协程A: await event.wait()      # 阻塞等，直到别人 set()
# 协程B: event.set()              # 唤醒所有 wait 的
```

「等一个信号」。s04 里用 `threading.Event` 通知轮询线程退出，asyncio 版语义一样，只是异步可 `await`。

### 5.4 `asyncio.Queue`：生产者-消费者传数据

```python
q = asyncio.Queue()
await q.put(item)       # 生产
item = await q.get()    # 消费
```

协程间安全传数据。比用 `list` + `Lock` 手搓更省心。s04 用 `list + threading.Lock` 手搓了队列，换成 asyncio 版就是 `asyncio.Queue`。

> **重要坑**：这些异步原语**创建时会绑定「当前事件循环」**。所以不能在模块顶层（导入时还没循环）创建，要**延后到协程里或循环跑起来后**创建。s05 把 `Semaphore` 写成 `if _agent_semaphore is None: ... ` 懒初始化，正是为了避开这个坑（第 10 节陷阱 3）。

---

## 6. 阻塞调用的祸患：`asyncio.to_thread` / `run_in_executor`

asyncio 最大的规矩：**协程里绝对不能直接调同步阻塞函数**，否则整个循环冻住。

### 6.1 问题演示

```python
async def bad():
    time.sleep(5)            # ❌ 同步阻塞！整个事件循环卡死 5 秒，别的协程全停
    # 哪怕用 asyncio.sleep 也救不了，因为 time.sleep 不让出控制权
```

`time.sleep` 是同步的，它**不会 `await`、不会让出**，直接霸占线程。这 5 秒里事件循环什么都干不了。同理，任何同步的 HTTP 库（`requests`、老版 SDK）、同步的文件读写、`input()` 都是地雷。

### 6.2 正确解法一：`asyncio.to_thread`（Python 3.9+，推荐）

```python
async def good():
    result = await asyncio.to_thread(blocking_function, arg1, arg2)
```

把同步函数丢到**默认线程池**里跑，立即返回一个可 `await` 的对象。`await` 时事件循环不被占用，可以跑别的协程；阻塞的调用在**工作线程**里执行，完成后结果送回协程。

s05 的 `_agent_loop` 就这么干——Anthropic SDK 的 `client.messages.create` 是同步的，用 `to_thread` 包住，才不会冻住事件循环、4 并发才名副其实：

```python
response = await asyncio.to_thread(
    client.messages.create, model=..., messages=...,
)
```

### 6.3 正确解法二：`run_in_executor`（更底层，可指定线程池）

```python
loop = asyncio.get_running_loop()
result = await loop.run_in_executor(None, blocking_function, arg)   # None=默认池
# 或自定义池:
# executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
# await loop.run_in_executor(executor, blocking_function, arg)
```

`to_thread` 其实就是 `run_in_executor(None, ...)` 的语法糖。需要**自定义线程池大小**（比如限制最多 10 个工作线程）时才用 `run_in_executor` + 显式 executor。

### 6.4 终极正确解法：用原生异步库

最佳实践是**用原生异步版本**：`httpx.AsyncClient` 替代 `requests`、`aiofiles` 替代 `open()`、`asyncio.sleep()` 替代 `time.sleep()`。这些库内部用 `await` 让出，不占线程，最高效。只是当某个库**只有同步版**（如 Anthropic 旧 SDK）时，才退而用 `to_thread` 兜底。

---

## 7. 跨世界桥接：同步 ↔ 异步

这是 s05 最精妙也最容易懵的部分——同步代码和异步代码怎么互相调用。

### 7.1 异步调同步：`to_thread`（上一节已讲）

协程里要调同步阻塞函数 → `await asyncio.to_thread(sync_fn, args)`。

### 7.2 同步调异步：`run_coroutine_threadsafe`（s05 的 `run_async`）

反过来，**同步代码**（比如主线程的 REPL）想跑一个协程，怎么办？不能 `await`（同步代码没 `await` 关键字），也不能 `asyncio.run`（会另起循环、和后台循环冲突）。答案是 **`run_coroutine_threadsafe`**：

```python
def run_async(coro):
    loop = get_event_loop()                                    # 后台线程的常驻循环
    return asyncio.run_coroutine_threadsafe(coro, loop).result()
```

`run_coroutine_threadsafe(coro, loop)` 把协程**提交到指定的事件循环**（必须在另一个线程跑着的循环），返回一个 `concurrent.futures.Future`（注意是 `concurrent.futures`，不是 asyncio 的）；`.result()` 在当前（同步）线程**阻塞等**这个 future 完成，拿到协程返回值。

效果：对同步调用者来说，`run_async(...)` 表现得像一个普通同步函数（输入→等→拿结果），但底下协程跑在后台异步循环里、能和其他协程共享循环并发。这就是 s05「同步 REPL + 异步内核」的全部魔法。

**千万别搞混**：
- `loop.create_task(coro)` —— 只能在**持有该循环的线程**里调（已经在 async 上下文里时）。
- `asyncio.run_coroutine_threadsafe(coro, loop)` —— **跨线程**提交，从别的线程往这个循环塞协程。s05 主线程→后台线程正是这个。

### 7.3 为什么要躲进后台线程

s05 不在主线程直接 `asyncio.run`，而起一个 daemon 线程跑 `run_forever`，是因为：
- 主线程要跑同步 REPL（`input` 阻塞），没法当 asyncio 循环的宿主（循环跑起来会霸占线程）；
- 又要并发跑 agent（信号量限流、多协程），必须有常驻循环；
- 折中：循环搬后台线程，主线程同步，桥接一下两全。

模式口诀：**「常驻循环放后台线程 + 主线程同步 + `run_coroutine_threadsafe` 桥」**，这是「想用 asyncio 但又不想把整个程序改异步」的万能套路。

---

## 8. 取消与超时：`Task.cancel` / `wait_for` / `CancelledError`

### 8.1 取消任务

```python
task = asyncio.create_task(long_running())
await asyncio.sleep(1)
task.cancel()                # 请求取消：在 task 的下一个 await 处抛 CancelledError
try:
    await task
except asyncio.CancelledError:
    print("被取消了")
```

`cancel()` 不是立刻杀——它在任务**下一次 `await` 时**抛 `CancelledError`，给协程机会清理（`finally` 块会跑）。协程可以捕获 `CancelledError` 做收尾，但通常不该吞掉它（除非有特殊理由），否则取消不生效。

### 8.2 超时

```python
try:
    result = await asyncio.wait_for(slow_call(), timeout=5.0)
except asyncio.TimeoutError:
    print("超时")
```

`wait_for` 给协程套个超时，到点自动取消并抛 `TimeoutError`。调外部 API 时几乎必加，避免永久挂起。s05 的 `_agent_loop` **没有**给 API 调用加超时——这是个留白（生产代码该加）。

### 8.3 整体超时：`asyncio.timeout`（3.11+）

```python
async with asyncio.timeout(5.0):
    await do_something()
```

更现代的上下文管理器超时，比 `wait_for` 灵活（可以包住任意代码块）。

---

## 9. 实战模式速查表（对照 claw0）

| 模式 | 怎么写 | claw0 里的体现 |
|------|--------|---------------|
| 顶层入口跑一次 | `asyncio.run(main())` | s05 没用（要常驻） |
| 常驻循环后台线程 | `new_event_loop` + `run_forever` + daemon 线程 | s05 `get_event_loop` |
| 同步调异步 | `run_coroutine_threadsafe(coro, loop).result()` | s05 `run_async` |
| 异步调同步(阻塞SDK) | `await asyncio.to_thread(sync_fn, args)` | s05 `_agent_loop` 调 LLM |
| 并发等一批 | `await asyncio.gather(*coros)` | （s05 单条处理，未用；多客户端由 websockets 框架并发） |
| 限流 | `async with Semaphore(N):` | s05 `Semaphore(4)` |
| 同步原语懒初始化 | `if x is None: x = Semaphore(N)` | s05 信号量懒建 |
| 失败状态复位 | `try: ... finally: cleanup()` | s05 `on_typing` 的 try/finally |
| 防死循环护栏 | `for _ in range(N):` | s05 `_agent_loop` 15 圈上限 |

---

## 10. 新手必踩的陷阱（逐条对照）

### 陷阱 1：协程没有 await / create_task，不会跑

```python
async def work(): await asyncio.sleep(1)
work()            # ❌ 只造了协程对象，没跑！会有 RuntimeWarning: coroutine was never awaited
# 正确:
await work()                          # 直接 await
asyncio.create_task(work())          # 或排进循环
```

`async def` 定义的函数，调用它只是「造一个协程对象」，必须被调度才会执行。

### 陷阱 2：协程里调了同步阻塞，冻住循环

```python
async def bad():
    time.sleep(5)          # ❌ 不让出，整个循环卡 5 秒
    requests.get(url)     # ❌ 同步 HTTP，同样卡死
# 正确:
await asyncio.sleep(5)
await asyncio.to_thread(requests.get, url)   # 或用 httpx.AsyncClient
```

**任何会真正阻塞线程的同步调用都是地雷**。记住：asyncio 的并发全靠「大家在 await 时让出」，有人不让出，别人就全停。

### 陷阱 3：同步原语在模块顶层创建

```python
# 模块顶层（❌ 此时还没事件循环）:
sem = asyncio.Semaphore(4)      # 绑到一个不存在的/临时的循环，后面用会报错
# 正确：延后到循环跑起来后:
sem = None
async def run():
    global sem
    if sem is None:
        sem = asyncio.Semaphore(4)
```

s05 正是用懒初始化避开这个坑。asyncio 的 Lock/Semaphore/Event/Queue 都有此问题。

### 陷阱 4：create_task 的返回值没存住

```python
async def parent():
    asyncio.create_task(work())   # ❌ 返回的 Task 没人引用，可能被 GC 掉、任务中途消失
# 正确: 存住引用
    task = asyncio.create_task(work())
    tasks.add(task)
    task.add_done_callback(tasks.discard)
    await task
```

没有外部引用的 Task，Python 可能回收它，导致任务莫名没跑完。官方文档明确警告过这点。

### 陷阱 5：混用 `threading` 和 `asyncio` 的同步原语

```python
async with threading.Lock():     # ❌ threading.Lock 不支持 async with
# 正确: 用 asyncio.Lock（在协程里）
async with asyncio.Lock():
```

`threading.Lock` 是同步的，不能 `async with`；`asyncio.Lock` 是异步的，不能跨线程用。**同步世界用 threading 原语，异步世界用 asyncio 原语，别串**。s04 用 `threading.Lock`/`threading.Event`（因为是多线程模型），s05 用 `asyncio.Semaphore`（因为是 asyncio 模型）——各用各的。

### 陷阱 6：以为多线程能加速 CPU 任务

```python
# 想用多线程算数学加速 ❌——GIL 让它还是单核轮流
# CPU 密集要并行，用多进程:
from multiprocessing import Pool
with Pool(4) as p:
    results = p.map(heavy_compute, data)
```

### 陷阱 7：在异步函数里 `return` 的协程没被 await

```python
async def outer():
    return inner_async()        # ❌ 返回了协程对象但没 await，没跑
# 正确:
    return await inner_async()  # 或 return inner_async() 然后由调用方 await
```

`return coro`（不 await）在某些情况下是有意的（让调用方接管），但 9 成情况是漏了 `await`。

---

## 11. 选型决策树

```
你要处理的是什么？
│
├─ CPU 密集（大量计算）
│    └─► multiprocessing（多进程真并行）
│
├─ IO 密集（网络/磁盘/等用户）
│   │
│   ├─ 并发量小（< 几十），或依赖大量同步库不想改
│   │    └─► threading（多线程，简单）
│   │
│   └─ 高并发（上百上千连接），或愿意用 async 库
│        └─► asyncio
│           │
│           ├─ 顶层入口就一个 main → asyncio.run(main())
│           │
│           ├─ 要常驻服务/混同步代码 → 后台线程跑 run_forever + run_coroutine_threadsafe 桥
│           │
│           └─ 用的库只有同步版 → asyncio.to_thread 包住
```

对照 claw0：IO 密集（全是网络）+ 高并发（网关多客户端）+ 同步 SDK（Anthropic SDK）+ 同步 REPL 入口 → 完美命中「asyncio + 后台线程桥 + to_thread 兜底同步 SDK」这条路。

---

## 12. 一句话总结

Python 异步并发的本质是：**用 `async def` 定义能 `await` 暂停的协程，用事件循环在协程暂停时去跑别的、在就绪时恢复它，从而在单线程里并发处理大量 IO 等待**。核心规则只有三条：①协程必须被调度才跑（`await`/`create_task`/`asyncio.run`）；②协程里不能调同步阻塞（用 `to_thread` 或原生异步库）；③同步原语要绑循环所以得懒初始化、且别和 threading 原语混用。s05 的「后台常驻循环 + `run_coroutine_threadsafe` 桥接同步 REPL + `to_thread` 包同步 SDK + `Semaphore` 限流」就是这套规则在真实网关里的标准落地姿势。
