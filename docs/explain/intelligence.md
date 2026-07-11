# 第 06 节详解：智能层（Intelligence）

> 赋予灵魂，教会记忆——每轮对话前，agent 的「大脑」是如何组装的。

本文是对 `sessions/zh/s06_intelligence.py`（950 行）和 `s06_intelligence.md` 的逐层深读。s01-s02 里系统提示词是硬编码字符串；**s06 把它变成「由多个层级动态组装」的过程**——从磁盘文件加载身份/灵魂、发现技能、搜索记忆，每轮重新拼出一个完整的系统提示词。这是整个教学项目的**核心集成点**：前面所有零件（agent 循环、工具、会话）都还只是「壳」，到这一节 agent 才真正有了「人格」和「记忆」。

---

## 0. 一句话定位

s06 = **智能层**。它回答一个问题：**「每次调 LLM 之前，那个 `system` 提示词到底是怎么来的？」**

答案是一个**分层组装**过程：

```
磁盘文件 (SOUL.md, IDENTITY.md, MEMORY.md, ...)
        │  BootstrapLoader 加载
        ▼
   build_system_prompt()  ──►  8 层拼成一个字符串
        ▲                         (身份→灵魂→工具→技能→记忆→引导→运行时→渠道)
        │  每轮注入
   _auto_recall() 用用户消息搜记忆，把结果塞进第 5 层
        │
        ▼
   client.messages.create(system=这个拼好的字符串, ...)
```

核心思想：**系统提示词不是写死的，而是「磁盘文件 + 运行时记忆」每轮动态组装的**。改 `SOUL.md` 换人格、写 `MEMORY.md` 教记忆——不改代码。这是「提示词即数据」的工程化。

---

## 1. 架构总览

```
[SOUL.md] [IDENTITY.md] [TOOLS.md] [MEMORY.md] [HEARTBEAT.md] ...
     \        |           |          |           /
      v       v           v          v          v
    +-------------------------------+
    |     BootstrapLoader           |   启动阶段: 加载 8 个文件, 截断+总量上限
    |  (load, truncate, cap)        |
    +-------------------------------+
                |
                v
    +-------------------------------+        +-------------------+
    |   build_system_prompt()       | <----> | SkillsManager     |
    |   (8 层组装, 每轮重建)         |        | (discover, parse) |
    +-------------------------------+        +-------------------+
                |                                     ^
                v                                     |
    +-------------------------------+        +-------------------+
    |   Agent Loop (每轮)           | <----> | MemoryStore       |
    |   search -> build -> call LLM|        | (write, search)   |
    +-------------------------------+        +-------------------+
```

五个核心组件：

| 组件 | 代码位置 | 职责 |
|------|---------|------|
| `BootstrapLoader` | `:119-161` | 启动时从工作区加载最多 8 个 markdown 文件，截断+总量上限 |
| `SkillsManager` | `:188-276` | 扫描多目录发现带 frontmatter 的 `SKILL.md`，格式化成提示词块 |
| `MemoryStore` | `:288-583` | 双层记忆存储 + 混合搜索（TF-IDF + 哈希向量 + 时间衰减 + MMR） |
| `build_system_prompt` | `:674-746` | 8 层组装核心函数，每轮重建 |
| `_auto_recall` | `:831-836` | 用用户消息自动搜记忆，注入提示词第 5 层 |

外加记忆工具 `memory_write`/`memory_search`（`:593-603`）让 agent 自己读写记忆。下面逐个拆。

---

## 2. BootstrapLoader：从磁盘加载「出厂设定」

agent 启动时要从工作区加载一组「出厂设定文件」——这些文件定义了它的身份、人格、工具指南等。

### 2.1 八个 Bootstrap 文件

```python
BOOTSTRAP_FILES = [
    "SOUL.md", "IDENTITY.md", "TOOLS.md", "USER.md",
    "HEARTBEAT.md", "BOOTSTRAP.md", "AGENTS.md", "MEMORY.md",
]
```

每个文件管一方面：

| 文件 | 作用 | 提示词第几层 |
|------|------|------------|
| `IDENTITY.md` | 身份（你是谁） | 第 1 层 |
| `SOUL.md` | 灵魂/人格（你怎么说话） | 第 2 层 |
| `TOOLS.md` | 工具使用指南 | 第 3 层 |
| `MEMORY.md` | 长期记忆（常驻事实） | 第 5 层 |
| `HEARTBEAT.md`/`BOOTSTRAP.md`/`AGENTS.md`/`USER.md` | 引导上下文 | 第 6 层 |

这些文件**在 agent 启动时一次性加载**，之后缓存——除非重启，否则中途改文件不会重载（这是「启动态 vs 每轮态」的区分，见第 6 节）。

### 2.2 三种加载模式

```python
def load_all(self, mode: str = "full") -> dict[str, str]:
    if mode == "none":
        return {}
    names = ["AGENTS.md", "TOOLS.md"] if mode == "minimal" else list(BOOTSTRAP_FILES)
    ...
```

- **`full`**：主 agent，加载全部 8 个文件——完整人格+记忆。
- **`minimal`**：子 agent / cron 任务，只加载 `AGENTS.md`+`TOOLS.md`——省 token，够干活即可。
- **`none`**：最简，啥都不加。

这个区分很实际：主 agent 要有人格有记忆（full），但 s07 的定时心跳任务只是「干个活」（minimal），不需要人格，省下 token 给真正的内容。**模式 = 按 agent 角色裁剪提示词体积**。

### 2.3 双重上限保护

```python
MAX_FILE_CHARS = 20000      # 单文件上限
MAX_TOTAL_CHARS = 150000    # 总量上限

def truncate_file(self, content, max_chars=MAX_FILE_CHARS):
    if len(content) <= max_chars:
        return content
    cut = content.rfind("\n", 0, max_chars)   # 在行边界处截断, 不劈断一行
    if cut <= 0:
        cut = max_chars
    return content[:cut] + f"\n\n[... truncated ({len(content)} chars total, showing first {cut}) ...]"
```

两道闸：
- **单文件 20k 字符**：防某个文件（比如有人写了 50k 的 SOUL.md）独占提示词。截断时在**行边界**切（`rfind("\n")`），不劈断一行，并附说明「截断了，原始多大」——对 LLM 透明。
- **总量 150k 字符**：防所有文件加起来撑爆上下文窗口。`load_all` 里累加 `total`，超了就停（`break`）或按剩余额度再截。

这两道闸体现「**提示词是有限资源**」的认知：系统提示词占的是上下文窗口的额度，不能让配置文件无节制膨胀。生产代码会更精细（按 token 而非字符算），但 s06 用字符数教学，够清楚。

---

## 3. 灵魂系统：SOUL.md

```python
def load_soul(workspace_dir: Path) -> str:
    path = workspace_dir / "SOUL.md"
    if not path.is_file():
        return ""
    try:
        return path.read_text(encoding="utf-8").strip()
    except Exception:
        return ""
```

`SOUL.md` 单独有个 `load_soul`（虽然 BootstrapLoader 也加载它）。它定义 agent 的**人格**——「你温暖、好奇、爱追问」。注入到提示词**靠前位置**（第 2 层），因为**越靠前影响力越强**（LLM 对靠前的指令更敏感）。

这是「**人格即文件**」的体现：换 `SOUL.md` 内容就换人格，不改代码、不重启框架。对比 s01-s05 硬编码的 `SYSTEM_PROMPT`，s06 把人格外置成数据——这是生产 agent 框架的标准做法。

---

## 4. SkillsManager：技能发现与注入

技能（Skill）= 一个目录里带 `SKILL.md`（含 YAML frontmatter）的可调用能力。

### 4.1 frontmatter 解析（不依赖 pyyaml）

```python
def _parse_frontmatter(self, text: str) -> dict[str, str]:
    meta = {}
    if not text.startswith("---"):
        return meta
    parts = text.split("---", 2)
    if len(parts) < 3:
        return meta
    for line in parts[1].strip().splitlines():
        if ":" not in line:
            continue
        key, _, value = line.strip().partition(":")
        meta[key.strip()] = value.strip()
    return meta
```

`SKILL.md` 格式：

```markdown
---
name: weather
description: 查天气
invocation: /weather
---
这里是技能的详细说明（body），告诉 LLM 怎么用。
```

`---` 之间是 frontmatter（元数据：name/description/invocation），之后是 body（详细说明）。s06 自己写了个极简 YAML 解析（只认 `key: value` 行），**不引入 pyyaml 依赖**——教学版取舍，够用就好。body 在 `parts[2]`（split("---", 2) 的第三段）。

### 4.2 多目录扫描 + 优先级覆盖

```python
def discover(self, extra_dirs=None):
    scan_order = []
    if extra_dirs: scan_order.extend(extra_dirs)
    scan_order.append(self.workspace_dir / "skills")          # 内置
    scan_order.append(self.workspace_dir / ".skills")         # 托管
    scan_order.append(self.workspace_dir / ".agents" / "skills")  # 个人
    scan_order.append(Path.cwd() / ".agents" / "skills")      # 项目
    scan_order.append(Path.cwd() / "skills")                  # 工作区

    seen: dict[str, dict[str, str]] = {}
    for d in scan_order:
        for skill in self._scan_dir(d):
            seen[skill["name"]] = skill    # 同名后者覆盖前者
    self.skills = list(seen.values())[:MAX_SKILLS]
```

按**优先级顺序**扫描多个目录（内置→托管→个人→项目→工作区）。关键：用 `seen[skill["name"]] = skill`——**同名技能，后扫描的覆盖先扫描的**。所以工作区（最后扫）的技能优先级最高，能覆盖内置的——让用户能「定制/覆盖」内置技能。这是「**分层配置、后者覆盖**」的设计，和路由绑定表的「最具体优先」精神一致。

`[:MAX_SKILLS]`（150）是数量上限——防技能太多撑爆提示词。

### 4.3 格式化成提示词块

```python
def format_prompt_block(self):
    lines = ["## Available Skills", ""]
    total = 0
    for skill in self.skills:
        block = f"### Skill: {skill['name']}\nDescription: ...\nInvocation: ...\n"
        if skill.get("body"): block += f"\n{skill['body']}\n"
        block += "\n"
        if total + len(block) > MAX_SKILLS_PROMPT:    # 30k 上限
            lines.append(f"(... more skills truncated)")
            break
        lines.append(block)
        total += len(block)
    return "\n".join(lines)
```

把所有技能拼成一个 `## Available Skills` 块，塞进提示词第 4 层。同样有 `MAX_SKILLS_PROMPT`（30k）上限保护。技能块**在启动时格式化一次、缓存**——`skills_block` 在 agent_loop 里只算一次，每轮复用（技能不像记忆那样每轮变）。

---

## 5. MemoryStore：双层记忆 + 混合搜索

这是 s06 最重的部分。记忆系统让 agent 能「记住」跨会话的事实。

### 5.1 双层存储

```python
# 层 1: 长期记忆 (常驻, 手动维护)
workspace/MEMORY.md        # 比如 "用户偏好 Python"

# 层 2: 每日日志 (agent 通过 memory_write 工具自动写)
workspace/memory/daily/2026-07-11.jsonl   # 每天一个文件, 每条一行 JSON
```

两层各有分工：
- **`MEMORY.md`（evergreen 常驻）**：长期事实，手动或 agent 主动维护的高价值信息。整段加载、按段落拆分参与搜索。
- **`daily/{date}.jsonl`（每日）**：agent 通过 `memory_write` 工具写的日志，每条带 `ts`/`category`/`content`。按条参与搜索。

```python
def write_memory(self, content, category="general"):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = self.memory_dir / f"{today}.jsonl"
    entry = {"ts": ..., "category": category, "content": content}
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")   # 追加写
```

`write_memory` 按**当天日期**命名文件、**追加**写——和 s03 的 JSONL 会话持久化同构（追加写、按行重放）。`category` 给记忆分类（preference/fact/context），方便后续筛选。

### 5.2 加载 + 分块

```python
def _load_all_chunks(self):
    chunks = []
    evergreen = self.load_evergreen()
    if evergreen:
        for para in evergreen.split("\n\n"):     # 长期记忆按段落拆
            if para.strip():
                chunks.append({"path": "MEMORY.md", "text": para})
    if self.memory_dir.is_dir():
        for jf in sorted(self.memory_dir.glob("*.jsonl")):
            for line in jf.read_text().splitlines():
                entry = json.loads(line)
                chunks.append({"path": f"{jf.name} [{cat}]", "text": entry["content"]})
    return chunks
```

搜索的粒度是「**chunk（块）**」：`MEMORY.md` 按段落（`\n\n`）拆成多块，每日 JSONL 每条算一块。每块带 `path`（来源）和 `text`——这样搜索结果能告诉用户「这条记忆来自哪」。

### 5.3 基础搜索：TF-IDF + 余弦相似度

`search_memory`（`:353`）是纯 Python 的 TF-IDF 搜索。三步：

**① 分词**：
```python
@staticmethod
def _tokenize(text):
    tokens = re.findall(r"[a-z0-9一-鿿]+", text.lower())
    return [t for t in tokens if len(t) > 1 or "一" <= t <= "鿿"]
```
小写英文 + 单个 CJK 字符（中文按字分），过滤单字符英文（太短没区分度）。中文按字、英文按词——双语兼容的朴素分词。

**② TF-IDF 向量化**：
```python
df: dict[str, int] = {}     # 文档频率: 每个 token 在多少块出现
for tokens in chunk_tokens:
    for t in set(tokens):
        df[t] = df.get(t, 0) + 1
n = len(chunks)

def tfidf(tokens):
    tf = {}                 # 词频
    for t in tokens: tf[t] = tf.get(t, 0) + 1
    return {t: c * (math.log((n + 1) / (df.get(t, 0) + 1)) + 1) for t, c in tf.items()}
```
TF-IDF = 词频 × 逆文档频率。核心思想：**在所有块里都出现的词（如「的」「the」）区分度低、权重小；只在这块出现的词区分度高、权重大**。`log((n+1)/(df+1))` 是 IDF 公式，`+1` 平滑防除零。

**③ 余弦相似度**：
```python
def cosine(a, b):
    common = set(a) & set(b)
    if not common: return 0.0
    dot = sum(a[k] * b[k] for k in common)
    na = math.sqrt(sum(v*v for v in a.values()))
    nb = math.sqrt(sum(v*v for v in b.values()))
    return dot / (na * nb) if na and nb else 0.0
```
把每个块和查询都表示成 TF-IDF 向量（稀疏 dict），算它们夹角的余弦——越接近 1 越相似。只对**共有的 token**算点积（稀疏向量优化，省得遍历全词典）。

最后按分数降序取 `top_k`。**整个过程不依赖外部向量数据库**——纯 Python 几十行实现「够用」的语义搜索，这是教学版的取舍（生产用 embedding API）。

### 5.4 混合搜索管道（hybrid_search）

`hybrid_search`（`:556`）是增强版，串五阶段：

```python
def hybrid_search(self, query, top_k=5):
    chunks = self._load_all_chunks()
    keyword_results = self._keyword_search(query, chunks, top_k=10)   # ① 关键词(TF-IDF)
    vector_results = self._vector_search(query, chunks, top_k=10)      # ② 向量(哈希投影)
    merged = self._merge_hybrid_results(vector_results, keyword_results)  # ③ 加权合并
    decayed = self._temporal_decay(merged)                              # ④ 时间衰减
    reranked = self._mmr_rerank(decayed)                                # ⑤ MMR 多样性
    return reranked[:top_k]
```

**① 关键词搜索**：就是上面的 TF-IDF，按字面匹配。擅长精确词命中。

**② 向量搜索（哈希投影模拟 embedding）**：
```python
@staticmethod
def _hash_vector(text, dim=64):
    tokens = MemoryStore._tokenize(text)
    vec = [0.0] * dim
    for token in tokens:
        h = hash(token)
        for i in range(dim):
            bit = (h >> (i % 62)) & 1
            vec[i] += 1.0 if bit else -1.0
    norm = math.sqrt(sum(v*v for v in vec)) or 1.0
    return [v / norm for v in vec]
```
用 `hash(token)` 的各个 bit 往 64 维向量里 +1/-1，归一化后当「假 embedding」。**这不是真语义嵌入**——它演示的是「**第二搜索通道的模式**」，让你理解「向量搜索」长什么样。生产里这步会换成真 embedding API（OpenAI/本地模型）。注释明说了：`teaches the PATTERN of a second search channel`。

**③ 加权合并**：
```python
@staticmethod
def _merge_hybrid_results(vector_results, keyword_results,
                          vector_weight=0.7, text_weight=0.3):
    merged = {}
    for r in vector_results:
        key = r["chunk"]["text"][:100]    # 用文本前 100 字符当去重键
        merged[key] = {"chunk": ..., "score": r["score"] * 0.7}
    for r in keyword_results:
        if key in merged: merged[key]["score"] += r["score"] * 0.3
        else: merged[key] = {...}
```
两路结果按文本前缀去重合并，向量权重 0.7、关键词 0.3——**偏向量、关键词补强**。双通道的意义：向量擅长语义近似（「颜色」匹配「蓝色」），关键词擅长精确命中（专有名词、代码），互补。

**④ 时间衰减**：
```python
@staticmethod
def _temporal_decay(results, decay_rate=0.01):
    for r in results:
        age_days = ...   # 从 path 里的日期解析
        r["score"] *= math.exp(-decay_rate * age_days)
```
`exp(-0.01 × 天数)`——越近的记忆得分越高，老的衰减。让 agent 偏向「最近的事」。日期从文件名（`2026-07-11.jsonl`）解析。

**⑤ MMR 重排序（Maximal Marginal Relevance）**：
```python
@staticmethod
def _mmr_rerank(results, lambda_param=0.7):
    # MMR = lambda * 相关性 - (1-lambda) * 与已选最大相似度
    while remaining:
        for idx in remaining:
            relevance = results[idx]["score"]
            max_sim = max(jaccard(tokenized[idx], tokenized[sel]) for sel in selected)
            mmr = 0.7 * relevance - 0.3 * max_sim
        pick max mmr ...
```
贪心选：每轮挑「既相关、又和已选结果不重复」的。用 Jaccard 相似度（token 集合交并比）衡量重复度。**目的：避免返回 5 条几乎一样的记忆**——多样性。这是搜索系统的高级技巧，s06 用最简形式演示。

五阶段管道是「**召回 → 排序 → 多样性**」的完整检索范式微缩版。教学价值在于展示**模式**，每一步生产里都可换更强的实现。

---

## 6. 记忆工具：memory_write / memory_search

```python
def tool_memory_write(content, category="general"):
    return memory_store.write_memory(content, category)

def tool_memory_search(query, top_k=5):
    results = memory_store.hybrid_search(query, top_k)
    return "\n".join(f"[{r['path']}] (score: {r['score']}) {r['snippet']}" for r in results)

TOOLS = [{"name": "memory_write", ...}, {"name": "memory_search", ...}]
TOOL_HANDLERS = {"memory_write": tool_memory_write, "memory_search": tool_memory_search}
```

agent 能**主动**调这两个工具：学到重要事实时 `memory_write` 存下来；需要回忆时 `memory_search` 搜。这是「**agent 自我记忆**」——不是被动等用户喂，而是自己判断「这值得记」就存。和 s02 的 `bash`/`read_file` 工具同构（schema + handler dispatch），只是这两个专门管记忆。

注意：s06 的工具是 s02 工具的**补充非替代**（代码注释明说）。完整 agent 会把 `bash`/`read_file`/`memory_*` 合并成一个工具列表。s06 为教学清晰只列记忆工具。

---

## 7. build_system_prompt：8 层组装（核心）

这是 s06 的灵魂函数。每轮调 LLM 前都调它，把上面所有零件拼成一个系统提示词。

### 7.1 八层结构

```python
def build_system_prompt(mode="full", bootstrap=None, skills_block="",
                        memory_context="", agent_id="main", channel="terminal"):
    sections = []
    # 第1层 身份
    # 第2层 灵魂
    # 第3层 工具指南
    # 第4层 技能
    # 第5层 记忆
    # 第6层 引导上下文
    # 第7层 运行时
    # 第8层 渠道
    return "\n\n".join(sections)
```

逐层：

**第 1 层 身份**（`IDENTITY.md`）：
```python
identity = bootstrap.get("IDENTITY.md", "").strip()
sections.append(identity if identity else "You are a helpful personal AI assistant.")
```
最基本：「你是谁」。有文件用文件，没有用默认值兜底。**永远在最前**——身份是一切的基础。

**第 2 层 灵魂**（`SOUL.md`，仅 full 模式）：
```python
if mode == "full":
    soul = bootstrap.get("SOUL.md", "").strip()
    if soul:
        sections.append(f"## Personality\n\n{soul}")
```
人格。**为什么放第 2 层**：越靠前影响力越强，身份（你是谁）之后立刻是人格（你怎么表现），这俩是 agent 行为最强的塑造者。

**第 3 层 工具指南**（`TOOLS.md`）：
```python
tools_md = bootstrap.get("TOOLS.md", "").strip()
if tools_md:
    sections.append(f"## Tool Usage Guidelines\n\n{tools_md}")
```
教 agent 怎么用工具（什么时候该调、怎么调）。

**第 4 层 技能**（`skills_block`，仅 full）：
```python
if mode == "full" and skills_block:
    sections.append(skills_block)
```
SkillsManager 格式化好的技能清单。agent 据此知道「有哪些技能可调」。

**第 5 层 记忆**（`MEMORY.md` + 召回的，仅 full）：
```python
if mode == "full":
    mem_md = bootstrap.get("MEMORY.md", "").strip()
    parts = []
    if mem_md: parts.append(f"### Evergreen Memory\n\n{mem_md}")
    if memory_context: parts.append(f"### Recalled Memories (auto-searched)\n\n{memory_context}")
    if parts: sections.append("## Memory\n\n" + "\n\n".join(parts))
    sections.append("## Memory Instructions\n\n- Use memory_write ...")
```
两层记忆：**常驻**（`MEMORY.md`，每轮都在）+ **召回**（`memory_context`，本轮按用户消息搜出来的）。再加一段「记忆使用指南」教 agent 怎么用记忆工具。**这层每轮都可能变**——因为召回结果取决于用户这轮说了什么。

**第 6 层 引导上下文**（剩余文件，full+minimal）：
```python
if mode in ("full", "minimal"):
    for name in ["HEARTBEAT.md", "BOOTSTRAP.md", "AGENTS.md", "USER.md"]:
        content = bootstrap.get(name, "").strip()
        if content:
            sections.append(f"## {name.replace('.md', '')}\n\n{content}")
```
其他配置文件（心跳说明、引导、agent 列表、用户信息）。辅助上下文。

**第 7 层 运行时上下文**：
```python
now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
sections.append(f"## Runtime Context\n\n- Agent ID: {agent_id}\n- Model: {MODEL_ID}\n"
                f"- Channel: {channel}\n- Current time: {now}\n- Prompt mode: {mode}")
```
**每轮新生成**的运行时信息：当前时间、agent id、模型、渠道、模式。让 agent 知道「现在几点、自己在哪个通道」。时间尤其重要——agent 回答「今天」相关问题时靠这层。

**第 8 层 渠道提示**：
```python
hints = {
    "terminal": "You are responding via a terminal REPL. Markdown is supported.",
    "telegram": "You are responding via Telegram. Keep messages concise.",
    "discord": "You are responding via Discord. Keep messages under 2000 characters.",
    "slack": "You are responding via Slack. Use Slack mrkdwn formatting.",
}
sections.append(f"## Channel\n\n{hints.get(channel, f'You are responding via {channel}.')}")
```
按渠道给格式建议——Telegram 要简短、Discord 要 <2000 字、Slack 用 mrkdwn。**让同一个 agent 在不同平台表现得体**。这层和 s04 的通道概念呼应：agent 内核平台无关，但出站格式要适配平台。

最后 `"\n\n".join(sections)`——空行分隔各层，拼成一个字符串。

### 7.2 设计要点

**① 越靠前影响力越强**：身份→灵魂在最前，因为它们最该塑造行为；渠道提示在最后，是「微调」性质的格式建议。这是利用 LLM 对靠前内容更敏感的特性，把最重要的放前面。

**② 每轮重建**：`build_system_prompt` 在 agent_loop 里**每轮都调**（`:887`），不缓存。因为第 5 层（召回记忆）和第 7 层（时间）每轮都可能变。对比：bootstrap 文件和 skills_block 是**启动时算一次缓存**的（不变的不重算）。这是「**静态部分缓存、动态部分每轮重建**」的取舍。

**③ mode 裁剪**：`full` 全 8 层，`minimal` 只留身份+工具+引导+运行时+渠道（去灵魂/技能/记忆），`none` 更少。按 agent 角色给「精简版」提示词。

---

## 8. _auto_recall：自动记忆注入

```python
def _auto_recall(user_message: str) -> str:
    results = memory_store.hybrid_search(user_message, top_k=3)
    if not results:
        return ""
    return "\n".join(f"- [{r['path']}] {r['snippet']}" for r in results)
```

**每轮**用用户消息当查询，搜记忆 top 3，格式化成 `- [来源] 摘要` 列表，塞进提示词第 5 层的「Recalled Memories」。

关键：**用户不需要显式说「查记忆」**——agent 自动搜、自动注入。用户说「你还记得我喜欢的颜色吗」，`_auto_recall` 用这句话搜出「用户喜欢蓝色」的记忆，塞进提示词，LLM 看到就能回答。这是「**记忆的透明召回**」——对用户无感，agent 像真的「想起来」一样。

在 agent_loop 里：
```python
memory_context = _auto_recall(user_input)          # 搜
system_prompt = build_system_prompt(               # 拼
    mode="full", bootstrap=bootstrap_data,
    skills_block=skills_block, memory_context=memory_context)
```

---

## 9. agent_loop：启动态 + 每轮态

```python
def agent_loop():
    # === 启动态: 算一次, 缓存 ===
    loader = BootstrapLoader(WORKSPACE_DIR)
    bootstrap_data = loader.load_all(mode="full")      # 加载 8 文件
    skills_mgr = SkillsManager(WORKSPACE_DIR)
    skills_mgr.discover()                              # 发现技能
    skills_block = skills_mgr.format_prompt_block()    # 格式化技能块

    messages = []
    while True:
        user_input = input(...)
        ...
        # === 每轮态: 重新算 ===
        memory_context = _auto_recall(user_input)      # 每轮搜记忆
        system_prompt = build_system_prompt(...)        # 每轮重建提示词
        messages.append({"role": "user", "content": user_input})

        # 工具循环(和 s04/s05 同构)
        while True:
            response = client.messages.create(system=system_prompt, tools=TOOLS, messages=messages)
            ...
            if end_turn: print回复; break
            elif tool_use: 调 process_tool_call, 塞结果, continue
```

清晰区分两个阶段：

| | 启动态 | 每轮态 |
|---|---|---|
| 何时跑 | 程序启动一次 | 每条用户消息 |
| 做什么 | 加载 bootstrap、发现技能 | 搜记忆、重建提示词 |
| 缓存 | 是（bootstrap_data、skills_block） | 否（每轮重算） |
| 为什么 | 这些不变，省算 | 记忆/时间会变 |

工具循环部分和 s04/s05 **同构**：`end_turn` 收尾、`tool_use` 调 `process_tool_call` 塞结果继续、失败回滚 messages。区别只在 `system=system_prompt` 用的是**每轮重建的动态提示词**，而非硬编码。

---

## 10. REPL 命令

```python
/soul       # 看 SOUL.md 内容
/skills     # 列已发现技能
/memory     # 记忆统计(长期字符数、每日文件数、条目数)
/search <q> # 手动搜记忆(看混合搜索结果+分数)
/prompt     # 打印完整组装好的系统提示词(调试用, 看 8 层拼出来长啥样)
/bootstrap  # 看加载了哪些文件+各字符数+总量
```

`/prompt` 特别有用——让你**看到**每轮拼出来的完整提示词，直观理解「8 层组装」到底是什么效果。`/search` 让你手动触发搜索看分数，理解混合搜索的召回质量。这些命令把内部机制**可视化**，是教学设计。

---

## 11. 和 s05 的对照：智能层带来了什么

| 维度 | s05 Gateway | s06 Intelligence |
|------|------------|-------------------|
| 系统提示词 | `agent.system_prompt()` 硬拼三行 | 8 层动态组装 |
| 人格来源 | `AgentConfig.personality` 字段 | `SOUL.md` 文件 |
| 记忆 | 无 | 双层存储 + 混合搜索 + 自动召回 |
| 技能 | 无 | 多目录发现 + frontmatter |
| 提示词体积控制 | 无 | 单文件 20k + 总量 150k + 技能 30k |
| 每轮重建 | 否（配置不变） | 是（记忆/时间变） |

s06 的跃迁：从「**配置驱动的简单人格**」到「**文件驱动的完整智能体**」。agent 不再只是「有个 personality 字符串」，而是有身份、灵魂、技能、记忆、运行时感知、渠道适配的完整「大脑」。这是从「路由器」到「智能体」的质变。

---

## 12. 留白与后续

| 留白 | 现状 | 谁来补 |
|------|------|--------|
| 记忆持久化跨重启 | daily JSONL 已持久，但搜索是全量加载 | 大规模要索引/分页 |
| 真 embedding | 哈希投影模拟 | 生产换 embedding API |
| 提示词压缩 | 字符截断 | s09 的溢出压缩 |
| 主动行为 | 只被动响应 | s07 心跳/cron 主动跑 |
| 多 agent 共享记忆 | 单 agent | s05 网关多 agent 各自记忆 |
| 技能执行 | 只描述，不自动调 | 生产加技能调用框架 |

智能层本身是「**组装**」层——它不发明新能力（记忆搜索、工具循环都是现成的），而是把所有能力**按层拼成一个提示词**。它的稳定性来自「分层 + 越前越强 + 静态缓存动态重建」这几个原则，这些原则从教学到生产基本不变。

---

## 13. 运行方法

```sh
cd claw0
python sessions/zh/s06_intelligence.py
```

需要 `.env` 配 `ANTHROPIC_API_KEY` 和 `MODEL_ID`，且从项目根目录跑（要找到 `workspace/`）。

试玩路径：

```sh
# 先看默认状态
/bootstrap        # 加载了哪些文件
/skills           # 发现的技能
/memory           # 记忆统计
/prompt           # 完整提示词长啥样

# 教它记点东西
You > 我最喜欢蓝色, 用 Python 编程
# (agent 可能调 memory_write 存下)

# 过几轮问它, 看自动召回
You > 你知道我什么偏好?
# (_auto_recall 搜到记忆, 注入提示词, agent 回答)

# 手动搜记忆
/search python
/search 颜色

# 改 SOUL.md 换人格(编辑 workspace/SOUL.md), 重启
# (agent 人格变了, 代码没动)
```

---

## 14. 一句话总结

s06 = **智能层，把「系统提示词」从硬编码字符串升级成「磁盘文件（身份/灵魂/工具/记忆/引导）+ 运行时记忆」每轮动态分层组装的过程**：启动时 `BootstrapLoader` 加载 8 个文件（截断+总量上限保护）、`SkillsManager` 扫多目录发现技能（同名后者覆盖）、`MemoryStore` 双层存储（常驻 `MEMORY.md` + 每日 JSONL）+ 五阶段混合搜索（TF-IDF 关键词 + 哈希向量 + 加权合并 + 时间衰减 + MMR 多样性）；每轮 `_auto_recall` 用用户消息自动搜记忆 top 3，`build_system_prompt` 把「身份→灵魂→工具→技能→记忆→引导→运行时→渠道」8 层按「越前越强」拼成一个字符串传给 LLM——静态部分（bootstrap/skills）启动缓存、动态部分（记忆/时间）每轮重建，`full/minimal/none` 三模式按 agent 角色裁剪体积。它让 agent 从「有 personality 字符串」变成「有身份/灵魂/技能/记忆/运行时感知/渠道适配的完整大脑」，是「提示词即数据、换文件换人格不改代码」的工程化，也是 s01-s05 所有零件的集成点；真 embedding、主动行为、提示词压缩等留给 s07-s09 补全。
