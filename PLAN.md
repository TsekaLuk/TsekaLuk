# Profile 优化方案

> 核心原则：**零信息删除，全量保留**。只做呈现层优化、增量功能和工程加固。

---

## Phase 1 — 美学呈现优化 (README.md)

### 1.1 品牌色统一

当前问题：Typing SVG 用 `#2E7EEE`，MBTI badge 用 `#4E8DF5`，footer 用 `#65A69A/#727A8C`，social badge 用 `#7C3AED`，整体色彩分裂。

改动：
- Typing SVG `color` 参数 → `65A69A`（品牌主色）
- MBTI badge → `color=65A69A`，Adaptive Mask badge → `color=727A8C`
- Website social badge → `color=65A69A`（替换 `7C3AED`）
- 保持第三方 logo badge 原色不动（OpenAI 紫、React 蓝等——它们的品牌色不应被覆盖）

涉及文件：`README.md` 第 17、23-24、30 行

### 1.2 Tech Stack 分组呼吸感

当前问题：4 个分类之间只有 `**粗体标题**` + `<br/>`，badge 密集连片，无法快速扫描分类边界。

改动：在每个分类标题前加一行空行 + HTML 注释分隔符，并在 badge 行之间加入概念类 badge（Skill/Theory/Product/Math/Web3）的独立分行，让"工具"和"概念"视觉上分层。

具体做法：
- 每个分类块用 `<br/><br/>` 增加 **组间间距**（当前只有一个 `<br/>`）
- Skill/Theory 类 badge 独立成行（已经是了，确认保留）
- 在 `**Full Stack Engineering**` 内部，把 Web3 行前加一个 `<br/>` 独立出来形成子分组

涉及文件：`README.md` 第 60-133 行

### 1.3 Dark Mode Logo 适配

当前问题：README 固定用 `logo-horizontal-zh.svg`，品牌仓库里已有 `logo-inverse.svg` 但未使用。深色模式下 logo 对比度可能不足。

改动：用 `<picture>` 标签做明暗适配：
```html
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="./brand-icons/logo-inverse.svg">
  <source media="(prefers-color-scheme: light)" srcset="./brand-icons/logo-horizontal-zh.svg">
  <img src="./brand-icons/logo-horizontal-zh.svg" width="60%" alt="Nebutra Intelligence — AI-Native One-Person Company"/>
</picture>
```

同时改善 `alt` 文本语义。

涉及文件：`README.md` 第 12 行

### 1.4 Footer 品牌签名

当前问题：底部只有波浪线 + view count，视觉上"虎头蛇尾"。

改动：在 footer stats 下方加一行 signature tagline（纯文本，不添加 badge）：
```html
<sub>Ship fast · Think deep · Build alone</sub>
```

涉及文件：`README.md` 第 221 行后

### 1.5 alt 文本语义化

当前问题：多个 `<img>` 的 alt 过于泛泛（`"Metrics"`, `"Snake Animation"`, `"Typing SVG"`），不利于 SEO 和无障碍。

改动：
- `alt="Typing SVG"` → `alt="Founder & CEO @ Nebutra Intelligence | AI-Native Innovator | Full Stack Engineer"`
- `alt="Metrics"` → `alt="TsekaLuk GitHub metrics — contributions, activity, community stats"`
- `alt="Snake Animation"` → `alt="GitHub contribution grid animated as a snake game"`

涉及文件：`README.md` 第 17、200、204 行

---

## Phase 2 — 黑客增长 (README + RSS 联动)

### 2.1 "What I'm Reading" 区块

当前问题：RSS 聚合了 80+ 博客，但产出只是藏在 `feeds/` 下的 XML。访客看不到阅读品味。

改动：
1. 在 `build_merged_rss.py` 中新增 `--readme-snippet` 参数，生成 markdown 片段（最近 5 篇文章，格式为 `- [Title](link) — *Source*`）写入指定文件
2. 在 README 的 `Recent Activity` 和 `Engineering Analytics` 之间新增 `📡 Blog Radar` 区块，用占位注释标记：
   ```
   <!--BLOG_RADAR:start-->
   ...自动生成的 5 条...
   <!--BLOG_RADAR:end-->
   ```
3. 在 `rss-feed.yml` workflow 中增加一步：运行脚本生成 snippet → 写入 README → 一起 auto-commit

涉及文件：
- `scripts/build_merged_rss.py` — 新增 `--readme-snippet` 逻辑
- `README.md` — 新增 Blog Radar 区块（占位标记）
- `.github/workflows/rss-feed.yml` — 新增 snippet 生成 + README commit 步骤

### 2.2 Activity 信息密度提升

当前问题：`max_lines: 6`，5 条都是 star，产出型事件被挤掉。

改动：
- `max_lines` 从 `6` → `10`，增加产出型事件露出概率
- **不 disable star**（尊重信息完整性）

涉及文件：`.github/recent-activity.config.yml` 第 4 行

---

## Phase 3 — 轮子优化 (工程加固)

### 3.1 RSS 脚本：可观测性

当前问题：`except Exception: return url, []` 静默吞错，80+ 源任何一个挂了都不知道。

改动：
1. 添加 `logging` 模块，失败时记录 URL + 异常类型
2. `main()` 末尾打印摘要行：`OK: 38/42 feeds | FAILED: 4 (antirez.com, ...)`
3. 新增 `--strict` 参数：失败比例 > 50% 时 exit code = 1，触发 Actions 告警

涉及文件：`scripts/build_merged_rss.py`

### 3.2 Workflow 版本钉死 + 连锁触发治理

当前问题：
- `lowlighter/metrics@latest` 有 supply chain 风险
- `actions/checkout` 在 snake.yml 用 v3，rss-feed.yml 用 v4
- RSS commit 触发 push → 连锁触发 metrics + snake workflow

改动：
1. `lowlighter/metrics@latest` → `lowlighter/metrics@v3.34`（钉到当前最新稳定版）
2. `snake.yml` 的 `actions/checkout@v3` → `actions/checkout@v4`
3. 给 `metrics.yml` 和 `snake.yml` 的 push trigger 加 `paths-ignore`：
   ```yaml
   push:
     branches: ["main"]
     paths-ignore:
       - "feeds/**"
       - "README.md"
   ```

涉及文件：
- `.github/workflows/metrics.yml`
- `.github/workflows/snake.yml`

### 3.3 新增 Dependabot 配置

当前问题：无自动依赖更新。

改动：新建 `.github/dependabot.yml`：
```yaml
version: 2
updates:
  - package-ecosystem: "github-actions"
    directory: "/"
    schedule:
      interval: "weekly"
  - package-ecosystem: "pip"
    directory: "/scripts"
    schedule:
      interval: "monthly"
```

涉及文件：新建 `.github/dependabot.yml`

---

## 变更清单

| # | 文件 | 操作 | Phase |
|---|------|------|-------|
| 1 | `README.md` | 编辑：品牌色、dark mode logo、alt 文本、分组间距、footer 签名、Blog Radar 占位 | 1 + 2 |
| 2 | `scripts/build_merged_rss.py` | 编辑：logging、摘要输出、`--strict`、`--readme-snippet` | 2 + 3 |
| 3 | `.github/recent-activity.config.yml` | 编辑：max_lines 6 → 10 | 2 |
| 4 | `.github/workflows/rss-feed.yml` | 编辑：新增 snippet 生成 + README commit | 2 |
| 5 | `.github/workflows/metrics.yml` | 编辑：钉版本、paths-ignore | 3 |
| 6 | `.github/workflows/snake.yml` | 编辑：checkout v4、paths-ignore | 3 |
| 7 | `.github/dependabot.yml` | **新建** | 3 |

共 7 个文件，其中 6 个编辑、1 个新建，零删除。
