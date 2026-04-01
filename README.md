---
layout: default
title: 首页
---

# 📊 Topic 10：API 调用与 SQLite 数据库管理

**从数据获取到数据库管理再到 SQL 分析的完整流程**

[![Python](https://img.shields.io/badge/Python-3.8%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![SQLite](https://img.shields.io/badge/SQLite-3.x-003B57?logo=sqlite&logoColor=white)](https://www.sqlite.org/)
[![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-F37626?logo=jupyter&logoColor=white)](https://jupyter.org/)

---

## 👥 小组成员

林川胜、李泽欣、李贤记、冯晓怡

---

## 📖 项目简介

本项目实现了从数据获取到数据库管理再到 SQL 分析的完整流程，适用于课程作业提交与他人复现。

### 核心功能

- **FRED API 调用** - 任意序列、时间范围的宏观经济数据
- **baostock 批量下载** - 限频、失败重试、下载日志
- **SQLite 数据库** - 三张核心表 + 更新日志 + 数据质量表
- **SQL 分析** - 3 个必做查询 + 2 个自定义查询
- **主题研究** - "美联储加息周期对人民币汇率影响"

---

## 📁 项目结构

```
topic10/
├── 📓 01_api_download.ipynb      # 数据下载 Notebook
├── 📓 02_database_setup.ipynb    # 数据库构建 Notebook
├── 📓 03_sql_analysis.ipynb      # SQL 分析 Notebook
├── 🐍 topic10_workflow.py        # 一键建库脚本
├── 🐍 update_db.py               # 增量更新脚本
├── 📄 setup.py                   # 包安装配置
├── 📄 requirements.txt           # 依赖清单
├── 📄 .env.example               # 环境变量模板
│
├── 📂 cache/                     # 数据缓存
│   ├── a_share/                  # A 股数据
│   ├── fred_macro_monthly_raw.csv
│   └── fred_macro_monthly_clean.csv
│
├── 📂 output/                    # 输出结果
│   ├── query1_spread.png         # 收益率曲线利差图
│   ├── fed_vs_fx.png             # 利率与汇率对比图
│   └── 自动结论汇总.md
│
└── 🗄️ fin_data.db                # SQLite 数据库
```

---

## 🚀 快速开始

### 1️⃣ 创建并激活虚拟环境

```bash
cd topic10
python -m venv .venv

# macOS / Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

### 2️⃣ 安装依赖

```bash
pip install -r requirements.txt
```

> 💡 **可选**：安装为可调用命令 `pip install -e .`

### 3️⃣ 配置 API Key

```bash
cp .env.example .env
```

编辑 `.env` 文件：

```env
FRED_API_KEY=你的FRED_API_KEY
```

### 4️⃣ 执行项目

**方式 A（推荐）**：按 Notebook 顺序执行

```bash
jupyter lab
# 依次运行 01 → 02 → 03 notebook
```

**方式 B**：脚本模式

```bash
python topic10_workflow.py
python update_db.py
```

---

## 📓 Notebook 说明

### 可执行文件功能

| 文件 | 功能 |
|:---|:---|
| `01_api_download.ipynb` | 下载/读取 FRED 与 A 股数据并缓存 |
| `02_database_setup.ipynb` | 创建 SQLite 数据库并写入数据 |
| `03_sql_analysis.ipynb` | 执行 SQL 分析与主题可视化 |
| `topic10_workflow.py` | 脚本化一键建库主流程 |
| `update_db.py` | 增量更新数据库（宏观 + A 股） |

### 推荐执行顺序（首次复现）

```bash
jupyter nbconvert --to notebook --execute --inplace 01_api_download.ipynb
jupyter nbconvert --to notebook --execute --inplace 02_database_setup.ipynb
jupyter nbconvert --to notebook --execute --inplace 03_sql_analysis.ipynb
```

### 日常更新

```bash
python update_db.py
```

---

## 🗄️ 数据库说明

### 核心表

| 表名 | 字段 | 说明 |
|:---|:---|:---|
| `macro_data` | date, series_id, value | 宏观经济数据 |
| `stock_price` | code, date, open, high, low, close, volume, adj_close | 股票行情 |
| `stock_info` | code, name, industry, list_date, market_cap | 股票信息 |

### 扩展表

| 表名 | 说明 |
|:---|:---|
| `update_log` | 更新记录 |
| `data_quality` | 质量检测结果 |

---

## 📈 输出结果

| 文件 | 说明 |
|:---|:---|
| `output/query1_spread.png` | 收益率曲线利差图 |
| `output/fed_vs_fx.png` | 利率与汇率对比图 |
| `output/自动结论汇总.md` | 三个 notebook 的真实结果自动汇总文本 |

---

## ⚙️ setup.py 说明

`setup.py` 是 Python 打包与安装配置文件，主要作用：

- 定义项目元信息（名称、版本、依赖）
- 支持本地可编辑安装：`pip install -e .`
- 注册命令行入口：
  - `topic10-build` → `topic10_workflow.py` 的 `main()`
  - `topic10-update` → `update_db.py` 的 `main()`

> 💡 如果只用于课程作业，`setup.py` 不是必须执行；但它能让项目更规范、便于他人复现。

---

## ⏰ 定时运行（自动更新）

### Linux / Mac（cron）

```bash
# 编辑定时任务
crontab -e

# 每天 18:30 自动更新数据库
30 18 * * * cd /你的路径/topic10 && /你的python路径/python update_db.py >> output/update_cron.log 2>&1
```

### Windows（任务计划程序）

1. 触发器：每天（如 18:30）
2. 操作：启动程序
3. 程序/脚本：`python.exe`
4. 添加参数：`update_db.py`
5. 起始于：`topic10` 目录

---

## ❓ 常见问题

| 问题 | 解决方案 |
|:---|:---|
| 没有 `FRED_API_KEY` | 前往 [FRED 官网](https://fred.stlouisfed.org/docs/api/api_key.html) 免费申请 |
| baostock 网络波动 | `update_db.py` 会自动回退本地缓存 |
| Jupyter 内核不对 | 确认使用 `.venv` 对应解释器 |
| 不应提交的文件 | `.env`、`.venv/`、`*.db`、`__pycache__/` 已在 `.gitignore` 排除 |

---

<p align="center">
<b>Made with ❤️ by Topic 10 Team</b>
</p>
