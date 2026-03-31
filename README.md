# Topic10：API 调用与 SQLite 数据库管理

本项目实现了从数据获取到数据库管理再到 SQL 分析的完整流程，适用于课程作业提交与他人复现。


小组成员：林川胜、李泽欣、李贤记、冯晓怡。

## 与 Codex 对话记录
[查看对话](https://threads/019d328d-91dd-7cc2-b2b9-ae3d6156363b)

## 一、`setup.py` 是做什么的？
`setup.py` 是 Python 打包与安装配置文件，主要作用：
- 定义项目元信息（名称、版本、依赖）
- 支持本地可编辑安装：`pip install -e .`
- 注册命令行入口（console scripts）：
  - `topic10-build` -> `topic10_workflow.py` 的 `main()`
  - `topic10-update` -> `update_db.py` 的 `main()`

如果你只用于课程作业，`setup.py` 不是必须执行；但它能让项目更规范、便于他人复现。

## 二、项目里所有可执行文件做什么？按什么顺序用？

### 1. 可执行文件说明
- `01_api_download.ipynb`：下载/读取 FRED 与 A 股数据并缓存
- `02_database_setup.ipynb`：创建 SQLite 数据库并写入数据
- `03_sql_analysis.ipynb`：执行 SQL 分析与主题可视化
- `topic10_workflow.py`：脚本化一键建库主流程
- `update_db.py`：增量更新数据库（宏观 + A 股）

### 2. 推荐执行顺序（首次复现）
```bash
jupyter nbconvert --to notebook --execute --inplace 01_api_download.ipynb
jupyter nbconvert --to notebook --execute --inplace 02_database_setup.ipynb
jupyter nbconvert --to notebook --execute --inplace 03_sql_analysis.ipynb
```

执行完后，建议同步生成自动结论汇总（基于 notebook 的真实运行输出）：
```bash
python - <<'PY'
import nbformat
from pathlib import Path
root = Path('topic10_api_database')
out = root / 'output' / '自动结论汇总.md'
notebooks = [
    ('01_api_download.ipynb', '01 数据获取（FRED + A股）'),
    ('02_database_setup.ipynb', '02 数据库建库与写入'),
    ('03_sql_analysis.ipynb', '03 SQL 查询与主题分析'),
]
sections = []
for nb_name, title in notebooks:
    nb = nbformat.read(root / nb_name, as_version=4)
    md_text = '### 自动结论（真实结果）\n- 未找到输出，请先执行该 notebook。'
    for c in nb.cells:
        if c.cell_type == 'code' and '自动生成本 Notebook 的真实结果结论' in c.source:
            for o in c.get('outputs', []):
                if o.get('output_type') in ('display_data', 'execute_result'):
                    data = o.get('data', {})
                    if 'text/markdown' in data:
                        md_text = data['text/markdown']
                        break
            break
    if isinstance(md_text, list):
        md_text = ''.join(md_text)
    sections.append(f"## {title}\n\n{md_text.strip()}")
content = "# 自动结论汇总\n\n> 本文件由已执行的 notebook 自动结论单元汇总生成。\n\n" + "\n\n---\n\n".join(sections) + "\n"
out.write_text(content, encoding='utf-8')
print('written', out)
PY
```

### 3. 日常更新顺序
```bash
python update_db.py
```

## 三、项目介绍（你这个文件夹做了什么）
在 `topic10_api_database` 中已完成：
- FRED 通用 API 调用（任意序列、时间范围）
- baostock 批量下载（限频、失败重试、下载日志）
- SQLite 三张核心表 + 更新日志 + 数据质量表
- 3 个必做 SQL + 2 个自定义 SQL
- 主题分析：“美联储加息周期对人民币汇率影响”
- notebook 可执行版本与可视化输出

## 四、复现指南（虚拟环境版）

### 1. 创建并激活虚拟环境
```bash
cd topic10_api_database
python -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows
```

### 2. 安装依赖
```bash
pip install -r requirements.txt
```

可选（把项目安装成可调用命令）：
```bash
pip install -e .
```

### 3. 配置 API Key
```bash
cp .env.example .env
```
在 `.env` 中写入：
```env
FRED_API_KEY=你的FRED_API_KEY
```

### 4. 执行项目
方式 A（推荐课程提交）：按 notebook 顺序执行（见上）。

方式 B（脚本模式）：
```bash
python topic10_workflow.py
python update_db.py
```

## 五、项目结构

```text
topic10_api_database/
├── 01_api_download.ipynb
├── 02_database_setup.ipynb
├── 03_sql_analysis.ipynb
├── topic10_workflow.py
├── update_db.py
├── setup.py
├── requirements.txt
├── .env.example
├── .gitignore
├── cache/
│   ├── a_share/
│   ├── fred_macro_monthly_clean.csv
│   └── fred_macro_monthly_raw.csv
├── output/
│   ├── query1_spread.png
│   ├── fed_vs_fx.png
│   └── ...
└── fin_data.db
```

## 六、数据库说明
核心表：
- `macro_data(date, series_id, value)`
- `stock_price(code, date, open, high, low, close, volume, adj_close)`
- `stock_info(code, name, industry, list_date, market_cap)`

扩展表：
- `update_log`：更新记录
- `data_quality`：质量检测结果

## 七、提交建议输出
- `output/query1_spread.png`：收益率曲线利差图
- `output/fed_vs_fx.png`：利率与汇率对比图
- `output/自动结论汇总.md`：三个 notebook 的真实结果自动汇总文本（可直接贴报告）

## 八、定时运行（自动更新）

### 1. Linux / Mac（cron）
示例：每天 18:30 自动更新数据库
```bash
30 18 * * * cd /你的路径/topic10_api_database && /你的python路径/python update_db.py >> output/update_cron.log 2>&1
```

可先用命令编辑：
```bash
crontab -e
```

### 2. Windows（任务计划程序）
可在“任务计划程序”中新建基本任务：
1. 触发器：每天（如 18:30）  
2. 操作：启动程序  
3. 程序/脚本：`python.exe`  
4. 添加参数：`update_db.py`  
5. 起始于：`topic10_api_database` 目录  

## 九、常见问题
1. 没有 `FRED_API_KEY`：宏观数据无法更新。
2. baostock 网络波动：`update_db.py` 会回退本地缓存。
3. Jupyter 内核不对：请确认使用 `.venv` 对应解释器。
4. 不要提交：`.env`、`.venv/`、`*.db`、`__pycache__/`。
