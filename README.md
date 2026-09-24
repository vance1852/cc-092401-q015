# 人形机器人试验统计准入服务

本仓库是一套已经实现并可直接运行的服务端项目，用于把人形机器人在递送、讲解和灵巧操作等场景中的结构化试验记录转成可复核的统计准入结论。系统以 SQLite 保存机器人、软件构建、不可变协议版本、试验批次、原始观测、排除申请、分析快照、准入决定和审计事件，不依赖机器人设备、图片、音频、视频或外部基础设施。

现有代码按职责分为：

- `api.py`：标准库实现的 HTTP JSON 接口与无网络路由测试边界；
- `service.py`：角色权限、批次状态机、幂等导入、排除复核、任务租约、审批和报告；
- `analysis.py`：分层覆盖、Wilson 区间、描述性统计、确定性 bootstrap 和准入规则；
- `contracts.py`：协议、指标、分层、权重、随机种子和单次观测的数据契约；
- `jsonio.py`：严格 JSON/JSONL 读取、规范化序列化与内容摘要；
- `numeric.py`：不依赖第三方库的描述性统计和 Wilson 区间；
- `storage.py`：完整 SQLite 业务模式、约束、索引和事务辅助；
- `clock.py`：生产时钟与可确定性推进的测试时钟；
- `exports.py`：持久化证据包导出的确定性分片、原子写入与清单复核；
- `exportctl.py`：不启动 HTTP 服务即可创建、推进和校验导出的独立命令行；
- `acceptance.py`：贯通建档、导入、封存、分析、审批、报告和证据包导出的离线验收。

系统已经实现以下主流程：协议发布后不可原地覆盖；批次按版本从草稿进入运行、封存、分析和决定状态；观测分片同时受请求幂等键和来源行唯一身份保护；排除请求必须由不同角色复核；分析任务使用 SQLite 租约避免重复执行并支持过期接管；同一输入快照使用固定算法版本和随机种子得到一致结果；分析者与审批人职责分离，报告保留输入摘要、统计规则和批次审计链。

监管报送还提供**持久化证据包导出任务**：审计人员提交批次集合后，系统在同一个事务中立即冻结每个批次引用的协议、分析、决定摘要以及观测、审计事件的上界（`observation_high_id`/`event_high_id` 与冻结时刻）；后台工作进程领取租约后，按批次请求顺序和固定记录类型顺序生成分片 JSONL，每片先原子落盘再独立复核，只有全部分片摘要复核通过后才汇总生成清单并把任务置为完成。清单记录每个分片的记录范围、首尾记录键、记录类型分布、内容摘要以及覆盖全部摘要的整体摘要。任务复用与分析任务相同的租约模型，支持失败重试、到期接管和“从最后已确认分片继续”；同一导出请求（批次集合、顺序、分片大小、输出目录）由请求摘要派生任务编号，重放返回同一任务而不是重复产物。批次在冻结之后产生的新事件不属于本次导出，清单元数据标明截点；任一分片被篡改或缺损，任务都会失败且不会产出清单。

## 环境

- Linux
- Python 3.11 或更高版本
- 无需安装第三方 Python 包

如需安装到隔离环境，可在依赖已经准备好的容器中执行：

```bash
python3 -m pip install --no-index --no-deps .
```

## 测试

在 `project/` 目录执行：

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

测试使用临时目录和内存数据库，不访问网络，也不依赖常驻服务。

## 构建检查

本项目是纯 Python 源码包，构建检查采用字节码编译：

```bash
python3 -m compileall -q src tests
```

## 无浏览器验收

下面的命令会读取 `fixtures/` 中的协议与观测记录，在临时 SQLite 数据库中完成用户与设备建档、协议发布、批次启动、观测导入、批次封存、任务领取、统计分析、准入审批和审计报告导出，随后输出一行 JSON 结果：

```bash
PYTHONPATH=src python3 -m robot_trials.acceptance --workspace .
```

成功时退出码为 `0`，输出中的 `status` 为 `ok`。验收过程不会写入仓库，也不需要浏览器或外部服务。

## 证据包导出命令（无需启动 HTTP）

证据包导出可以完全通过 `exportctl` 离线驱动，所有任务、租约和分片确认状态都持久化在 SQLite 中，命令可反复执行、跨进程接管：

```bash
# 1. 审计人员提交批次集合并立即冻结上界（重复提交同一请求返回同一任务编号）
PYTHONPATH=src python3 -m robot_trials.exportctl create \
  --database robot_trials.sqlite3 --actor auditor-1 \
  --batches batch-001,batch-002,batch-003 \
  --output-dir evidence --records-per-shard 1000

# 2. 工作进程领取任务（租约到期后可被其他进程接管）
PYTHONPATH=src python3 -m robot_trials.exportctl claim \
  --database robot_trials.sqlite3 --worker worker-1 --lease-seconds 300

# 3. 从最后已确认分片继续推进（可分多次执行；中途崩溃后重跑即可）
PYTHONPATH=src python3 -m robot_trials.exportctl advance \
  --database robot_trials.sqlite3 --worker worker-1 \
  --task exp-<请求摘要> --max-shards 10

# 或者一步领取并循环推进到完成
PYTHONPATH=src python3 -m robot_trials.exportctl process \
  --database robot_trials.sqlite3 --worker worker-1 --max-shards-per-turn 10

# 4. 查看状态 / 独立复核清单与全部分片
PYTHONPATH=src python3 -m robot_trials.exportctl status \
  --database robot_trials.sqlite3 --actor auditor-1 --task exp-<请求摘要>
PYTHONPATH=src python3 -m robot_trials.exportctl verify \
  --database robot_trials.sqlite3 --task exp-<请求摘要>
```

产物写入 `evidence/exp-<请求摘要>/`：`data-00000.jsonl` 等分片文件和 `manifest.json` 清单。`verify` 会重读磁盘上的清单与每个分片，比对内容摘要、记录范围、类型分布并复核整体摘要，任一项被篡改都会以非零退出码和 `export_integrity_failure` 报错。

## 启动 HTTP 服务

```bash
PYTHONPATH=src python3 -m robot_trials.api --database robot_trials.sqlite3 --host 127.0.0.1 --port 8080
```

接口使用 `X-Actor-Id` 表示当前操作人，写入观测时还需提供 `Idempotency-Key`。正式使用前应先创建操作员、统计负责人、审批人和审计人员，再登记机器人、软件构建与协议版本。服务进程可以停止后重新启动，SQLite 中的业务状态、分析任务和租约信息会保留。
