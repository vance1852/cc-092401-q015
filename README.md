# 人形机器人试验统计准入服务

本仓库是一套已经实现并可直接运行的服务端项目，用于把人形机器人在递送、讲解和灵巧操作等场景中的结构化试验记录转成可复核的统计准入结论。系统以 SQLite 保存机器人、软件构建、不可变协议版本、试验批次、原始观测、排除申请、分析快照、准入决定和审计事件，不依赖机器人设备、图片、音频、视频或外部基础设施。

现有代码按职责分为：

- `api.py`：标准库实现的 HTTP JSON 接口与无网络路由测试边界；
- `service.py`：角色权限、批次状态机、幂等导入、排除复核、任务租约、审批和报告；
- `export.py`：监管证据包导出任务、提交时冻结、分片 JSONL 与清单生成、摘要复核和独立命令入口；
- `analysis.py`：分层覆盖、Wilson 区间、描述性统计、确定性 bootstrap 和准入规则；
- `contracts.py`：协议、指标、分层、权重、随机种子和单次观测的数据契约；
- `jsonio.py`：严格 JSON/JSONL 读取、规范化序列化与内容摘要；
- `numeric.py`：不依赖第三方库的描述性统计和 Wilson 区间；
- `storage.py`：完整 SQLite 业务模式、约束、索引和事务辅助；
- `clock.py`：生产时钟与可确定性推进的测试时钟；
- `acceptance.py`：贯通建档、导入、封存、分析、审批和报告的离线验收。

系统已经实现以下主流程：协议发布后不可原地覆盖；批次按版本从草稿进入运行、封存、分析和决定状态；观测分片同时受请求幂等键和来源行唯一身份保护；排除请求必须由不同角色复核；分析任务使用 SQLite 租约避免重复执行并支持过期接管；同一输入快照使用固定算法版本和随机种子得到一致结果；分析者与审批人职责分离，报告保留输入摘要、统计规则和批次审计链。

监管报送场景下，审计人员可以一次提交数百个批次的导出请求。系统在提交时立即冻结每个批次引用的协议摘要、分析版本、决定和审计事件上界，后台工作进程按确定顺序把冻结记录写成分片 JSONL，并生成记录每个分片内容摘要、记录范围和整体摘要的清单。导出任务复用分析任务的租约模型，支持失败重试、到期接管和从最后已确认分片继续；同一导出请求重复提交返回同一任务而不产生重复产物。批次在冻结后产生的新事件不影响本次导出，任务元数据会标明事件截点和冻结后新增事件数。只有全部分片摘要复核成功后任务才完成，任何分片被篡改或缺失都会阻止完成并在校验结果中列出。

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

## 证据包导出命令

无需启动 HTTP 服务，可直接用独立命令创建、推进并校验监管证据包导出：

```bash
# 创建导出任务（同一批次集合与分片大小重复提交返回同一任务）
PYTHONPATH=src python3 -m robot_trials.export --database robot_trials.sqlite3 submit \
    --actor auditor-1 --batch batch-a --batch batch-b --shard-size 100

# 工作进程领取并推进任务，直到全部完成（支持崩溃后由其他进程接管续跑）
PYTHONPATH=src python3 -m robot_trials.export --database robot_trials.sqlite3 work \
    --worker export-worker-1 --export-root exports

# 查看任务状态、冻结截点和清单摘要
PYTHONPATH=src python3 -m robot_trials.export --database robot_trials.sqlite3 status \
    --actor auditor-1 --export-id 1

# 独立复核分片文件与清单摘要；全部一致时退出码为 0
PYTHONPATH=src python3 -m robot_trials.export --database robot_trials.sqlite3 verify \
    --export-id 1 --export-root exports
```

导出产物按 `exports/export-<id>/shard-XXXXXX.jsonl` 与 `exports/export-<id>/manifest.json` 组织，清单遵循 `robot-trials-export/1` 格式，记录每个分片的内容摘要、记录范围和整体摘要。

## 启动 HTTP 服务

```bash
PYTHONPATH=src python3 -m robot_trials.api --database robot_trials.sqlite3 --host 127.0.0.1 --port 8080
```

接口使用 `X-Actor-Id` 表示当前操作人，写入观测时还需提供 `Idempotency-Key`。正式使用前应先创建操作员、统计负责人、审批人和审计人员，再登记机器人、软件构建与协议版本。服务进程可以停止后重新启动，SQLite 中的业务状态、分析任务和租约信息会保留。
