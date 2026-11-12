# 量衡仪器测量处理

这是一个可独立运行的本地业务项目。核心模块保留数据接收、领域规则、持久化记录及查询接口；演练数据均为合成资料，不需要真实账号或外部业务服务。

## 运行环境

Python 3.11 或更新版本，核心业务使用标准库与 SQLite。

Dockerfile 固定运行时版本。首次准备执行 docker build -t local-product .，之后以 docker run --rm --network none local-product 运行完整检查；所有 HTTP 演练只在容器内部回环地址通信。

## 开发与运行

业务测试：
    python -m unittest discover -s tests -v
编译或语法检查：
    python -m compileall -q metrology demo_support tests
现有业务演示：
    python -m metrology demo
输入端演练检查：
    python demo_support/check.py
网关二进制直采综合演练：
    python -I demo_support/drill_gateway.py
合并运行：
    sh demo_support/verify.sh

输入协议、样本与故障开关见 demo_support/协议.md。输入端检查只证明本地来源的约定行为；业务演示展示当前已有功能。调用者可以在独立临时数据目录重复运行，原始样本不会被覆盖。

## 网关二进制直采

新网关的二进制观测批流无需先拆成 JSON，可直接进入计量处理链。控制台动作（经 python -m metrology run 动作.json 调用）：

- gateway_directory：注册带有效时段的通道目录版本（document 可为内联对象或 JSON 文件路径）；gateway_directories 列出已注册版本。
- gateway_collect：从回环 http URL 启动一个有界、可取消的采集会话，参数含 session、gateway、url、received_at、chunk_size、timeout。
- gateway_session / gateway_events / gateway_frames：查看终态、重同步与 CRC 证据、逐帧接收记录（可按 status 过滤）。
- gateway_reprocess：目录版本补齐后重处理会话中的 deferred 帧；gateway_reprocess_frame：对单个原始帧做隔离重处理。

帧身份包含 boot_id 与序号回绕世代；重发与同序异载荷分开记录；流尾 counts 与实到帧对账，缺帧、尾标损坏与正常结束是不同状态。未知目录版本的帧保持 deferred，不会用当前校准凑出值。
