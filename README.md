# 04-changecast（变投影）

Changecast — 变更日志投影：源变更流 → 按序投影 → 乱序/晚到策略钩子 → 快照可查互证。

与 Feedstitch（多源 Feed 归并）、Batchgate（作业依赖编排）、Syncledger（双端对账）主链路不同。夹具全本地，不连活库。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5400 |
| API | http://localhost:10400 |

## 主链

配置投影流 → 领取带过期时间的流租约 → 读取 CDC 夹具并按 seq 应用到投影表 → 栅栏提交批次/水位 →
快照与租约账本互证

开跑互斥已升级为**租约**（过期 + 心跳续租 + 抢占/回收 + 提交栅栏），详见
[docs/07-lease-preempt.md](docs/07-lease-preempt.md)。
租约内同流第二趟一律拒绝；进程崩溃留下 `pending` 批次后，到期可按策略抢占或回收，
旧批的迟到提交返回 `FENCED` 且不改投影/水位。

演示入口：前端「租约台」 http://localhost:5400/leases 。

## 种子投影流

- **member_profile**：会员档案变更（`member_id`，字段 `display_name` / `tier` / `region`）
- **store_archive**：门店档案变更（`store_id`，字段 `name` / `status` / `city`）
- **channel_attr**：渠道属性变更（`channel_code`，字段 `label` / `active`）

底座默认按 **事件 seq 升序** 投影；乱序缓冲表空挂，深逻辑留给后题。对外文案不用「工单」「CRM」。

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 测试

```bash
cd backend && python -m pytest -q
```

## 0-1 切口（未实现，见出题规划）

`arrival_vs_event_order` / `ooo_window` / `event_idempotency` / `tombstone_policy` / `watermark_rewind` / …
