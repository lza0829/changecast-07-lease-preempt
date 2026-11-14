# 07 · 投影流租约（过期、心跳、抢占/回收、提交栅栏）

在前一版「同流并发只放行一趟」的互斥基础上，本切片把开跑互斥升级为**带过期时间与栅栏令牌的流租约**，
并补齐进程崩溃后的解锁路径：过期不能被无限占位，解锁也不能造成双写。

## 租约模型

| 字段 | 含义 |
| --- | --- |
| `lease_token` | 领取时生成，兼作**栅栏令牌**；提交必须原样带回 |
| `generation` | 每领取一次 +1；抢占/回收后旧代令牌永久失效 |
| `expires_at` | 领取或心跳时刻 + TTL（默认 120s，可 2–3600s） |
| `state` | `active` / `preempted` / `reclaimed` / `released`；`active` 且已过 `expires_at` 对外呈现为 `expired` |

租约在 `stream_leases` 上按流唯一；`lease_events` 记录 `acquired / rejected / heartbeat / preempt /
reclaim / released / completed / fenced_commit` 八类事件，计数由账本直接聚合。

## 规则

1. **租约内同流其他开跑一律拒绝**（`LEASE_HELD`），不建批次、不动投影。
2. **心跳续租**：只有未被抢占、未过期、令牌匹配的持有者能续租；对过期租约心跳返回 `LEASE_EXPIRED`。
3. **过期后按策略互斥**（`settings.lease_preempt_policy`）：
   - `allow_preempt`：新开跑作废旧批（`preempted`），写 `preempt` 事件并记原因，随后领取新一代租约。
   - `refuse_preempt`：仍拒绝新开跑（`LEASE_EXPIRED_REFUSED`），直到 `POST /api/leases/reclaim`
     或 `POST /api/leases/release` 把旧批标失败并释锁。
4. **栅栏提交**：投影只有在「批次仍 pending + 租约 active + 令牌匹配 + 未过期」时才允许写入投影表、
   事件留痕与水位；任何一条不满足都返回 `FENCED`，批次落为 `fenced/failed/preempted`，
   **投影与水位不变**。旧批迟到的提交无法覆盖新批。

## 接口

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `GET` | `/api/leases` | 各流租约、策略、默认 TTL、计数器 |
| `GET` | `/api/leases/{stream_id}` | 单流租约 + 计数器 + 最近完成批次 |
| `GET` | `/api/leases/events?stream_id=` | 租约账本 |
| `GET` | `/api/leases/audit` | 投影 / 水位 / 批次 / 账本互证 |
| `PUT` | `/api/leases/policy` | 切换抢占策略与默认 TTL |
| `POST` | `/api/leases/acquire` | 领取租约（可只占位不开跑） |
| `POST` | `/api/leases/heartbeat` | 心跳续租 |
| `POST` | `/api/leases/release` | 显式释放，旧批标 `failed` |
| `POST` | `/api/leases/reclaim` | 回收**已过期**租约，旧批标 `failed` |
| `POST` | `/api/project` | 领取 → 暂存 → 栅栏提交的整趟开跑 |
| `POST` | `/api/runs/start` | 领取并暂存但**不提交**（模拟进程崩溃） |
| `POST` | `/api/runs/commit` | 崩溃批次恢复后提交，用于演示被栅栏挡住 |

错误体统一为 `{"detail": {"code": "...", "message": "...", ...}}`，码表：
`LEASE_HELD / LEASE_EXPIRED_REFUSED / LEASE_EXPIRED / LEASE_NOT_HELD / LEASE_NOT_EXPIRED /
LEASE_TOKEN_MISMATCH / FENCED`。

## 页面演示（http://localhost:5400/leases「租约台」）

1. 点「领取租约」占住某条流 → 点「同流再开跑」→ 被拒，`拒绝次数 +1`。
2. 点「模拟崩溃开跑」→ 出现 `pending` 批次并占住租约；TTL 到期后状态变「已过期（未回收）」。
3. 按策略二选一：
   - 「允许抢占」下直接点「开跑」→ 旧批 `preempted`、`抢占次数 +1`，账本记抢占原因；
   - 「禁止抢占」下开跑仍被拒 → 点「回收过期租约」→ 旧批 `failed`，随后可正常开跑。
4. 点「旧批迟到提交」→ 返回 `FENCED`，投影快照与水位都不变；`栅栏拦截 +1`。
5. 面板底部「投影与水位互证」列出投影计数 vs 最近完成批次、水位 vs `max_seq`、
   投影行是否有事件留痕、作废批次是否有原因等校验项。

## 互证口径（`/api/leases/audit`）

- 存活的投影行数 / 已删行数 == 最近完成批次的 `projected_alive` / `projected_deleted`
- 流的 `watermark_seq` == 最近完成批次的 `max_seq`
- 每条投影行的 `applied_event_id` 都能在 `applied_events` 找到留痕
- `applied_events` 只挂在 `completed` 批次上（被栅栏拦下的批次不产生留痕）
- `preempted / failed / fenced` 批次必须带 `fence_reason`
- `preempt` 事件数 ≥ 被抢占批次数
- 不存在「无 active 租约却仍 pending」的批次

## 测试

```bash
cd backend && python -m pytest -q      # 13 项，覆盖拒绝/心跳/过期两策略/栅栏/回收/释放/互证
```
