# 优先级门控队列：设计取舍与一致性保证

## 背景

旧的门控指令队列（`psdline/store/commands.py` 的 `CommandLog`）是按列车存放的
先来先服务待办列表，没有执行器、优先级和时效概念——一条紧急关门如果排在一批开门
指令后面，就得等前面全部跑完，容易误事。

`psdline/door/pq/` 在**保留旧链路**给既有到发编排使用的同时，新增一条独立的优先级
执行线，满足：紧急指令插队、普通指令有序、超时自动作废并记原因、作废后回到明确
门态，并把并发、部分失败、幂等、崩溃安全、历史不可变都落到存储语义里。

## 关键取舍

### 1. 插队：抢占普通，不抢占紧急（默认，可配置）

紧急关门入队时：

* 对待执行队列里的**开门类**指令直接顶替（`superseded_by_emergency`），关门类保留；
* 对**正在执行**的普通指令（如批量开门），在最小可中断单元（一个门区）边界中止
  （`preempted`），随后立刻执行紧急关门；
* **绝不打断另一条紧急/关门指令**——关门动作本身不可被打断，紧急指令之间严格 FIFO。

配置项 `pq.policy` 可切到 `wait_running`：任何指令一旦开始执行就不打断，紧急指令只
插到待执行队首，牺牲一点实时性换取完全可预期。

### 2. 时效：只约束"等待"，紧急关门不过期

每条指令提交时按 `submitted_at + ttl_s` 存绝对截止时间。选取前统一结算：

* 已过截止时间且仍在等待的指令作废（`expired`）；
* 已开始执行（RUNNING）的指令不受截止时间影响，只受紧急抢占影响；
* 紧急关门与系统故障安全关门默认没有截止时间——安全义务不能被时效悄悄丢弃；
* 普通指令默认 30s，可按指令显式覆盖或设为永不过期。

### 3. 作废留痕：全部保留且防篡改

队列状态完全由一条 **SHA-256 哈希链流水**（`pq/events/<train>.jsonl`）重建：

* 每个事件含 `prev_hash`，首条以固定创世锚点为前驱；任何对历史的增、删、改都会在
  `verify` 的断链或哈希不符处暴露；
* 状态快照 `pq/state/<train>.json` 只是**可删除重建的检查点缓存**，终态指令不进快照；
* 作废/失败/中止都有明确原因码（见下），提供 `pq-dispositions` 处置结果查询与
  `pq-history` 不可变流水查询。

| 原因码 | 触发 |
| --- | --- |
| `expired` | 超过截止时间仍未开始执行 |
| `superseded_by_emergency` | 待执行的开门类指令被后来的紧急关门顶替 |
| `preempted` | 正在执行的普通指令被紧急关门在可中断边界中止 |
| `crashed` | 上一属主崩溃，恢复时开门类指令无法确认物理态 |
| `failed` | 执行器重试耗尽，指令无法完成 |

### 4. 作废/失败后回到明确状态：FAILSAFE_CLOSE → CLOSED

任何异常终结都把涉及站台登记为"不安全"（`unsafe_stations`），门态置 `unknown`；
当队列空闲时自动补一条**系统级故障安全关门**（紧急优先级、不过期、幂等），关门
成功后不安全站台收敛、门态明确回到 `closed`。

* 故障安全关门是恢复手段，**不会顶替**仍有效的普通开门指令（只有客户端紧急关门会）；
* 若故障安全关门本身持续失败，队列停在显式的"阻塞待修复"现场
  （`failsafe_blocked`、门态 `unknown`），不无限重试；现场修复后由显式
  `retry-failsafe` 补一条新的故障安全关门，旧失败记录不可变保留。

## 一致性与可靠性保证

* **并发**：每列车一把进程内 `RLock` + 一把跨进程 `fcntl.flock`（`pq/locks/`）。
  不同列车互不阻塞。打开流程在锁内先从磁盘 `resync` 哈希链尾再追加，多进程并发
  入队序号连续、链不断。
* **幂等**：入队以 `(train, command_id)` 为幂等键，重复提交返回原指令并留
  `submit_duplicate` 痕迹；载荷冲突（同 ID 不同 kind/station）拒绝。关门物理动作
  幂等（已关闭的门重复关门仍成功），崩溃恢复可安全重放。
* **部分失败恢复**：执行器失败分瞬时（重新入队首重试，默认最多 3 次）与持续
  （重试耗尽作废并转故障安全关门）。
* **崩溃安全**：每次状态迁移先把事件追加并 `fsync` 到哈希链，再（节流地）更新
  快照检查点。崩溃窗口的几种情形：
  * 链写一半留下半行 → 新属主 `resync` 时截断，该事件物理动作未完成，走恢复裁决；
  * 遗留 RUNNING 的**开门类**指令 → 判 `crashed`（门可能半开），转故障安全关门；
  * 遗留 RUNNING 的**关门类**指令 → 因关门幂等而安全续做；
  * 只有不安全站台没有 RUNNING → 补故障安全关门。
  属主代际 `owner_epoch` 随 CLAIMED 落链，新进程用不同代际识别"非本属主遗留"。
* **历史不可变**：流水只追加；快照不含终态，删除后从链全量/增量重建（实测全量重建
  仅毫秒级）。
* **队列 / 门态 / 审计一致**：哈希链是唯一事实源，平台审计是它的**可重建镜像**。
  每条事件按事件哈希（`record_once`）幂等镜像进 `AuditLog`；镜像失败只登记待补，
  下轮 `resync_audit` 按链补齐。`view` 同时给出队列门态与执行器物理门态对照。

## 对外入口

* 库：`PriorityQueueRunner.submit / pump / drain / view / dispositions / history /
  verify_chain / retry_failsafe / is_safe`。
* 配置：`pq.policy / default_ttl_s / emergency_ttl_s / max_attempts`。
* HTTP：`/api/pq-submit|pump|drain|retry-failsafe|safe`、
  `/api/pq-view|pq-dispositions|pq-history/{train}`、`/api/pq-verify`、
  `/api/pq-worker/start|stop`。
* CLI：`psdline pq {submit,pump,drain,retry-failsafe,safe,view,dispositions,history,
  verify,worker-start,worker-stop}`。
* 后台线程 `PQWorker` 周期泵送，紧急入队立即唤醒；确定性仿真与测试直接调用
  `pump`。
