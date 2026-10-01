# 实验 3 记录：流式读取与 consumer-id

日期：2026-09-25　|　阶段 0（对应教程 07、09 章）

> 版本：首次采集于 Paimon 2.2-SNAPSHOT（`d15d250cf`）；2026-10-02 用 `run-all.sh` 在 **2.0.0 正式版**上复核，关键结论一致。

运行：
```bash
cd labs && ./lab03-a.sh && ./lab03-b.sh
```
- `stream.sh <sql> <秒数>`：流模式运行，脚本最后一条 SELECT 持续读取，每行带 **RowKind + 墙钟时间**。
- `lab03-a.sh` / `lab03-b.sh`：编排脚本（后台流读 + 前台写入），日志在 `logs/lab03-*.log`。
- 日志里偶尔出现的 `MiniCluster is not yet running or has already been shut down` 是上一个作业关闭时的无害噪音。

## A：changelog-producer = none vs lookup

两张表只差 `changelog-producer`；第 1 轮在流读启动前写入，第 2、3 轮在流读运行中写入。

| 时间 | 写入 | orders_none 流读 | orders_lookup 流读 |
|---|---|---|---|
| 启动 | （已有订单 1、2） | `+I[1,CREATED]` `+I[2,CREATED]` | 同左 |
| 20:56:40 | 订单 1 → PAID | 20:56:45 `-U[1,CREATED]` `+U[1,PAID]` | 同左 |
| 20:56:54 | 删除订单 2 | 20:56:57 `-D[2,CREATED]` | 同左 |

**输出完全一样，区别在执行计划和快照：**

```
orders_none:                                orders_lookup:
ChangelogNormalize(key=[order_id])          DropUpdateBefore（EXPLAIN 无 sink 才有，实际 collect 保留 -U）
+- Exchange(hash[order_id])                 +- TableSourceScan(orders_lookup)
   +- TableSourceScan(orders_none)
```

| | orders_none | orders_lookup |
|---|---|---|
| 每次写入的快照 | 1 个 APPEND | APPEND + COMPACT（lookup 在提交前把 L0 合并上去） |
| `changelog_record_count` | NULL | 2（+I+I）→ 2（-U+U）→ 1（-D） |
| `-U` 由谁产生 | Flink `ChangelogNormalize`：keyed state 保存**每个 key 的最新值**，据此补出 -U | Paimon 写入时 lookup 产生（07 章），流读直接读 changelog 文件 |
| 流读扫描器 | `DeltaFollowUpScanner`（读 APPEND 快照的 delta） | `ChangelogFollowUpScanner`（读有 changelog 的快照） |

**踩坑收获（第一版实验失败的原因）**：表为空时启动流读，`tryFirstPlan` 返回 `SnapshotNotExistPlan`，要等一个 `discovery-interval`（默认 10s）再规划，而规划时做的是**最新快照的全量读**——若期间已写了两轮，读到的是合并后的状态（`+I[1,PAID]`），中间的变化历史看不到。**全量读只给结果，不给过程。**

## B：consumer-id

| 阶段 | 操作 | 结果 |
|---|---|---|
| B1 | 带 `consumer-id='lab03'` 流读 orders_c | 全量读到 `+I 1/2/3`；checkpoint 完成后写出 `consumer/consumer-lab03` = `{"nextSnapshot": 3}` |
| B2 | 流读停止期间写入：订单 1→PAID、删订单 2、加订单 4（两表都写，快照 3~8） | — |
| B2 | 两表都激进过期（retain_min=1, time-retained=1ms） | **orders_c 过期 2 个，保留 3~8**（被 consumer 保护）；**orders_nc 过期 7 个，只剩 8** |
| B3 | 同一个 consumer-id 重启，**没有任何 Flink 状态** | 直接从快照 3 继续：`-U[1,CREATED] +U[1,PAID] -D[2,CREATED] +I[4,CREATED]`，**不重复全量、不丢变更** |
| B4 | 对照组：orders_nc 从“停下的位置”快照 3 开始流读 | **不报错**，只读到 `+I[4,CREATED]`——订单 1 的更新和订单 2 的删除**静默丢失** |

### 源码对应
- **启动时必须配置 `consumer.expiration-time`**：第一次运行报错 “You need to configure 'consumer.expiration-time' ...”，校验在 `FlinkSourceBuilder`。目的：防止被遗弃的 consumer 让快照永远无法过期。
- **写 consumer**：`consumer.mode` 默认 exactly-once → `MonitorSource`，checkpoint 完成时 `scan.notifyCheckpointComplete(nextSnapshot)` → `ConsumerManager.resetConsumer`。
- **保护快照**：`ExpireSnapshotsImpl.expire()` 中 `maxExclusive = min(maxExclusive, consumerManager.minNextSnapshot())`。
- **从 consumer 恢复**：`AbstractDataTableScan` 中 `consumer.isPresent()` → `ContinuousFromSnapshotStartingScanner(consumer.nextSnapshot())`。
- **静默丢失的根源**：`ContinuousFromSnapshotStartingScanner.scan()`
  ```java
  // If the snapshotId < earliestSnapshotId, start from the earliest.
  return new NextSnapshot(Math.max(startingSnapshotId, earliestId));
  ```
  注意区分：**从 checkpoint 恢复**时如果 nextSnapshotId 已过期，`NextSnapshotFetcher.rangeCheck` 会抛 `OutOfRangeException`（明确失败）；而**用 `scan.snapshot-id` 指定起点**时会静默跳到最早快照。

## 结论
1. 流读主键表要选 changelog producer：`none` 把去重/补 -U 的成本转嫁给每个下游作业（ChangelogNormalize 的全量 keyed state），`lookup` 在写入端一次性产生。
2. 快照过期与流读是一对矛盾：没有 consumer 的保护，作业停久了要么恢复失败（OutOfRange），要么（指定起点时）静默丢数据。
3. consumer-id = 把读取位点持久化到表里，既能让表“知道”不能过期哪些快照，又能在没有 Flink 状态时续读。

## 思考题
1. B3 中重启作业读到的 `-U[1,CREATED]`：如果 orders_c 用的是 `changelog-producer=none`，重启后没有 ChangelogNormalize 的历史状态，会输出什么？删除订单 2 还能正确输出吗？
2. 多个下游作业读同一张表时，快照会被保留到哪个 consumer 的位置？一个作业长期不运行会怎样？`consumer.expiration-time` 起什么作用？
3. `consumer.mode = at-least-once` 走的是哪条代码路径？为什么会“至少一次”？（提示：09 章 `ConsumerProgressCalculator`）
4. 为什么 lookup 表每次写入会多出一个 COMPACT 快照？流读为什么只读 COMPACT 快照？（提示：changelog 挂在哪个快照上）
