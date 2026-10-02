# 实验 2 记录：时间旅行与快照过期

日期：2026-09-25　|　阶段 0

> 版本：首次采集于 Paimon 2.2-SNAPSHOT（`d15d250cf`）；2026-10-02 用 `run-all.sh` 在 **2.0.0 正式版**上复核，关键结论一致。

运行：
```bash
cd labs && ./run.sh sql/lab02/step1-setup.sql && ./run.sh sql/lab02/step2-time-travel.sql && ./run.sh sql/lab02/step3-expire.sql
```

## 快照历史与文件引用关系（步骤 1 + 步骤 3 的 ⓪）

| 快照 | 类型 | 引用的数据文件 |
|---|---|---|
| 1 | APPEND | `ff3609ed`、`e85503d2`、`81377c95` |
| 2 | APPEND | 快照 1 的 3 个 + `a51fa8bf`（订单 1 新版本）+ `bc9affb4`（订单 6） |
| 3 | APPEND | 快照 2 的 5 个 + `d99ce62a`（订单 3 的 -D 记录） |
| 4 | COMPACT | 只有 `ca85fdf6`、`a2a9870d`（level 5） |

结论：**快照 = 某一时刻“哪些文件有效”的清单**；文件本身不可变，被多个快照共享。

## 步骤 2：时间旅行

| 操作 | 结果 | 说明 |
|---|---|---|
| `scan.snapshot-id = 1 / 2 / 3` | 5 行 / 6 行（订单 1=PAID、多了订单 6）/ 5 行（订单 3 消失） | 读哪个快照，就用那个快照的 manifest 清单 |
| `scan.snapshot-id = 99` | `out of available snapshotId range [1, 4]` | 只能读 EARLIEST~LATEST 之间 |
| `create_tag v1 → snapshot 1` | `tag/tag-v1` 与 `snapshot/snapshot-1` **内容完全相同** | Tag 就是把快照 JSON 复制一份放到 `tag/` 目录，不受快照过期影响 |
| `$audit_log` + `incremental-between = '1,3'` | `+I 订单1(PAID)`、`-D 订单3`、`+I 订单6` | 增量读 = 快照 2、3 的 delta 文件；订单 1 是 `+I` 而不是 `+U`，因为 `changelog-producer=none`，delta 里存的是 upsert 记录 |

补充：`-D 订单3` 带着完整的旧值（amount=250.00）——Flink 的 DELETE 会先读出命中行，再以 `-D` 写回（回答了实验 1 的思考题 2）。

## 步骤 3：快照过期的三道保险

源码：`table/ExpireSnapshotsImpl.expire()`

```java
long min = Math.max(latestSnapshotId - retainMax + 1, earliest);   // 超出 max 的强制过期
long maxExclusive = latestSnapshotId - retainMin + 1;              // 最近 min 个永远保留
maxExclusive = Math.min(maxExclusive, consumerManager.minNextSnapshot()...);  // consumer 保护
for (long id = min; id < maxExclusive; id++) {
    if (olderThanMills <= nextSnapshot.timeMillis()) return expireUntil(earliest, id);  // 不够老就停
}
return expireUntil(earliest, maxExclusive);
```

| 调用 | 结果 | 原因 |
|---|---|---|
| `retain_max => 2` | 报错 `retainMax (2) must not be less than retainMin (10)` | retain_min 取表默认值 10 |
| `retain_max => 3, retain_min => 1` | 过期 **1** 个（快照 1） | 快照 1 超出 max，强制过期；快照 2、3 在 max 内，但距今不到 1 小时（time-retained） |
| 同上 + `snapshot.time-retained=1ms` | 过期 **2** 个（快照 2、3） | 时间保护解除，只保留最近 1 个 |

**数据文件什么时候被删？**
- 第一次过期（删快照 1）：**一个数据文件都没删**——快照 1 的 3 个文件仍被快照 2 和 Tag v1 引用。
- 第二次过期（删快照 2、3）：删掉 `a51fa8bf`、`bc9affb4`、`d99ce62a`——只被快照 2/3 引用的文件；快照 1 的 3 个文件因为 **Tag v1 还引用着**而保留。
- 快照 1 按 id 已读不到（`range [4, 4]`），但 **`scan.tag-name = 'v1'` 仍然读出快照 1 的 5 行**。
- `delete_tag v1` 后：Tag 独占的 3 个文件被删除，最终只剩快照 4 的 2 个文件。

规则：**一个数据文件只有在“没有任何存活的快照或 Tag 引用它”时才会被物理删除。**（`SnapshotDeletion` / `TagDeletion`）

## 与源码的对应
| 现象 | 源码 |
|---|---|
| 按快照号 / Tag 读 | `table/source/snapshot/StaticFromSnapshotStartingScanner`、`StaticFromTagStartingScanner`、`TimeTravelUtil` |
| 增量读 | `IncrementalDeltaStartingScanner` / `IncrementalDiffStartingScanner`，`$audit_log` 系统表 |
| Tag | `tag/`、`utils/TagManager`（`tag/tag-<name>` 文件） |
| 过期三道保险 | `ExpireSnapshotsImpl.expire()`、`ProcedureUtils.fillInSnapshotOptions` |
| 删文件 | `operation/SnapshotDeletion`、`TagDeletion`、`FileDeletionBase` |

## 思考题
1. 生产上 Flink 流作业每个 checkpoint 产生 1~2 个快照，默认参数下快照最多保留多久、至少保留几个？
2. 为什么过期判断用的是“**下一个**快照的时间”而不是自身的时间？（提示：一个快照在它被下一个快照取代之前一直是“最新”的）
3. 流读作业停了 2 小时后重启，会发生什么？怎么避免？（回顾 09 章 consumer-id，代码里就是 `consumerManager.minNextSnapshot()` 那一行）
4. Tag 可以设 `time_retained`，到期自动删除。它和快照过期是谁在执行？（提示：`TableCommitImpl.maintain()`）
5. 如果手动删掉某个快照引用的 parquet 文件，读这个快照会怎样？`remove_orphan_files` 删的又是哪类文件？

## 补充：按时间旅行与文件共享（`sql/lab02/time-travel-by-time.sql`，S1 第 6 期素材）

```bash
./run.sh sql/lab02/time-travel-by-time.sql
```

三次提交、每次间隔 3 秒（Paimon 2.0.0）：

| 观察 | 结果 |
|---|---|
| 每个快照引用的数据文件 | 快照 1/2/3 分别 1/2/3 个，快照 1 的文件被 3 个快照共用；磁盘上一共 3 个 parquet |
| 每个快照的 manifest | 1/2/3 个，旧 manifest 被新快照直接复用（数量少，未触发 manifest 合并） |
| snapshot JSON | 每个 595 字节，只记 base / delta manifest list 的文件名 |
| `scan.timestamp-millis` = 快照 2 提交时间、快照 2 与 3 之间 | 都读到快照 2 |
| `FOR SYSTEM_TIME AS OF TIMESTAMP '…'`（快照 2 与 3 之间） | 快照 2 |
| 比快照 1 早 1 毫秒 | `There is currently no snapshot earlier than or equal to timestamp [...]` |

查询用的时间戳由 `-- @set` 从快照文件读出，结果每次可复现。规则：取“提交时间 ≤ 指定时刻”的最新快照（`SnapshotManager.earlierOrEqualTimeMills`）。
