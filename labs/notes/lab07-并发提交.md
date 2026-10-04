# 实验 7：并发提交——抢快照号、冲突、幂等

运行（Paimon Java API，不经过 Flink）：

```bash
MAIN_CLASS=learning.paimon.CommitLab ./run.sh                       # 三个场景全跑
MAIN_CLASS=learning.paimon.CommitLab JAVA_PROPS=-Dscenario=race ./run.sh   # 只跑一个：race / conflict / idempotent
```

源码：`labs/src/main/java/learning/paimon/CommitLab.java`；对应教程 05 章、S2 第 5 讲。

| 场景 | 做法 | 结果（Paimon 2.0.0） |
|---|---|---|
| race | 两个线程（job-A、job-B）各向同一张 append 表提交 30 次 | WARN `Atomic commit failed for snapshot #N` 出现 18~25 次（每次运行不同），最终 60 个快照、各 30 个、0 失败 |
| conflict | 主键表 1 个 bucket，写 3 个 L0 文件；两个合并作业都基于快照 3 全量合并，先后提交 | 第一个成功（空 APPEND 快照 4 + COMPACT 快照 5）；第二个先产生空 APPEND 快照 6，然后 `File deletion conflicts detected!`（`Trying to delete file ... which is not previously added`） |
| idempotent | job-X 提交 identifier 5；同一 commitUser 重启后 `filterAndCommit(5)`；job-Y 也提交 identifier 5 | job-X 重复提交被过滤（0 个）；job-Y 正常提交（1 个） |

几个细节：
- 冲突信息里的 “Base commit user” 是**最新快照的提交者**（`ConflictDetection` 第 213 行），实验里是出错的 compact-job-2 自己（它刚提交了空 APPEND 快照）。
- `StreamWriteBuilder` 的提交默认 `ignoreEmptyCommit(false)`，所以每次 commit 即使没有新文件也会产生一个 APPEND 快照。
- 本地文件系统的“原子 rename”是 `LocalFileIO` 里一把 **static** 锁 + 目标存在检查 + `ATOMIC_MOVE`，只在同一 JVM 内互斥；本实验的两个“作业”是同一 JVM 的两个线程。
- conflict 场景里显式设了 `num-levels = 6`：只调大 `num-sorted-run.compaction-trigger` 会让默认 `num-levels`（= trigger + 1）一起变大。
