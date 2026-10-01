# 07 Lookup Changelog：-U/+U 是怎么产生的

源码（`mergetree/` 下）：`compact/ForceUpLevel0Compaction`、`compact/LookupMergeTreeCompactRewriter`、`compact/ChangelogMergeTreeRewriter`、`compact/LookupChangelogMergeFunctionWrapper`、`compact/LookupMergeFunction`、`LookupLevels`、`LookupUtils`

## 学习目标
- 理解“L0 上推”为什么是产生 changelog 的时机。
- 掌握哪些合并产 changelog、升级时的三种策略。
- 能对任意输入推演出 before/after 和产生的 changelog。

## 一句话结论
**每次 L0 数据被合并到更高层时，对每个 key 去更高层 lookup 出旧值（before），与新数据合并得到新值（after），比较二者产生 changelog。**

## 1. 为什么选“L0 → 高层”这个时机
- L0 = 上次合并以来**新写入**的数据；L1+ = 已合并过的数据。
- **不变式**：lookup 模式下，数据离开 L0 时一定已和 lookup 到的旧值合并过，所以**每个 key 在 L1+ 的记录永远是完整最新状态**，而不是增量。
- 因此：before = 该 key 在高层的最新状态，after = before 合并 L0 增量。

保证在同一个 checkpoint 内完成：
- `ForceUpLevel0Compaction`：Universal 没选中也 `forcePickL0()`。默认 `lookup-compact = RADICAL` 每次都强制；`GENTLE` 每 `lookup-compact.max-interval` 次才强制。
- `lookup-wait`（默认 true）：`MergeTreeCompactManager.compactNotCompleted()` 中 `needLookup && L0 非空` 视为未完成，`prepareCommit` 会等它。所以 checkpoint 提交时数据文件与 changelog 文件一起提交。

## 2. 哪些合并产 changelog

`MergeTreeCompactTask` 把文件分成“重叠需重写”和“不重叠可升级”两类：

**① 重写路径** `ChangelogMergeTreeRewriter.rewrite()`
```java
if (rewriteChangelog(...)) → rewriteOrProduceChangelog(...)   // 产 changelog
else                       → rewriteCompaction(...)           // 普通合并
```
`rewriteLookupChangelog` 条件：**outputLevel != 0 且参与合并的文件中有 L0 文件**。高层之间的合并不产 changelog。

**② 升级路径** `upgrade()` → `upgradeStrategy()`：

| 情况 | 策略 | 原因 |
|---|---|---|
| 文件不是 L0 | `NO_CHANGELOG_NO_REWRITE` | 不是新数据 |
| 目标层文件格式不同 / 向量索引 / DV 模式且有删除行 | `CHANGELOG_WITH_REWRITE` | 必须物理重写 |
| 输出到 maxLevel | `CHANGELOG_NO_REWRITE` | 没有更老数据，文件内容即完整状态 |
| `deduplicate` 且无 sequence field | `CHANGELOG_NO_REWRITE` | 最新值即最终结果 |
| 其它引擎（partial-update / aggregation 等） | `CHANGELOG_WITH_REWRITE` | L0 文件只有增量，必须把合并结果写出去以维持不变式 |

“NO_REWRITE” = **读一遍产出 changelog，但数据文件不重写**，只改 level 元数据。

## 3. 每个 key 的计算：`LookupChangelogMergeFunctionWrapper.getResult()`

多路归并后同 key 的所有记录通过 `add()` 进入 `LookupMergeFunction` 缓冲（`candidates`，可溢写），带着 level：

```java
// 1. 从参与合并的记录中挑 before
KeyValue highLevel = mergeFunction.pickHighLevel();   // level > 0 中 level 最小的
boolean containLevel0 = mergeFunction.containLevel0();

// 2. 本次没有高层记录 → lookup
if (highLevel == null) {
    lookupResult = lookup.apply(key);                  // = lookupLevels.lookup(key, outputLevel + 1)
    ...
    mergeFunction.insertInto(highLevel, comparator);   // 按 sequence 插入
}

// 3. 计算 after
KeyValue result = mergeFunction.getResult();

// 4. 有 L0 数据参与才产 changelog
if (containLevel0 && lookupStrategy.produceChangelog) setChangelog(highLevel, result);
```

细节：
- **level 最小的高层记录就是 before**：level 越小越新；结合不变式，它就是完整最新状态。
- **lookup 从 `outputLevel + 1` 开始**：`createUnit` 的输出层是“下一个更老 run 的 level - 1”，≤ outputLevel 的层要么参与本次合并、要么为空。
- **`LookupMergeFunction.getResult()` 只合并 level ≤ 0 的记录 + 那一条高层记录**，更高层的更老记录已经“包含”在它里面，直接忽略。
- **lookup 实现**（`LookupLevels` / `LookupUtils`）：逐层查找，第一个命中即返回；每层按 `minKey/maxKey` **二分**定位文件；为数据文件创建本地 **lookup file**（SST 类 KV 文件）缓存在 `lookupFileCache`，有远程预建 SST 则下载，否则从数据文件构建。首次慢，之后是本地点查——这是 lookup 模式主要开销。

## 4. 产生 -U/+U：`setChangelog(before, after)`

```java
if (before == null || !before.isAdd()) {
    if (after.isAdd())  → +I(after)
} else {
    if (!after.isAdd()) → -D(before)
    else if (valueEqualiser == null || !equals(before, after))
                        → -U(before), +U(after)
}
```

| before | after | changelog |
|---|---|---|
| 无 / DELETE | INSERT | `+I` |
| 有 | DELETE | `-D` |
| 有 | 有，值不同 | `-U` + `+U` |
| 有 | 有，值相同 | 不输出（**仅当** `changelog-producer.row-deduplicate=true`，否则仍输出 -U/+U） |

输出（`rewriteOrProduceChangelog`）：
- `result` → outputLevel 新数据文件（`dropDelete` 时丢弃 DELETE）；
- `changelogs` → 单独的 **changelog 文件**，`changelog-producer.ignore-update-before` / `ignore-delete` 在此过滤；
- 返回 `CompactResult(before, after, changelogFiles)` → 提交写入快照的 `changelogManifestList` → 流读 `ChangelogFollowUpScanner` 消费。

## 5. 例子

**例 1：deduplicate**
```
已有：L5: (id=1, name='a', v=10)
新写：L0: (id=1, name='a', v=20)
forcePickL0 → runs=[L0, L5]，outputLevel = L4
key=1：candidates 只有 L0 → lookup(key, 5) → before=(1,'a',10)
       after = (1,'a',20)
changelog: -U(1,'a',10), +U(1,'a',20)
升级策略：dedup 无 sequence field → CHANGELOG_NO_REWRITE（L0 文件只改 level）
```

**例 2：aggregation (sum)**
```
已有：L5: (id=1, total=10)
新写：L0: (id=1, total=+5)       ← 增量
before=(1,10)，after=sum → (1,15)
changelog: -U(1,10), +U(1,15)
升级策略：CHANGELOG_WITH_REWRITE —— 只改 level 的话 L4 存的是增量 5，破坏不变式，必须写出 (1,15)
```

## 6. 相关变体
- **first-row**：`FirstRowMergeFunctionWrapper`，lookup 只返回“是否存在”，不存在才 `+I`，存在直接丢弃，不产 -U/+U。
- **删除向量**：同样走 lookup，但返回旧记录的**文件名 + 行号**，调用 `dvMaintainer.notifyNewDeletion` 标记删除（见 06 章）。

## 7. 与其它 changelog-producer 对比

| `changelog-producer` | 原理 | 代价 |
|---|---|---|
| `none` | 不产 changelog，下游自己去重（Flink 加 ChangelogNormalize） | 最低 |
| `input` | 输入流直接当 changelog，要求源是完整 CDC | 低 |
| `lookup` | 本章 | 中高（本地 lookup 文件 + 每 checkpoint 强制合并） |
| `full-compaction` | 只在全量合并时对比（`FullChangelogMergeFunctionWrapper`） | 较低，延迟取决于全量合并频率 |

## 动手实验
- [ ] 跑 `LookupChangelogMergeFunctionWrapperTest`、`LookupLevelsTest`、`ChangelogMergeTreeRewriterTest`。
- [ ] 断点：`getResult`（看 before/after）、`upgradeStrategy`（看策略）、`LookupLevels.createLookupFile`（看何时建 lookup 文件）。
- [ ] 建 `changelog-producer=lookup` 表，流读打印 RowKind，构造 +I、-U/+U、-D，再打开 `row-deduplicate` 对比。

## 自测题
1. 为什么产 changelog 的时机是 “L0 上推” 而不是写入时？
2. lookup 为什么从 `outputLevel + 1` 开始？
3. 为什么 aggregation 引擎升级时必须重写文件，而 dedup 不需要？
4. `row-deduplicate` 不开时，值没变的更新会产生什么？
