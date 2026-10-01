# 03 LSM 合并策略：UniversalCompaction 怎么选文件

源码：`mergetree/compact/UniversalCompaction.java`（约 190 行，值得逐行精读）

## 学习目标
- 知道 `runs` 的排列顺序与含义。
- 掌握 `pick()` 的 4 级判断，能手算任意输入的选择结果和 outputLevel。
- 理解 `ForceUpLevel0Compaction`、`EarlyFullCompaction`、`OffPeakHours` 的作用。

## 一句话结论
它不是挑“文件”，而是**从最新的 run 开始挑一段连续前缀**，合并成一个 run，并放到“下一个更老 run 所在 level 的上一层”，始终保持“level 越低数据越新”。

## 1. 输入：runs 的顺序

`MergeTreeCompactManager.triggerCompaction()` 用 `Levels.levelSortedRuns()` 构造输入：

```java
level0.forEach(file -> runs.add(new LevelSortedRun(0, SortedRun.fromSingle(file))));  // L0 每个文件一个 run
for (i ...) if (levels.get(i).nonEmpty()) runs.add(new LevelSortedRun(i + 1, run));   // L1..Lmax 每层一个 run
```

- L0 文件互相可能重叠，**每个 L0 文件单独算一个 run**，按 `maxSequenceNumber` 降序（最新在前）。
- L1 及以上每层一个 run，空层跳过。
- 所以：**index 0 最新，最后一个最老**（通常是最高层）。

```
runs = [L0_最新, L0, L0, L1, L3, L5_最老]
```

默认参数：

| 参数 | 默认 |
|---|---|
| `num-sorted-run.compaction-trigger` | 5 |
| `num-levels` | trigger + 1 = 6（maxLevel = 5） |
| `compaction.size-ratio` | 1（%） |
| `compaction.max-size-amplification-percent` | 200 |

## 2. `pick()` 的 4 级判断

```
pick(numLevels, runs)
 ├─ 0. EarlyFullCompaction   （可选）按时间/大小触发全量合并
 ├─ 1. pickForSizeAmp        空间放大过大 → 全量合并
 ├─ 2. pickForSizeRatio      大小相近 → 合并一段前缀
 ├─ 3. 文件数兜底            runs > trigger → 强制合并
 └─ 都不满足 → Optional.empty()
```

### 第 0 级：EarlyFullCompaction（默认关闭）
满足任一则全量合并到 maxLevel：
- `compaction.optimization-interval`：距上次全量合并超过该时间；
- `compaction.total-size-threshold`：所有 run 总大小小于阈值（小表直接全合）；
- 增量大小阈值：非 maxLevel 的数据量超过阈值。

### 第 1 级：`pickForSizeAmp`（空间放大）
```java
if (runs.size() < numRunCompactionTrigger) return null;
long candidateSize   = 除最后一个 run 外所有 run 的大小之和;
long earliestRunSize = 最后一个 run（最老）的大小;
if (candidateSize * 100 > maxSizeAmp * earliestRunSize)          // 即 candidate > 2 × 最老 run
    return CompactUnit.fromLevelRuns(maxLevel, runs);            // 全量合并 → maxLevel
```
新数据超过底座 2 倍时全量合并，这也是全量合并最主要的触发方式。

### 第 2 级：`pickForSizeRatio`（核心）
```java
long candidateSize = runs[0].size;
boolean compactionTriggered = forcePick || candidateCount > 1;
for (i = candidateCount; i < runs.size(); i++) {
    next = runs[i];
    if (candidateSize * (100 + sizeRatio + offPeakRatio) / 100.0 < next.size) {   // 下一个“明显更大”
        if (!compactionTriggered || next.level() > 1) break;
    }
    candidateSize += next.size;  candidateCount++;  compactionTriggered = true;
}
return compactionTriggered ? createUnit(runs, maxLevel, candidateCount) : null;
```
- **正常吸收**：累计大小 × 1.01 ≥ 下一个 run 大小，就把它吸进来（滚雪球）。
- **停止条件**：下一个明显更大时
  - 一个都没吸收 → 不合并；
  - 已开始吸收且下一个是 **level > 1** → 停；
  - 已开始吸收但下一个是 **L0 或 L1** → **不管大小继续吸收**。原因：输出只能放到 ≥1 且低于下一个更老 run 的层，剩下 L0/L1 会导致无处可放。
- `offPeakRatio`：配置 `compaction.offpeak.start.hour` / `end.hour` / `offpeak-ratio` 后，低峰期比例放大，合并更激进。

### 第 3 级：文件数兜底
```java
if (runs.size() > numRunCompactionTrigger) {
    int candidateCount = runs.size() - numRunCompactionTrigger + 1;
    return pickForSizeRatio(maxLevel, runs, candidateCount);   // 强制带上前 candidateCount 个，再按比例扩展
}
```
合并 k 个 run 变 1 个，run 数减少 k-1，正好回到 trigger。

## 3. `createUnit`：输出到哪一层

```java
if (runCount == runs.size())  outputLevel = maxLevel;                         // 全选 → 最高层
else                          outputLevel = max(0, runs[runCount].level() - 1); // 下一个更老 run 的上一层
if (outputLevel == 0) {                                                       // 不允许输出到 L0
    继续吸收后面的 run，直到吸收到第一个非 L0 的 run，outputLevel = 该 run 的 level;
}
if (runCount == runs.size()) outputLevel = maxLevel;
```

**不变式：level 越低数据越新。** 这也是为什么只能选“最新的连续前缀”，不能跳着选。

选出后 `MergeTreeCompactManager` 还会：
- 过滤无意义任务：只有一个文件且已经在 outputLevel；
- 计算 `dropDelete`：outputLevel ≥ 当前最高非空层（或 DV 模式）时，DELETE 记录可以物理删除。

## 4. 手算例题（默认参数，单位 MB）

**例 A：大小比例 → 输出到中间层**
```
runs = [L0:10, L0:10, L0:10, L3:100, L5:100]
第1级：130 > 2×100？否
第2级：10×1.01≥10 吸收(20)；20.2≥10 吸收(30)；30.3<100 且 L3>1 → 停
outputLevel = 3 - 1 = 2
结果：3 个 L0 → 一个 L2 run(30)；runs = [L2:30, L3:100, L5:100]
```

**例 B：空间放大 → 全量合并**
```
runs = [L0:60, L0:60, L0:60, L1:50, L5:100]
第1级：230 > 200 → 全量合并到 L5
```

**例 C：文件数兜底 + 强制扫 L0**
```
runs = [L0:1, L0:5, L0:20, L2:80, L3:300, L5:1000]
第1级：406 > 2000？否
第2级：1×1.01<5，未吸收 → null
第3级：6>5，candidateCount=2，强制 [1,5]=6
      6.06<20，但下一个是 L0 → 仍吸收(26)
      26.26<80 且 L2>1 → 停
outputLevel = 2 - 1 = 1
结果：[L1:26, L2:80, L3:300, L5:1000]
```

## 5. 包装策略与相关配置

| 配置/类 | 作用 |
|---|---|
| `num-sorted-run.stop-trigger`（默认 trigger+3=8） | run 数超过时写入阻塞等合并（反压） |
| `ForceUpLevel0Compaction` | lookup 类表（lookup changelog、DV、first-row）或 `compaction.force-up-level-0=true` 时使用：Universal 没选中也调用 `forcePickL0()` 把 L0 推上去。`lookup-compact=GENTLE` 时每 `lookup-compact.max-interval` 次才强制一次 |

## 动手实验
- [ ] 跑 `UniversalCompactionTest`，把例 A/B/C 写成测试用例，先手算再验证。
- [ ] 跑 `ForceUpLevel0CompactionTest`，对比 lookup 模式的差异。
- [ ] 把 `org.apache.paimon.mergetree.compact` 日志设为 DEBUG，观察 `Universal compaction due to size amplification / size ratio / file num`。

## 自测题
1. 为什么 L0 每个文件单独算一个 run？
2. 为什么已经开始吸收后，遇到 L0/L1 要无视大小继续吸收？
3. 给定 `[L0:2, L0:2, L4:3, L5:100]`，默认参数下会选什么？输出到哪层？（提示：run 数 4 < 5）
4. `dropDelete` 在什么条件下为 true？为什么？
