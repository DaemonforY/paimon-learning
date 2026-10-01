# 实验 6 记录：Schema 演进——删列再加回，旧数据为什么回不来

日期：2026-10-02　|　对应教程 02 章　|　版本：Paimon 2.0.0 + Flink 1.20.1

运行：
```bash
cd labs && ./run.sh sql/lab06/schema-evolution.sql
```

## 过程与结果

表 `schema_demo (order_id PK, status, amount)`，先写订单 1、2，再依次执行 4 次 ALTER：

| 操作 | 生成 | 字段 id 变化 | 查询结果 |
|---|---|---|---|
| 建表 | `schema-0` | order_id=0, status=1, amount=2；`highestFieldId=2` | — |
| `ADD remark STRING`，再写订单 3 | `schema-1` | 新增 remark=**3** | 订单 1、2 的 remark 为 **NULL**，订单 3 为 VIP |
| `RENAME status TO order_status` | `schema-2` | order_status 仍是 **1** | 列名变了，旧数据照常读出 |
| `DROP amount` | `schema-3` | 去掉 id=2 | — |
| `ADD amount DECIMAL(10,2)` | `schema-4` | 新 amount=**4**；`highestFieldId=4` | **3 个订单的 amount 全部为 NULL**，原来的 99.90 / 15.00 / 8.80 不会回来 |

`schema_demo$files`：

| schema_id | record_count | key 范围 |
|---|---|---|
| 0 | 2 | [1, 2] |
| 1 | 1 | [3, 3] |

每个数据文件都记录了**写入时的 schema_id**。

## 解读

1. **每次 ALTER 生成一个新的 `schema-N` 文件**，旧文件不改；数据文件也不重写（`$files` 里还是那两个文件）。
2. **列靠字段 id 对应，不靠列名**：读取时，Paimon 用数据文件的 schema_id 找到它当时的字段列表，再按 id 映射到当前 schema：
   - 当前 schema 有、旧文件没有的 id（如 remark=3）→ 补 NULL；
   - 改名不改 id（status → order_status 仍是 1）→ 旧数据照常读出；
   - 删除后再加的同名列是**新 id**（amount=4），旧文件里 id=2 的数据不再对应任何列 → 读出 NULL。
3. `highestFieldId` 只增不减，保证 id 永不复用——这正是“删掉再加回来，旧数据不会复活”的原因。

源码：`paimon-core/.../schema/SchemaEvolutionUtil.java` 的 `createIndexMapping(tableFields, dataFields)`——先把数据文件的字段建成 `fieldId → 位置` 的映射，再按当前表字段的 id 逐个查找，查不到的记为 `NULL_FIELD_INDEX`（读出 NULL）。

## 思考题
1. 如果 Paimon 按列名而不是 id 对应，“删列再加回同名列”会发生什么？这在什么场景下会造成数据错误？
2. 改列类型（如 INT → BIGINT）时，旧文件怎么读？（提示：源码 `schema/` 下的类型转换与 `CastExecutors`）
3. 写入作业运行中有人执行了 ALTER，正在写的文件用哪个 schema？
