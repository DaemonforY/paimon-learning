-- 实验 5：嵌入式运行缺依赖时的报错
-- 同一段 SQL（建表 → 写入 → 读取），在 classpath 中分别去掉一个 jar 运行，观察报错出现在哪一步、缺的是哪个类。

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

DROP TABLE IF EXISTS deps_demo;
CREATE TABLE deps_demo (
  id BIGINT,
  v  STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH ('bucket' = '1');

INSERT INTO deps_demo VALUES (1, 'a'), (2, 'b');

SELECT * FROM deps_demo;
