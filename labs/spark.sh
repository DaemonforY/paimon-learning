#!/usr/bin/env bash
# 用法：./spark.sh <sql 文件>
# 编译 spark/ 子工程，然后用 SparkSqlRunner 在本地（local[2]）逐条执行 Spark SQL。
#
# 可选环境变量：
#   PAIMON_VERSION  Paimon 版本，默认 2.0.0（paimon-spark-3.5_2.12，Maven Central 正式版）
#   WAREHOUSE       Paimon warehouse 目录，默认 labs/warehouse-spark
#   JAVA_HOME       JDK 17 或 11；未设置时 macOS 上自动查找
#   MAVEN_MIRROR    设为 aliyun 时使用阿里云 Maven 镜像
#   JAVA_PROPS      额外的 JVM 参数（调试用）
set -euo pipefail
cd "$(dirname "$0")"

PAIMON_VERSION=${PAIMON_VERSION:-2.0.0}
WAREHOUSE=${WAREHOUSE:-warehouse-spark}

if [ -z "${JAVA_HOME:-}" ] && [ -x /usr/libexec/java_home ]; then
  JAVA_HOME=$(/usr/libexec/java_home -v 17 2>/dev/null || /usr/libexec/java_home -v 11 2>/dev/null || true)
fi
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"

MVN_ARGS=(-q "-Dpaimon.version=$PAIMON_VERSION")
[ "${MAVEN_MIRROR:-}" = "aliyun" ] && MVN_ARGS+=(-s ../maven-settings-aliyun.xml)
CLASSPATH_FILE="target/classpath-$PAIMON_VERSION.txt"
(
  cd spark
  if [ ! -f "$CLASSPATH_FILE" ] || [ pom.xml -nt "$CLASSPATH_FILE" ]; then
    mvn -o "${MVN_ARGS[@]}" dependency:build-classpath -Dmdep.outputFile="$CLASSPATH_FILE" > /dev/null 2>&1 \
      || mvn "${MVN_ARGS[@]}" dependency:build-classpath -Dmdep.outputFile="$CLASSPATH_FILE"
  fi
  mvn -o "${MVN_ARGS[@]}" compile > /dev/null 2>&1 || mvn "${MVN_ARGS[@]}" compile
)

# Spark 在 JDK 17 上需要的模块开放参数（与 Spark 的 JavaModuleOptions 一致）
JDK_OPTS="-XX:+IgnoreUnrecognizedVMOptions -Djdk.reflect.useDirectMethodHandle=false"
for p in java.lang java.lang.invoke java.lang.reflect java.io java.net java.nio java.util java.util.concurrent \
         java.util.concurrent.atomic jdk.internal.ref sun.nio.ch sun.nio.cs sun.security.action sun.util.calendar; do
  JDK_OPTS="$JDK_OPTS --add-opens=java.base/$p=ALL-UNNAMED"
done

exec "$JAVA" $JDK_OPTS -Dwarehouse="$WAREHOUSE" ${JAVA_PROPS:-} \
  -cp "spark/target/classes:$(cat spark/$CLASSPATH_FILE)" learning.paimon.spark.SparkSqlRunner "$@"
