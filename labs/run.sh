#!/usr/bin/env bash
# 用法：./run.sh <sql 文件>
# 编译实验工程，然后运行 SqlRunner 逐条执行 SQL 文件。
#
# 可选环境变量：
#   PAIMON_VERSION  Paimon 版本，默认 2.0.0（Maven Central 正式版）。
#                   要对照源码调试时，设为你本地编译安装的版本，例如 2.2-SNAPSHOT。
#   WAREHOUSE       Paimon warehouse 目录，默认 labs/warehouse
#   JAVA_HOME       JDK 路径（JDK 11 或 17）；未设置时 macOS 上自动查找，其它系统使用 PATH 中的 java
#   MAVEN_MIRROR    设为 aliyun 时使用阿里云 Maven 镜像（国内网络推荐）
#   SKIP_BUILD=1    跳过 mvn（并发启动多个作业时由调用方先统一编译一次）
#   JAVA_PROPS      额外的 JVM 参数（stream.sh / debug.sh 用它传参）
#   EXCLUDE_JARS    从 classpath 去掉文件名匹配该正则的 jar（实验 5 复现缺依赖报错）
#   EXTRA_JARS      额外追加到 classpath 的 jar，冒号分隔（实验 5 用）
set -euo pipefail
cd "$(dirname "$0")"

PAIMON_VERSION=${PAIMON_VERSION:-2.0.0}
WAREHOUSE=${WAREHOUSE:-warehouse}

# ---------- JDK ----------
if [ -z "${JAVA_HOME:-}" ] && [ -x /usr/libexec/java_home ]; then
  JAVA_HOME=$(/usr/libexec/java_home -v 17 2>/dev/null || /usr/libexec/java_home -v 11 2>/dev/null || true)
fi
if [ -n "${JAVA_HOME:-}" ]; then
  export JAVA_HOME
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA=java
fi
JAVA_MAJOR=$("$JAVA" -version 2>&1 | awk -F'"' '/version/ {split($2, v, "."); print (v[1] == "1") ? v[2] : v[1]}')
if [ -z "$JAVA_MAJOR" ] || [ "$JAVA_MAJOR" -lt 11 ]; then
  echo "需要 JDK 11 或 17，当前：$("$JAVA" -version 2>&1 | head -1)。请设置 JAVA_HOME。" >&2
  exit 1
fi

# ---------- Maven ----------
MVN_ARGS=(-q "-Dpaimon.version=$PAIMON_VERSION")
if [ "${MAVEN_MIRROR:-}" = "aliyun" ]; then
  MVN_ARGS+=(-s maven-settings-aliyun.xml)
fi
CLASSPATH_FILE="target/classpath-$PAIMON_VERSION.txt"

if [ "${SKIP_BUILD:-0}" != "1" ]; then
  if [ ! -f "$CLASSPATH_FILE" ] || [ pom.xml -nt "$CLASSPATH_FILE" ]; then
    # 先离线解析（依赖都已在本地仓库时最快），失败再在线下载
    mvn -o "${MVN_ARGS[@]}" dependency:build-classpath -Dmdep.outputFile="$CLASSPATH_FILE" 2>/dev/null \
      || mvn "${MVN_ARGS[@]}" dependency:build-classpath -Dmdep.outputFile="$CLASSPATH_FILE"
  fi
  mvn -o "${MVN_ARGS[@]}" compile 2>/dev/null || mvn "${MVN_ARGS[@]}" compile
fi

# ---------- 运行 ----------
# Flink 在 JDK 17 上需要的模块开放参数（JDK 11 也兼容）
JDK_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED \
--add-exports=java.base/sun.net.util=ALL-UNNAMED --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED"

CLASSPATH_VALUE=$(cat "$CLASSPATH_FILE")
if [ -n "${EXCLUDE_JARS:-}" ]; then
  # 实验 5 用：从 classpath 中去掉文件名匹配该正则的 jar，复现“缺依赖”时的报错
  CLASSPATH_VALUE=$(tr ':' '\n' <<< "$CLASSPATH_VALUE" | grep -vE "/[^/]*(${EXCLUDE_JARS})[^/]*\.jar$" | paste -sd: -)
  echo "[run.sh] 已从 classpath 排除：${EXCLUDE_JARS}"
fi
if [ -n "${EXTRA_JARS:-}" ]; then
  # 实验 5 用：额外追加 jar（冒号分隔）
  CLASSPATH_VALUE="$CLASSPATH_VALUE:$EXTRA_JARS"
  echo "[run.sh] 已追加 jar：$(tr ":" "\n" <<< "$EXTRA_JARS" | xargs -n1 basename | paste -sd, -)"
fi

exec "$JAVA" $JDK_OPTS -Dwarehouse="$WAREHOUSE" ${JAVA_PROPS:-} \
  -cp "target/classes:$CLASSPATH_VALUE" learning.paimon.SqlRunner "$@"
