#!/bin/bash

SPARK_HOME="/opt/homebrew/Cellar/apache-spark/3.5.0/libexec"
SPARK_MASTER_URL="localhost"
SPARK_MASTER_PORT=7077

${SPARK_HOME}/sbin/start-master.sh
echo "Started spark master"

${SPARK_HOME}/sbin/start-worker.sh spark://${SPARK_MASTER_URL}:${SPARK_MASTER_PORT}

echo "Started worker"
