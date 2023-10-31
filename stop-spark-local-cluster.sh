#!/bin/bash

SPARK_HOME="/opt/homebrew/Cellar/apache-spark/3.5.0/libexec"

${SPARK_HOME}/sbin/stop-worker.sh *
echo "Workers stopped"

${SPARK_HOME}/sbin/stop-master.sh
echo "Spark master stopped"
