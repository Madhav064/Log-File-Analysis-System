#!/usr/bin/env bash
# Starts the service named by the container's command, in the foreground.
set -euo pipefail

case "${1:-client}" in
  namenode)
    # Format only the first time; the name volume keeps the metadata afterwards.
    [ -f /data/hdfs/name/current/VERSION ] || hdfs namenode -format -nonInteractive
    exec hdfs namenode ;;
  datanode)        exec hdfs datanode ;;
  resourcemanager) exec yarn resourcemanager ;;
  nodemanager)     exec yarn nodemanager ;;
  historyserver)   exec mapred historyserver ;;
  spark-master)    exec spark-class org.apache.spark.deploy.master.Master --host spark-master ;;
  spark-worker)    exec spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077 ;;
  client)          exec sleep infinity ;;
  *)               exec "$@" ;;
esac
