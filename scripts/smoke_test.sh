#!/usr/bin/env bash
# Smoke test for the cluster: HDFS, Spark, MapReduce on YARN, and a Python Hadoop Streaming mapper.
# Run it inside the client container (the same command works on macOS, Windows and Linux):
#   docker compose -f infra/docker-compose.yml exec client bash scripts/smoke_test.sh
set -euo pipefail
step() { printf '\n== %s\n' "$*"; }
tmp=$(mktemp -d)

step "HDFS: write and read back"
printf 'hello hadoop\nhello spark\n' > "$tmp/in.txt"
hdfs dfs -mkdir -p /smoke
hdfs dfs -put -f "$tmp/in.txt" /smoke/in.txt
[ "$(hdfs dfs -cat /smoke/in.txt | wc -l)" -eq 2 ]
hdfs dfs -ls /smoke

step "Spark: SparkPi on the standalone cluster"
spark-submit --class org.apache.spark.examples.SparkPi "$SPARK_HOME"/examples/jars/spark-examples_*.jar 20 2>&1 | grep "Pi is roughly"

step "MapReduce on YARN: the bundled wordcount example"
hdfs dfs -rm -r -f -skipTrash /smoke/wc > /dev/null
hadoop jar "$HADOOP_HOME"/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount /smoke/in.txt /smoke/wc > "$tmp/wc.log" 2>&1 || { cat "$tmp/wc.log"; exit 1; }
hdfs dfs -cat /smoke/wc/part-* | tee "$tmp/wc.out"
grep -q $'^hello\t2$' "$tmp/wc.out"

step "Hadoop Streaming with a Python mapper (python3 must exist on the NodeManager)"
printf 'import sys\nfor line in sys.stdin:\n    print(line.split()[0] + "\\t1")\n' > "$tmp/mapper.py"
hdfs dfs -rm -r -f -skipTrash /smoke/streaming > /dev/null
hadoop jar "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.reduces=1 -files "$tmp/mapper.py" \
  -mapper "python3 mapper.py" -reducer cat -input /smoke/in.txt -output /smoke/streaming > "$tmp/st.log" 2>&1 || { cat "$tmp/st.log"; exit 1; }
hdfs dfs -cat /smoke/streaming/part-* | tee "$tmp/st.out"
[ "$(wc -l < "$tmp/st.out")" -eq 2 ]

printf '\nSMOKE TEST PASSED\n'
