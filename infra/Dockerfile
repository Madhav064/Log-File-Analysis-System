# One image for every service (namenode, datanode, resourcemanager, nodemanager, spark-master,
# spark-worker, client). The base image is published for amd64 and arm64, and the Hadoop and
# Spark tarballs are plain Java, so the same file builds natively on Apple Silicon and Windows.
FROM ubuntu:22.04

ARG HADOOP_VERSION=3.4.1
ARG SPARK_VERSION=3.5.3
ARG APACHE_ARCHIVE=https://archive.apache.org/dist

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Hadoop (-lean: without the AWS bundle, which we do not use). Checked against the published SHA-512.
RUN set -eux; cd /tmp; \
    f=hadoop-${HADOOP_VERSION}-lean.tar.gz; u=${APACHE_ARCHIVE}/hadoop/common/hadoop-${HADOOP_VERSION}; \
    curl -fsSL --retry 5 --retry-delay 5 -O $u/$f; curl -fsSL -O $u/$f.sha512; \
    sha512sum -c $f.sha512; \
    tar -xzf $f -C /opt; mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop; rm -f $f $f.sha512

# Spark (pre-built for Hadoop 3). Its bundled Hadoop client works against our HDFS.
RUN set -eux; cd /tmp; \
    f=spark-${SPARK_VERSION}-bin-hadoop3.tgz; u=${APACHE_ARCHIVE}/spark/spark-${SPARK_VERSION}; \
    curl -fsSL --retry 5 --retry-delay 5 -O $u/$f; curl -fsSL -O $u/$f.sha512; \
    sha512sum -c $f.sha512; \
    tar -xzf $f -C /opt; mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark; rm -f $f $f.sha512

# Java 11, not 17: Hadoop 3.4.1 supports it natively, whereas YARN does not start on Java 17 without a long list
# of --add-opens flags (for the daemons and for every task JVM). Spark 3.5 runs on both. This layer comes after
# the big downloads so changing the Java or Python packages does not re-download Hadoop and Spark.
# tzdata: Java has its own time-zone database, but Python needs this one to honour TZ=America/New_York;
# without it Python silently stays on UTC and timestamps collected from Spark are 4 hours off.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-11-jre-headless python3 python3-yaml python3-pytest netcat-openbsd procps tzdata \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java \
 && ln -s /usr/bin/python3 /usr/local/bin/python

ENV JAVA_HOME=/opt/java \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PYTHONPATH=/opt/project/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PATH=/opt/hadoop/bin:/opt/spark/bin:/opt/java/bin:$PATH \
    DFS_REPLICATION=1 \
    HDFS_NAMENODE_USER=root HDFS_DATANODE_USER=root HDFS_SECONDARYNAMENODE_USER=root \
    YARN_RESOURCEMANAGER_USER=root YARN_NODEMANAGER_USER=root MAPRED_HISTORYSERVER_USER=root

COPY hadoop/*.xml /opt/hadoop/etc/hadoop/
COPY spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh && mkdir -p /data/hdfs/name /data/hdfs/data

WORKDIR /opt/project
ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["client"]
