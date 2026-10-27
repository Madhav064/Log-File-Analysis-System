# Pinned versions

| Component | Version | Where it is pinned |
|---|---|---|
| Hadoop | 3.4.1 (`-lean` tarball) | `infra/Dockerfile` (`HADOOP_VERSION`), checked against the published SHA-512 at build time |
| Spark | 3.5.3, pre-built for Hadoop 3 (bundles Hadoop client 3.3.4, which works against HDFS 3.4.1) | `infra/Dockerfile` (`SPARK_VERSION`), checked against the published SHA-512 |
| PySpark (local runs without Docker) | 3.5.3 | `requirements/jobs.txt`; **must equal the Spark version above** |
| Java (cluster image) | 11 (`openjdk-11-jre-headless` from Ubuntu 22.04) | `infra/Dockerfile`. Not 17: Hadoop 3.4.1's YARN fails to start on Java 17 (`InaccessibleObjectException: module java.base does not "opens java.lang"`) unless every daemon and task JVM gets `--add-opens` flags |
| Java (local, no Docker) | 11 or 17 (Spark 3.5 supports both) | install it yourself for `--env local`, or use the container instead |
| Python | 3.10+ (3.10 in the image) | `infra/Dockerfile` (apt `python3`) |
| Base image | `ubuntu:22.04`, published for amd64 and arm64 | `infra/Dockerfile` |

## One image on every laptop

`apache/hadoop` is amd64-only, so on an Apple Silicon Mac it would run under emulation. `infra/Dockerfile`
instead starts from `ubuntu:22.04` (amd64 and arm64) and installs the official Hadoop and Spark tarballs.
Their jars are architecture-independent, so Docker builds the same file natively on a Mac and on Windows
(WSL 2 backend). Hadoop prints a harmless "unable to load native-hadoop library" warning on arm64.

To change a version, edit the `ARG` in `infra/Dockerfile` and `requirements/jobs.txt` together,
then rebuild: `docker compose -f infra/docker-compose.yml --profile lite build`.
