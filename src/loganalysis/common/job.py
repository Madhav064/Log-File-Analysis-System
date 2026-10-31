"""The command-line wrapper shared by every Spark job (plan, architectural rule 6).

A job module only defines `run(spark, cfg)` and ends with:

    if __name__ == "__main__":
        run_job("errors", run, __doc__)

so it is runnable as `python -m loganalysis.<package>.<module> --env local|cluster`.
"""
import argparse
import time

from .config import ENVS, load_config
from .log import get_logger
from .spark import get_spark


def run_job(name, run, doc=None, argv=None):
    ap = argparse.ArgumentParser(description=(doc or name).strip().splitlines()[0])
    ap.add_argument("--env", default="local", choices=ENVS,
                    help="local: the bundled sample on the local filesystem; cluster: HDFS (client container)")
    args = ap.parse_args(argv)

    log = get_logger(name)
    cfg = load_config(args.env)
    started = time.time()
    log.info("start (env=%s)", args.env)
    spark = get_spark(name, cfg)
    try:
        run(spark, cfg)
    finally:
        spark.stop()
    log.info("done in %.1fs", time.time() - started)
