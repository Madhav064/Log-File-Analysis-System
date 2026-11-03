"""Logging for jobs and scripts: one format, to stderr, configured once."""
import logging

FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"


def get_logger(name):
    """A logger under the `loganalysis` namespace. The first call configures the handler."""
    root = logging.getLogger("loganalysis")
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(FORMAT, datefmt="%H:%M:%S"))
        root.addHandler(handler)
        root.setLevel(logging.INFO)
        root.propagate = False
        logging.getLogger("py4j").setLevel(logging.WARNING)
    return logging.getLogger(f"loganalysis.{name}")
