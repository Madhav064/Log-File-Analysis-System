"""Loads config/pipeline.yaml. Minimal on purpose: task B3 extends this package (Spark session, logging)."""
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
ENVS = ("local", "cluster")


def load_config(env="local"):
    """Return the config with cfg["paths"] resolved for `env`; local paths become absolute."""
    if env not in ENVS:
        raise ValueError(f"env must be one of {ENVS}, got {env!r}")
    cfg = yaml.safe_load((REPO_ROOT / "config" / "pipeline.yaml").read_text())
    paths = dict(cfg["paths"][env])
    for key, value in paths.items():
        if "://" not in value:
            paths[key] = str(REPO_ROOT / value)
    cfg["paths"] = paths
    cfg["env"] = env
    return cfg
