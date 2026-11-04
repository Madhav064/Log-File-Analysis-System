"""The dashboard: a Flask REST API over the exported serving JSON, plus the pages that chart it (plan M8)."""
from flask import Flask

from .config import Config


def create_app(config=None):
    app = Flask(__name__)
    app.config.from_object(Config)
    if config:
        app.config.update(config)

    from .services.repository import Repository
    app.extensions["repository"] = Repository(app.config["DATA_DIR"])
    return app
