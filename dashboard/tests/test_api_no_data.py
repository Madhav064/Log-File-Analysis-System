"""Before any export exists, every data endpoint answers 503 with a hint, never invented numbers."""
import pytest
from app import create_app

DATA_ENDPOINTS = ["/api/summary", "/api/errors/status", "/api/errors/timeline", "/api/errors/top-resources",
                  "/api/errors/top-hosts", "/api/traffic/timeline", "/api/traffic/profile", "/api/traffic/peaks",
                  "/api/top/resources", "/api/top/hosts", "/api/top/resource-types", "/api/anomalies",
                  "/api/pipeline/quality", "/api/pipeline/benchmark", "/api/pipeline/ingestion",
                  "/api/pipeline/runs", "/api/health"]


@pytest.fixture
def client(tmp_path):
    return create_app({"DATA_DIR": str(tmp_path / "no-export-yet"), "TESTING": True}).test_client()


@pytest.mark.parametrize("url", DATA_ENDPOINTS)
def test_503_with_the_export_hint(client, url):
    r = client.get(url)
    assert r.status_code == 503
    assert "pipeline.py export" in r.get_json()["error"]


def test_unknown_api_path_is_a_json_404(client):
    r = client.get("/api/nope")
    assert r.status_code == 404 and r.get_json()["code"] == 404


def test_stage_metadata_needs_no_data(client):
    r = client.get("/api/meta/stages")
    assert r.status_code == 200 and len(r.get_json()["data"]) == 10
