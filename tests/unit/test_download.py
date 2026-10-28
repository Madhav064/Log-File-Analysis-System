"""The NASA host is slow and a transfer already timed out once, so resume and verify matter."""
import hashlib
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from loganalysis.ingestion.download import decompress, download

PAYLOAD = bytes(range(256)) * 4000  # ~1 MB


class RangeHandler(BaseHTTPRequestHandler):
    """Serves PAYLOAD with Range support. The first request is cut off halfway, like a timed-out transfer."""
    requests = []

    def do_GET(self):
        start = int(self.headers["Range"].split("=")[1].rstrip("-")) if "Range" in self.headers else 0
        RangeHandler.requests.append(start)
        body = PAYLOAD[start:]
        self.send_response(206 if start else 200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body[: len(body) // 2] if len(RangeHandler.requests) == 1 else body)

    def log_message(self, *args):
        pass


@pytest.fixture
def server():
    RangeHandler.requests = []
    httpd = HTTPServer(("127.0.0.1", 0), RangeHandler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{httpd.server_port}/file.gz"
    httpd.shutdown()


def test_resumes_after_a_cut_off_transfer(server, tmp_path, monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    sha = hashlib.sha256(PAYLOAD).hexdigest()
    dest = download(server, tmp_path / "file.gz", sha)
    assert dest.read_bytes() == PAYLOAD
    assert len(RangeHandler.requests) == 2 and RangeHandler.requests[1] > 0  # second request resumed mid-file


def test_already_downloaded_file_is_not_fetched_again(server, tmp_path):
    (tmp_path / "file.gz").write_bytes(PAYLOAD)
    download(server, tmp_path / "file.gz", hashlib.sha256(PAYLOAD).hexdigest())
    assert RangeHandler.requests == []


def test_wrong_checksum_is_rejected_and_cleaned_up(server, tmp_path, monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        download(server, tmp_path / "file.gz", "0" * 64)
    assert not (tmp_path / "file.gz").exists() and not (tmp_path / "file.gz.part").exists()


def test_decompress_is_idempotent(tmp_path):
    import gzip
    gz = tmp_path / "a.gz"
    gz.write_bytes(gzip.compress(b"line1\nline2\n"))
    out = decompress(gz, tmp_path / "a")
    assert out.read_bytes() == b"line1\nline2\n"
    mtime = out.stat().st_mtime_ns
    assert decompress(gz, tmp_path / "a").stat().st_mtime_ns == mtime
