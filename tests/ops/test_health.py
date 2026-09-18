from unittest.mock import Mock

from fastapi.testclient import TestClient

from procurement.api import main


def test_liveness_does_not_require_storage(monkeypatch):
    monkeypatch.setattr(main, "create_s3_filesystem", Mock(side_effect=RuntimeError("offline")))
    assert TestClient(main.app).get("/health/live").status_code == 200
    response = TestClient(main.app).get("/health/ready")
    assert response.status_code == 503
    assert "offline" not in response.text


def test_readiness_requires_existing_bucket(monkeypatch):
    fs = Mock()
    monkeypatch.setattr(main, "create_s3_filesystem", lambda: fs)
    fs.exists.return_value = False
    assert TestClient(main.app).get("/health/ready").status_code == 503
    fs.exists.return_value = True
    assert TestClient(main.app).get("/health/ready").status_code == 200
