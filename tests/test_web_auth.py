import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(monkeypatch, tmp_path: Path) -> TestClient:
    monkeypatch.setenv("APP_SESSION_SECRET", "test-secret")
    monkeypatch.setenv("APP_ADMIN_USER", "admin")
    monkeypatch.setenv("APP_ADMIN_PASS", "adminpass")
    monkeypatch.setenv("APP_VIEWER_USER", "viewer")
    monkeypatch.setenv("APP_VIEWER_PASS", "viewerpass")
    monkeypatch.setenv("APP_DB_PATH", str(tmp_path / "web.duckdb"))

    import src.app.auth.deps as deps

    deps.get_session_manager.cache_clear()
    from src.app.web import routes

    importlib.reload(routes)
    import src.app.main as main

    importlib.reload(main)
    return TestClient(main.app)


def test_daily_requires_login(client: TestClient):
    response = client.get("/daily")
    assert response.status_code in (401, 307, 302)


def test_viewer_can_access_daily(client: TestClient):
    login_response = client.post(
        "/login",
        data={"username": "viewer", "password": "viewerpass"},
        follow_redirects=False,
    )
    assert login_response.status_code in (303, 302)

    daily_response = client.get("/daily")
    assert daily_response.status_code == 200


def test_viewer_denied_manual_run(client: TestClient):
    client.post(
        "/login",
        data={"username": "viewer", "password": "viewerpass"},
        follow_redirects=False,
    )
    response = client.get("/run/manual")
    assert response.status_code == 403


def test_operator_can_access_manual_run(client: TestClient):
    client.post(
        "/login",
        data={"username": "admin", "password": "adminpass"},
        follow_redirects=False,
    )
    response = client.get("/run/manual")
    assert response.status_code != 403
    assert response.status_code in (200, 501)
