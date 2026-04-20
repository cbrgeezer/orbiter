from fastapi.testclient import TestClient

from orbiter import DAG
from orbiter.api.server import build_app


def test_schedule_can_be_created_before_any_run(tmp_path):
    dag = DAG("api-schedule")

    @dag.task()
    async def root():
        return "ok"

    db_path = tmp_path / "orbiter.db"
    app = build_app(
        dag,
        db_path=str(db_path),
        enable_workers=False,
        enable_scheduler_service=False,
        auto_drive_submissions=False,
    )

    with TestClient(app) as client:
        response = client.post(
            "/schedules",
            json={
                "name": "hourly-import",
                "interval_seconds": 3600,
                "overlap_policy": "forbid",
                "params": {"rows": 10},
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert "schedule_id" in payload
