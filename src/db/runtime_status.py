from __future__ import annotations

import time
from typing import Any

from sqlalchemy.orm import Session

from src.db.models import RuntimeStatus, Status


DEFAULT_STATUS = {
    "id": 1,
    "bot_enabled": True,
    "bot_running": False,
    "ws_connected": False,
    "dashboard_connected": False,
    "last_heartbeat": None,
}

RUNTIME_DEFAULT_STATUS = {
    key: value for key, value in DEFAULT_STATUS.items() if key != "dashboard_connected"
}


def ensure_runtime_status_row(session: Session) -> RuntimeStatus:
    status = session.query(RuntimeStatus).filter_by(id=1).first()
    if not status:
        status = RuntimeStatus(**RUNTIME_DEFAULT_STATUS)
        session.add(status)
        session.commit()
        session.refresh(status)
    return status


def ensure_status_row(session: Session) -> Status:
    status = session.query(Status).filter_by(id=1).first()
    if not status:
        status = Status(**DEFAULT_STATUS)
        session.add(status)
        session.commit()
        session.refresh(status)
    return status


def get_runtime_status(session: Session) -> Status:
    ensure_runtime_status_row(session)
    return ensure_status_row(session)


def update_runtime_status(session: Session, **fields: Any) -> Status:
    legacy_status = ensure_runtime_status_row(session)
    status = ensure_status_row(session)

    for key, value in fields.items():
        if hasattr(legacy_status, key):
            setattr(legacy_status, key, value)
        if hasattr(status, key):
            setattr(status, key, value)

    if "last_heartbeat" not in fields:
        now = status.last_heartbeat or time.time()
        status.last_heartbeat = now
        legacy_status.last_heartbeat = now

    session.commit()
    session.refresh(legacy_status)
    session.refresh(status)
    return status
