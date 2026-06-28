"""PG adapter — dual-write phase state + events to PostgreSQL."""

import os
import json
from typing import Optional

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

PG_DSN = os.environ.get("PG_DSN", "postgresql://temporal:temporal@localhost:5432/temporal")

_pool: Optional[ThreadedConnectionPool] = None


def get_pool():
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(1, 10, PG_DSN)
    return _pool


class PGPhaseState:
    def upsert(self, project_id: str, current_phase: int, completed_phases: list[int], state: dict, title: str = ""):
        pool = get_pool()
        conn = pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO phase_state (project_id, current_phase, completed_phases, state_json, title, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (project_id) DO UPDATE SET
                        current_phase = EXCLUDED.current_phase,
                        completed_phases = EXCLUDED.completed_phases,
                        state_json = EXCLUDED.state_json,
                        title = EXCLUDED.title,
                        updated_at = NOW()
                """, (project_id, current_phase, completed_phases, json.dumps(state), title))
                conn.commit()
        finally:
            pool.putconn(conn)

    def get(self, project_id: str) -> Optional[dict]:
        pool = get_pool()
        conn = pool.getconn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM phase_state WHERE project_id = %s", (project_id,))
                row = cur.fetchone()
                if row:
                    return dict(row)
                return None
        finally:
            pool.putconn(conn)


class PGEventEmitter:
    def emit(self, project_id: str, event_type: str, payload: dict):
        pool = get_pool()
        conn = pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO workflow_events (project_id, event_type, payload)
                    VALUES (%s, %s, %s)
                """, (project_id, event_type, json.dumps(payload)))
                conn.commit()
        finally:
            pool.putconn(conn)
