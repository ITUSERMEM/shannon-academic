"""Background migration: scan Redis completed phases → write PG."""

import os
import time
import json
from redis import Redis
from pg_adapter import PGPhaseState, PGEventEmitter

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
BATCH_SIZE = 100
INTERVAL_SEC = 30


def migrate():
    r = Redis.from_url(REDIS_URL, decode_responses=True)
    pg_state = PGPhaseState()
    pg_events = PGEventEmitter()

    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match="academic:phase:state:*", count=BATCH_SIZE)
        for key in keys:
            project_id = key.split(":")[-1]
            try:
                state = r.json().get(key)
                if state is None:
                    continue
                completed = state.get("completed_phases", [])
                if not completed:
                    continue
                pg_state.upsert(
                    project_id=project_id,
                    current_phase=state.get("current_phase", 0),
                    completed_phases=completed,
                    state=state,
                    title=state.get("title", ""),
                )
                r.expire(key, 86400)
            except Exception as e:
                print(f"Error migrating {project_id}: {e}")

        if cursor == 0:
            break

    print(f"Migration scan complete. Sleep {INTERVAL_SEC}s...")
    time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    print("Background migration started")
    while True:
        migrate()
