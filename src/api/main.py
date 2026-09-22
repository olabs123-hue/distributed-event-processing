import os
import psycopg
from fastapi import FastAPI

app = FastAPI(title="Distributed Event Processing API")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://event_user:event_password@localhost:5432/events",
)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/stats")
def stats():
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), COALESCE(MIN(created_at_ms),0), COALESCE(MAX(processed_at_ms),0) FROM processed_events")
            count, first_created, last_processed = cur.fetchone()
    return {
        "processed_events": count,
        "first_event_created_at_ms": first_created,
        "last_event_processed_at_ms": last_processed,
    }
