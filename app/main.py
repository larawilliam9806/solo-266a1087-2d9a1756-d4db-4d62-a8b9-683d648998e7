"""FastAPI surface for the buoy data-lake compaction service.

Endpoints
---------
POST   /days/{day}/upload                 append a CSV as one parquet file
GET    /days/{day}/records                all records (consistent snapshot)
GET    /days/{day}/files                  active file list / count
POST   /days/{day}/snapshots              pin today's files under a snapshot id
GET    /days/{day}/snapshots              live snapshots for the day
GET    /snapshots/{snapshot_id}           snapshot metadata
GET    /snapshots/{snapshot_id}/records   raw rows visible at creation time
DELETE /snapshots/{snapshot_id}           release a snapshot
POST   /days/{day}/compactions            start background compaction (202)
GET    /days/{day}/compactions            list jobs for the day
GET    /days/{day}/compactions/{job_id}   job status
GET    /health                            liveness
"""

from __future__ import annotations

import logging
import os
import re
import threading
from contextlib import asynccontextmanager
from datetime import date

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.concurrency import run_in_threadpool

from .csvio import CsvError
from .lake import ConflictError, Lake, NotFoundError

log = logging.getLogger("buoy-lake")

DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _valid_day(day: str) -> str:
    if not DAY_RE.match(day):
        raise HTTPException(status_code=400, detail=f"invalid day {day!r}; expected YYYY-MM-DD")
    try:
        date.fromisoformat(day)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"invalid calendar day {day!r}")
    return day


def create_app(
    data_dir: str | None = None,
    compaction_delay_sec: float | None = None,
    gc_interval_sec: float | None = None,
) -> FastAPI:
    data_dir = data_dir or os.environ.get("DATA_DIR", "./data")
    if compaction_delay_sec is None:
        compaction_delay_sec = float(os.environ.get("LAKE_COMPACTION_DELAY_SEC", "0"))
    if gc_interval_sec is None:
        gc_interval_sec = float(os.environ.get("LAKE_GC_INTERVAL_SEC", "2"))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        lake = Lake(data_dir, compaction_delay_sec=compaction_delay_sec)
        stats = lake.recover()  # reconcile + resume interrupted compactions
        app.state.lake = lake
        app.state.recovery = stats

        stop = threading.Event()

        def gc_loop():
            while not stop.wait(gc_interval_sec):
                try:
                    lake.collect_garbage()
                except Exception:
                    log.exception("periodic garbage collection failed")

        gc_thread = threading.Thread(target=gc_loop, name="retired-file-gc", daemon=True)
        gc_thread.start()
        yield
        stop.set()
        gc_thread.join(timeout=5)

    app = FastAPI(title="Buoy Data Lake Compaction Service", version="1.0.0", lifespan=lifespan)

    def lake(request: Request) -> Lake:
        return request.app.state.lake

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/")
    def index():
        return {
            "service": "buoy-lake-compactor",
            "docs": "/docs",
            "endpoints": [
                "POST /days/{day}/upload",
                "GET /days/{day}/records",
                "GET /days/{day}/files",
                "POST /days/{day}/snapshots",
                "GET /days/{day}/snapshots",
                "GET /snapshots/{snapshot_id}",
                "GET /snapshots/{snapshot_id}/records",
                "DELETE /snapshots/{snapshot_id}",
                "POST /days/{day}/compactions",
                "GET /days/{day}/compactions",
                "GET /days/{day}/compactions/{job_id}",
            ],
        }

    @app.post("/days/{day}/upload")
    async def upload(day: str, request: Request, file: UploadFile | None = File(None)):
        """Append a CSV (multipart file or raw ``text/csv`` body) as one parquet."""
        day = _valid_day(day)
        content = await file.read() if file is not None else await request.body()
        if not content:
            raise HTTPException(status_code=400, detail="empty CSV body")
        try:
            # parquet write + fsync are blocking -> run off the event loop
            return await run_in_threadpool(lake(request).upload, day, content)
        except CsvError as exc:
            raise HTTPException(status_code=422, detail=str(exc))

    @app.get("/days/{day}/records")
    def records(day: str, request: Request):
        """All records for the day from one consistent snapshot + file count."""
        return lake(request).read_day(_valid_day(day))

    @app.get("/days/{day}/files")
    def files(day: str, request: Request):
        return lake(request).list_files(_valid_day(day))

    # -- day snapshots -------------------------------------------------
    # The canonical form is /days/{day}/snapshots for creation and
    # /snapshots/{id}... for reading/releasing.  Day-scoped read/delete
    # aliases are accepted too, since the id itself is globally unique.
    @app.post("/days/{day}/snapshots")
    def create_snapshot(day: str, request: Request):
        """Pin the files currently registered for the day; returns the id."""
        return lake(request).create_snapshot(_valid_day(day))

    @app.get("/days/{day}/snapshots")
    def list_snapshots(day: str, request: Request):
        day = _valid_day(day)
        return {"day": day, "snapshots": lake(request).list_snapshots(day)}

    def _snapshot_for_day(lk: Lake, day: str, snapshot_id: str) -> dict:
        body = lk.read_snapshot(snapshot_id)
        if body["day"] != day:
            raise HTTPException(
                status_code=404, detail=f"snapshot {snapshot_id} does not belong to day {day}"
            )
        return body

    @app.get("/snapshots/{snapshot_id}")
    def get_snapshot(snapshot_id: str, request: Request):
        """Snapshot content: metadata plus the raw rows visible at creation."""
        try:
            return lake(request).read_snapshot(snapshot_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

    @app.get("/snapshots/{snapshot_id}/records")
    def snapshot_records(snapshot_id: str, request: Request):
        """Every raw row visible when the snapshot was created (duplicates kept)."""
        try:
            return lake(request).read_snapshot(snapshot_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

    @app.get("/days/{day}/snapshots/{snapshot_id}")
    def snapshot_content_for_day(day: str, snapshot_id: str, request: Request):
        try:
            return _snapshot_for_day(lake(request), _valid_day(day), snapshot_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

    @app.get("/days/{day}/snapshots/{snapshot_id}/records")
    def snapshot_records_for_day(day: str, snapshot_id: str, request: Request):
        try:
            return _snapshot_for_day(lake(request), _valid_day(day), snapshot_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

    def _release(lk: Lake, snapshot_id: str) -> dict:
        try:
            return lk.release_snapshot(snapshot_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

    @app.delete("/snapshots/{snapshot_id}")
    def release_snapshot(snapshot_id: str, request: Request):
        """Release a snapshot; its id stops serving data (idempotent)."""
        return _release(lake(request), snapshot_id)

    @app.delete("/days/{day}/snapshots/{snapshot_id}")
    def release_snapshot_for_day(day: str, snapshot_id: str, request: Request):
        _valid_day(day)
        return _release(lake(request), snapshot_id)

    @app.post("/days/{day}/compactions", status_code=202)
    def start_compaction(day: str, request: Request):
        """Start a background compaction; 409 if one is already running."""
        try:
            return lake(request).start_compaction(_valid_day(day))
        except ConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc))

    @app.get("/days/{day}/compactions")
    def list_compactions(day: str, request: Request):
        day = _valid_day(day)
        return {"day": day, "jobs": lake(request).list_compactions(day)}

    @app.get("/days/{day}/compactions/{job_id}")
    def get_compaction(day: str, job_id: int, request: Request):
        day = _valid_day(day)
        try:
            job = lake(request).get_compaction(job_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        if job["day"] != day:
            raise HTTPException(status_code=404, detail=f"job {job_id} does not belong to day {day}")
        return job

    return app


app = create_app()
