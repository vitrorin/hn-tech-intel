import asyncio
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from .config import settings
from .db import (
    get_companies,
    get_company_by_id,
    get_metrics,
    get_threads,
    init_pool,
    run_migrations,
)
from .models import JobStatus
from .pipeline import run_pipeline

jobs: dict[str, JobStatus] = {}
pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    try:
        pool = await init_pool(settings.database_url)
        migrations_path = Path(__file__).parent.parent / "migrations" / "001_initial.sql"
        await run_migrations(pool, str(migrations_path))
    except Exception as exc:
        import logging
        logging.getLogger("hn_tech_intel").warning("DB unavailable at startup: %s", exc)
    yield
    if pool is not None:
        await pool.close()


app = FastAPI(title="HN Tech Intel", lifespan=lifespan)


class IngestRequest(BaseModel):
    thread_id: int


@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    html_path = Path(__file__).parent.parent / "static" / "index.html"
    try:
        return HTMLResponse(html_path.read_text())
    except FileNotFoundError:
        return HTMLResponse("<html><body><h1>HN Tech Intel</h1><p>UI coming soon.</p></body></html>")


@app.post("/ingest")
async def ingest(req: IngestRequest):
    if req.thread_id <= 0:
        raise HTTPException(status_code=400, detail="Invalid thread_id")
    job_id = str(uuid.uuid4())
    job = JobStatus(job_id=job_id, thread_id=req.thread_id)
    jobs[job_id] = job
    asyncio.create_task(run_pipeline(pool, jobs, job_id, req.thread_id, settings))
    return {"job_id": job_id}


@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return jobs[job_id]


@app.get("/threads")
async def list_threads():
    return await get_threads(pool)


@app.get("/companies")
async def list_companies(thread_id: int | None = None, page: int = 1, limit: int = 20):
    return await get_companies(pool, thread_id, page, limit)


@app.get("/companies/{company_id}")
async def get_company(company_id: int):
    company = await get_company_by_id(pool, company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    return company


@app.get("/metrics")
async def metrics():
    totals = await get_metrics(pool)
    total = totals.get("companies_total") or 0
    failed = totals.get("scrape_failed") or 0
    enriched = totals.get("companies_enriched") or 0
    scraped_ok = total - failed
    return {
        "pipeline": {
            "scrape_success_rate": round(1 - (failed / total), 3) if total else 0,
            "enrich_success_rate": round(enriched / scraped_ok, 3) if scraped_ok else 0,
        },
        "totals": {
            "threads_processed": totals.get("threads_processed") or 0,
            "companies_total": total,
            "companies_enriched": enriched,
        },
    }
