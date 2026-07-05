import asyncpg

from .config import Settings
from .db import (
    insert_company,
    insert_enrichment,
    insert_thread,
    update_company_scrape,
)
from .enricher import MODEL, enrich_batch
from .ingestor import fetch_thread, parse_companies
from .models import JobStatus
from .scraper import scrape_batch


async def run_pipeline(
    pool: asyncpg.Pool,
    jobs: dict[str, "JobStatus"],
    job_id: str,
    hn_thread_id: int,
    settings: Settings,
) -> None:
    job = jobs[job_id]
    try:
        # Stage 1: Fetch thread from HN
        thread_data, title, month = await fetch_thread(hn_thread_id)
        thread_db_id = await insert_thread(pool, hn_thread_id, title, month)
        companies = parse_companies(thread_data)

        # Stage 2: Insert companies (skip duplicates by domain_hash)
        company_pairs: list[tuple[int, object]] = []
        for company in companies:
            company_id = await insert_company(pool, company, thread_db_id)
            if company_id is not None:
                company_pairs.append((company_id, company))

        job.total = len(company_pairs)
        if not company_pairs:
            job.status = "done"
            return

        # Stage 3: Scrape all companies concurrently
        raw_results = await scrape_batch(
            [c for _, c in company_pairs],
            concurrency=settings.hn_scrape_concurrency,
            timeout=settings.scrape_timeout_seconds,
        )

        # Stage 4: Persist scrape results
        for (company_id, _), raw in zip(company_pairs, raw_results):
            await update_company_scrape(pool, company_id, raw)
            if raw.scrape_status == "done":
                job.scraped += 1
            else:
                job.failed += 1

        # Stage 5: Enrich successfully scraped companies
        enrichable_pairs = [
            (company, raw)
            for (_, company), raw in zip(company_pairs, raw_results)
            if raw.scrape_status == "done"
        ]
        enrichable_ids = [
            company_id
            for (company_id, _), raw in zip(company_pairs, raw_results)
            if raw.scrape_status == "done"
        ]

        outputs = await enrich_batch(
            enrichable_pairs, settings.anthropic_api_key, settings.llm_concurrency
        )

        for company_id, output in zip(enrichable_ids, outputs):
            if output is not None:
                await insert_enrichment(pool, company_id, MODEL, output)
                job.enriched += 1
            else:
                job.failed += 1

        # Mark thread processed
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE hn_threads SET processed_at = NOW() WHERE hn_id = $1",
                hn_thread_id,
            )

        job.status = "done"

    except Exception:
        job.status = "failed"
        raise
