import hashlib
import json
from pathlib import Path

import asyncpg

from .models import CompanyInput, EnrichmentOutput, RawCompanyData


async def init_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(database_url, min_size=2, max_size=10)


async def run_migrations(pool: asyncpg.Pool, sql_path: str) -> None:
    sql = Path(sql_path).read_text()  # sync read — called once at startup before event loop is fully loaded
    async with pool.acquire() as conn:
        await conn.execute(sql)


def _domain_hash(domain: str) -> str:
    return hashlib.sha256(domain.lower().encode()).hexdigest()


async def insert_thread(pool: asyncpg.Pool, hn_id: int, title: str, month: str) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO hn_threads (hn_id, title, month)
            VALUES ($1, $2, $3)
            ON CONFLICT (hn_id) DO UPDATE SET title = EXCLUDED.title
            RETURNING id
            """,
            hn_id, title, month,
        )
        return row["id"]


async def insert_company(pool: asyncpg.Pool, company: CompanyInput, thread_id: int) -> int | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO companies (domain, domain_hash, name, url, hn_thread_id)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (domain_hash) DO NOTHING
            RETURNING id
            """,
            company.domain, _domain_hash(company.domain),
            company.name, company.url, thread_id,
        )
        return row["id"] if row else None


async def update_company_scrape(pool: asyncpg.Pool, company_id: int, raw: RawCompanyData) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE companies SET
                raw_html_sample  = $1,
                detected_scripts = $2,
                detected_headers = $3,
                scrape_status    = $4,
                scrape_error     = $5,
                scraped_at       = NOW()
            WHERE id = $6
            """,
            raw.body_text[:4000],
            json.dumps(raw.detected_scripts),
            json.dumps(raw.detected_headers),
            raw.scrape_status,
            raw.scrape_error,
            company_id,
        )


async def insert_enrichment(
    pool: asyncpg.Pool, company_id: int, model: str, output: EnrichmentOutput
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO enrichments
                (company_id, model, technologies, industry, company_size, analyst_brief, enrich_status)
            VALUES ($1, $2, $3, $4, $5, $6, 'done')
            """,
            company_id, model,
            json.dumps(output.technologies),
            output.industry,
            output.company_size_estimate,
            output.analyst_brief,
        )


async def get_company_by_id(pool: asyncpg.Pool, company_id: int) -> dict | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT c.id, c.name, c.url, c.domain, c.scrape_status,
                   e.technologies, e.industry, e.company_size, e.analyst_brief
            FROM companies c
            LEFT JOIN enrichments e ON e.company_id = c.id
            WHERE c.id = $1
            """,
            company_id,
        )
        return dict(row) if row else None


async def get_companies(
    pool: asyncpg.Pool, thread_id: int | None, page: int, limit: int
) -> list[dict]:
    offset = (page - 1) * limit
    async with pool.acquire() as conn:
        if thread_id:
            rows = await conn.fetch(
                """
                SELECT c.id, c.name, c.url, c.domain, c.scrape_status,
                       e.technologies, e.industry, e.company_size, e.analyst_brief
                FROM companies c
                LEFT JOIN enrichments e ON e.company_id = c.id
                WHERE c.hn_thread_id = (SELECT id FROM hn_threads WHERE hn_id = $1)
                ORDER BY c.id
                LIMIT $2 OFFSET $3
                """,
                thread_id, limit, offset,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT c.id, c.name, c.url, c.domain, c.scrape_status,
                       e.technologies, e.industry, e.company_size, e.analyst_brief
                FROM companies c
                LEFT JOIN enrichments e ON e.company_id = c.id
                ORDER BY c.id DESC
                LIMIT $1 OFFSET $2
                """,
                limit, offset,
            )
        return [dict(r) for r in rows]


async def get_threads(pool: asyncpg.Pool) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT t.id, t.hn_id, t.title, t.month, t.created_at, t.processed_at,
                   COUNT(c.id) AS company_count
            FROM hn_threads t
            LEFT JOIN companies c ON c.hn_thread_id = t.id
            GROUP BY t.id
            ORDER BY t.created_at DESC
            """
        )
        return [dict(r) for r in rows]


async def get_metrics(pool: asyncpg.Pool) -> dict:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(DISTINCT t.id)                                               AS threads_processed,
                COUNT(DISTINCT c.id)                                               AS companies_total,
                COUNT(DISTINCT e.id)                                               AS companies_enriched,
                COUNT(DISTINCT CASE WHEN c.scrape_status = 'failed' THEN c.id END) AS scrape_failed
            FROM hn_threads t
            LEFT JOIN companies c ON c.hn_thread_id = t.id
            LEFT JOIN enrichments e ON e.company_id = c.id
            """
        )
        return dict(row)
