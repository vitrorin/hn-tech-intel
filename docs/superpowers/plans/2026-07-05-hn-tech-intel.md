# HN Tech Intel — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python ETL pipeline that reads Hacker News "Who's Hiring" threads, scrapes each company's website for tech stack signals, enriches results with Claude LLM, stores everything in PostgreSQL, and exposes it via a FastAPI + Web UI.

**Architecture:** Three async pipeline stages (ingest → scrape → enrich) orchestrated by `pipeline.py`. Each stage runs concurrently behind semaphores. FastAPI serves the API and a single-file Web UI. PostgreSQL deduplicates companies by SHA-256 of their domain.

**Tech Stack:** Python 3.12, httpx (async HTTP), BeautifulSoup4 (HTML parsing), Anthropic Claude claude-haiku-4-5-20251001 (LLM enrichment via tool_use structured output), asyncpg (PostgreSQL driver), FastAPI + uvicorn, pydantic v2, pydantic-settings, uv (dependency manager), Docker (PostgreSQL only).

## Global Constraints

- Python 3.12 minimum — use `str | None` union syntax (not `Optional[str]`)
- asyncpg for all DB access — no SQLAlchemy, no ORM
- Claude claude-haiku-4-5-20251001 via `anthropic.AsyncAnthropic` — NOT OpenAI syntax; use `tool_choice={"type": "tool", "name": "analyze_company"}` for structured output
- pydantic v2 — use `model_config = SettingsConfigDict(...)` not `class Config`
- All tests use `asyncio_mode = "auto"` (set in pyproject.toml); mark async tests with `@pytest.mark.asyncio`
- No hardcoded API keys — all secrets via `.env` file loaded by pydantic-settings
- Project root: `~/Desktop/Proyectos/hn-tech-intel/`

---

## File Map

| File | Responsibility |
|------|---------------|
| `pyproject.toml` | Project config, dependencies, pytest settings |
| `docker-compose.yml` | PostgreSQL 16 container |
| `.env.example` | Template for required env vars |
| `migrations/001_initial.sql` | Full DB schema (3 tables + indexes) |
| `src/config.py` | pydantic-settings `Settings` class |
| `src/models.py` | Pydantic models: `CompanyInput`, `RawCompanyData`, `EnrichmentOutput`, `JobStatus` |
| `src/db.py` | All asyncpg queries (insert, update, select, metrics) |
| `src/ingestor.py` | HN Algolia API fetch + comment parsing |
| `src/scraper.py` | Async scraping pool, tech signal extraction |
| `src/enricher.py` | Claude tool_use enrichment pool |
| `src/pipeline.py` | Orchestrates all 3 stages, updates JobStatus |
| `src/api.py` | FastAPI app, all 6 endpoints, lifespan |
| `static/index.html` | Single-page Web UI (Vanilla JS) |
| `tests/conftest.py` | Shared test fixtures |
| `tests/test_ingestor.py` | Unit tests for HN parsing |
| `tests/test_scraper.py` | Unit tests for tech signal extraction |
| `tests/test_enricher.py` | Unit tests for LLM enricher (mocked client) |

---

### Task 1: Project Scaffold

**Files:**
- Create: `pyproject.toml`
- Create: `docker-compose.yml`
- Create: `.env.example`
- Create: `src/__init__.py`, `tests/__init__.py`, `migrations/`, `static/`

**Interfaces:**
- Produces: `uv run pytest` works; `docker-compose up -d` starts postgres on port 5432

- [ ] **Step 1: Create directory structure**

```bash
cd ~/Desktop/Proyectos/hn-tech-intel
mkdir -p src tests migrations static
touch src/__init__.py tests/__init__.py
```

- [ ] **Step 2: Write `pyproject.toml`**

```toml
[project]
name = "hn-tech-intel"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "httpx>=0.27",
    "beautifulsoup4>=4.12",
    "anthropic>=0.34",
    "asyncpg>=0.29",
    "fastapi>=0.115",
    "uvicorn[standard]>=0.30",
    "pydantic>=2.9",
    "pydantic-settings>=2.5",
    "python-dotenv>=1.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.3",
    "pytest-asyncio>=0.24",
    "anyio>=4.6",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

- [ ] **Step 3: Write `docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: hn_tech_intel
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

- [ ] **Step 4: Write `.env.example`**

```
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/hn_tech_intel
ANTHROPIC_API_KEY=sk-ant-...
HN_SCRAPE_CONCURRENCY=15
LLM_CONCURRENCY=5
SCRAPE_TIMEOUT_SECONDS=10
```

Copy to `.env` and fill in `ANTHROPIC_API_KEY`.

- [ ] **Step 5: Install dependencies**

```bash
uv sync --extra dev
```

Expected: lock file created, `.venv/` populated.

- [ ] **Step 6: Start PostgreSQL**

```bash
docker-compose up -d
```

Expected: `postgres` container running on port 5432.

- [ ] **Step 7: Verify pytest runs**

```bash
uv run pytest --collect-only
```

Expected: `no tests ran` (0 tests collected, 0 errors).

---

### Task 2: Config + Models

**Files:**
- Create: `src/config.py`
- Create: `src/models.py`

**Interfaces:**
- Produces:
  - `from src.config import settings` → `Settings` instance with all env vars
  - `from src.models import CompanyInput, RawCompanyData, EnrichmentOutput, JobStatus`

- [ ] **Step 1: Write `src/config.py`**

```python
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    database_url: str
    anthropic_api_key: str
    hn_scrape_concurrency: int = 15
    llm_concurrency: int = 5
    scrape_timeout_seconds: int = 10


settings = Settings()
```

- [ ] **Step 2: Write `src/models.py`**

```python
from pydantic import BaseModel


class CompanyInput(BaseModel):
    name: str
    url: str
    domain: str
    raw_post_text: str


class RawCompanyData(BaseModel):
    company: CompanyInput
    detected_scripts: list[str]
    detected_headers: dict[str, str]
    body_text: str
    scrape_status: str  # "done" | "failed"
    scrape_error: str | None = None


class EnrichmentOutput(BaseModel):
    technologies: list[str]
    industry: str
    company_size_estimate: str  # "startup" | "smb" | "enterprise"
    analyst_brief: str


class JobStatus(BaseModel):
    job_id: str
    thread_id: int
    total: int = 0
    scraped: int = 0
    enriched: int = 0
    failed: int = 0
    status: str = "running"  # "running" | "done" | "failed"
```

- [ ] **Step 3: Verify imports work**

```bash
uv run python -c "from src.config import settings; print(settings.database_url)"
```

Expected: prints the DATABASE_URL from your `.env`.

---

### Task 3: Database Schema + Layer

**Files:**
- Create: `migrations/001_initial.sql`
- Create: `src/db.py`

**Interfaces:**
- Consumes: `CompanyInput`, `RawCompanyData`, `EnrichmentOutput` from `src.models`
- Produces:
  - `init_pool(database_url: str) -> asyncpg.Pool`
  - `run_migrations(pool, sql_path: str) -> None`
  - `insert_thread(pool, hn_id: int, title: str, month: str) -> int`
  - `insert_company(pool, company: CompanyInput, thread_id: int) -> int | None` — None if deduped
  - `update_company_scrape(pool, company_id: int, raw: RawCompanyData) -> None`
  - `insert_enrichment(pool, company_id: int, model: str, output: EnrichmentOutput) -> None`
  - `get_company_by_id(pool, company_id: int) -> dict | None`
  - `get_companies(pool, thread_id: int | None, page: int, limit: int) -> list[dict]`
  - `get_threads(pool) -> list[dict]`
  - `get_metrics(pool) -> dict`

- [ ] **Step 1: Write `migrations/001_initial.sql`**

```sql
CREATE TABLE IF NOT EXISTS hn_threads (
    id           SERIAL PRIMARY KEY,
    hn_id        INTEGER UNIQUE NOT NULL,
    title        TEXT,
    month        TEXT,
    created_at   TIMESTAMP DEFAULT NOW(),
    processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS companies (
    id               SERIAL PRIMARY KEY,
    domain           TEXT UNIQUE NOT NULL,
    domain_hash      CHAR(64) UNIQUE NOT NULL,
    name             TEXT,
    url              TEXT,
    hn_thread_id     INTEGER REFERENCES hn_threads(id),
    raw_html_sample  TEXT,
    detected_scripts JSONB,
    detected_headers JSONB,
    scrape_status    TEXT DEFAULT 'pending',
    scrape_error     TEXT,
    scraped_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS enrichments (
    id            SERIAL PRIMARY KEY,
    company_id    INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    model         TEXT NOT NULL,
    technologies  JSONB,
    industry      TEXT,
    company_size  TEXT,
    analyst_brief TEXT,
    enrich_status TEXT DEFAULT 'pending',
    enrich_error  TEXT,
    enriched_at   TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_companies_domain_hash ON companies(domain_hash);
CREATE INDEX IF NOT EXISTS idx_enrichments_company_id ON enrichments(company_id);
```

- [ ] **Step 2: Write `src/db.py`**

```python
import hashlib
import json

import asyncpg

from .models import CompanyInput, EnrichmentOutput, RawCompanyData


async def init_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(database_url, min_size=2, max_size=10)


async def run_migrations(pool: asyncpg.Pool, sql_path: str) -> None:
    with open(sql_path) as f:
        sql = f.read()
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
                WHERE c.hn_thread_id = $1
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
                COUNT(DISTINCT t.id)                                              AS threads_processed,
                COUNT(DISTINCT c.id)                                              AS companies_total,
                COUNT(DISTINCT e.id)                                              AS companies_enriched,
                COUNT(DISTINCT CASE WHEN c.scrape_status = 'failed' THEN c.id END) AS scrape_failed
            FROM hn_threads t
            LEFT JOIN companies c ON c.hn_thread_id = t.id
            LEFT JOIN enrichments e ON e.company_id = c.id
            """
        )
        return dict(row)
```

- [ ] **Step 3: Run migrations against the running DB**

```bash
uv run python -c "
import asyncio, asyncpg
async def main():
    pool = await asyncpg.create_pool('postgresql://postgres:postgres@localhost:5432/hn_tech_intel')
    with open('migrations/001_initial.sql') as f: sql = f.read()
    async with pool.acquire() as conn: await conn.execute(sql)
    print('OK')
asyncio.run(main())
"
```

Expected: prints `OK`.

---

### Task 4: Ingestor

**Files:**
- Create: `src/ingestor.py`
- Create: `tests/test_ingestor.py`

**Interfaces:**
- Consumes: `CompanyInput` from `src.models`
- Produces:
  - `parse_companies(thread_data: dict) -> list[CompanyInput]`
  - `fetch_thread(hn_id: int) -> tuple[dict, str, str]` — returns `(thread_data, title, month)`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_ingestor.py
import pytest
from src.ingestor import parse_companies, _extract_domain, _extract_company_name

SAMPLE_THREAD = {
    "children": [
        {"text": "Acme Corp | Remote | https://acme.io<p>We are hiring Python engineers."},
        {"text": "No URLs here, just plain text."},
        {"text": "Acme Corp again | https://acme.io"},  # dedup — same domain
        {"text": "Beta Inc - Full Stack | https://beta.com | We build cool things"},
        {"text": "No domain | https://localhost"},  # invalid domain — no TLD
    ]
}

def test_parse_companies_extracts_urls():
    companies = parse_companies(SAMPLE_THREAD)
    domains = {c.domain for c in companies}
    assert "acme.io" in domains
    assert "beta.com" in domains

def test_parse_companies_deduplicates_domains():
    companies = parse_companies(SAMPLE_THREAD)
    domains = [c.domain for c in companies]
    assert domains.count("acme.io") == 1

def test_parse_companies_skips_no_url():
    companies = parse_companies(SAMPLE_THREAD)
    assert len(companies) == 2

def test_extract_domain_strips_www():
    assert _extract_domain("https://www.example.com/path") == "example.com"

def test_extract_domain_handles_path():
    assert _extract_domain("https://acme.io/jobs?ref=hn") == "acme.io"

def test_extract_company_name_pipe_separator():
    assert _extract_company_name("Acme Corp | Remote | Python") == "Acme Corp"

def test_extract_company_name_dash_separator():
    assert _extract_company_name("Beta Inc - Full Stack") == "Beta Inc"

def test_extract_company_name_fallback():
    result = _extract_company_name("hiring great engineers everywhere")
    assert len(result) > 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_ingestor.py -v
```

Expected: `ImportError` or `ModuleNotFoundError` — `src.ingestor` doesn't exist yet.

- [ ] **Step 3: Write `src/ingestor.py`**

```python
import asyncio
import re
from urllib.parse import urlparse

import httpx

from .models import CompanyInput

HN_API = "https://hn.algolia.com/api/v1/items"
URL_RE = re.compile(r"https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}[^\s<>\"']*")


def _extract_domain(url: str) -> str:
    try:
        netloc = urlparse(url).netloc
        return netloc.removeprefix("www.")
    except Exception:
        return ""


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", text or "").strip()


def _extract_company_name(text: str) -> str:
    clean = _strip_html(text)
    for sep in [" | ", " - ", ": "]:
        if sep in clean:
            return clean.split(sep)[0].strip()[:80]
    words = clean.split()
    return " ".join(words[:4]) if words else "Unknown"


def parse_companies(thread_data: dict) -> list[CompanyInput]:
    companies: dict[str, CompanyInput] = {}
    for comment in thread_data.get("children", []):
        text = comment.get("text") or ""
        urls = URL_RE.findall(text)
        if not urls:
            continue
        url = urls[0]
        domain = _extract_domain(url)
        # Skip empty domains or localhost/IP-style addresses (no TLD dot)
        if not domain or domain in companies or "." not in domain:
            continue
        companies[domain] = CompanyInput(
            name=_extract_company_name(text),
            url=url,
            domain=domain,
            raw_post_text=_strip_html(text)[:500],
        )
    return list(companies.values())


async def fetch_thread(hn_id: int) -> tuple[dict, str, str]:
    """Returns (thread_data, title, month)."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        await asyncio.sleep(0.1)
        resp = await client.get(f"{HN_API}/{hn_id}")
        resp.raise_for_status()
        data = resp.json()
    title = data.get("title", f"HN Thread {hn_id}")
    created_at = data.get("created_at", "")
    month = created_at[:7] if created_at else ""
    return data, title, month
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_ingestor.py -v
```

Expected: all 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git init && git add pyproject.toml docker-compose.yml .env.example migrations/ src/ tests/test_ingestor.py
git commit -m "feat: project scaffold, models, db layer, and ingestor"
```

---

### Task 5: Scraper Pool

**Files:**
- Create: `src/scraper.py`
- Create: `tests/test_scraper.py`

**Interfaces:**
- Consumes: `CompanyInput`, `RawCompanyData` from `src.models`
- Produces:
  - `scrape_batch(companies: list[CompanyInput], concurrency: int, timeout: int) -> list[RawCompanyData]`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_scraper.py
import pytest
from bs4 import BeautifulSoup
from src.scraper import _extract_scripts, _extract_body_text, _extract_meta, scrape_batch
from src.models import CompanyInput

SAMPLE_HTML = """<html><head>
<meta name="generator" content="Next.js">
<script src="https://cdn.example.com/react.js"></script>
<script>var inline = 1;</script>
</head><body>
<nav>Nav content</nav>
<p>We build amazing fintech products.</p>
<script>var x = 1;</script>
<footer>Footer</footer>
</body></html>"""


def make_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def test_extract_scripts_only_external():
    soup = make_soup(SAMPLE_HTML)
    scripts = _extract_scripts(soup)
    assert scripts == ["https://cdn.example.com/react.js"]


def test_extract_scripts_ignores_inline():
    soup = make_soup(SAMPLE_HTML)
    scripts = _extract_scripts(soup)
    assert not any("inline" in s for s in scripts)


def test_extract_body_text_excludes_nav_footer_scripts():
    soup = make_soup(SAMPLE_HTML)
    text = _extract_body_text(soup)
    assert "var x" not in text
    assert "Nav content" not in text
    assert "We build amazing fintech products" in text


def test_extract_meta_generator():
    soup = make_soup(SAMPLE_HTML)
    meta = _extract_meta(soup)
    assert meta.get("generator") == "Next.js"


@pytest.mark.asyncio
async def test_scrape_batch_handles_unreachable_url():
    companies = [
        CompanyInput(
            name="Test",
            url="https://this-domain-does-not-exist-xyz123.io",
            domain="this-domain-does-not-exist-xyz123.io",
            raw_post_text="",
        )
    ]
    results = await scrape_batch(companies, concurrency=1, timeout=3)
    assert len(results) == 1
    assert results[0].scrape_status == "failed"
    assert results[0].scrape_error is not None


@pytest.mark.asyncio
async def test_scrape_batch_returns_one_result_per_company():
    companies = [
        CompanyInput(name="A", url="https://nonexistent-a.io", domain="nonexistent-a.io", raw_post_text=""),
        CompanyInput(name="B", url="https://nonexistent-b.io", domain="nonexistent-b.io", raw_post_text=""),
    ]
    results = await scrape_batch(companies, concurrency=2, timeout=2)
    assert len(results) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_scraper.py -v
```

Expected: `ImportError` — `src.scraper` doesn't exist yet.

- [ ] **Step 3: Write `src/scraper.py`**

```python
import asyncio
import re

import httpx
from bs4 import BeautifulSoup

from .models import CompanyInput, RawCompanyData

TECH_HEADERS = {"x-powered-by", "server", "x-generator", "x-framework"}


def _extract_scripts(soup: BeautifulSoup) -> list[str]:
    return [
        tag["src"]
        for tag in soup.find_all("script", src=True)
        if tag.get("src", "").startswith("http")
    ][:30]


def _extract_meta(soup: BeautifulSoup) -> dict[str, str]:
    result = {}
    for tag in soup.find_all("meta"):
        name = tag.get("name", tag.get("property", "")).lower()
        if name in {"generator", "framework", "application-name"}:
            result[name] = tag.get("content", "")
    return result


def _extract_headers(headers: httpx.Headers) -> dict[str, str]:
    return {k.lower(): v for k, v in headers.items() if k.lower() in TECH_HEADERS}


def _extract_body_text(soup: BeautifulSoup) -> str:
    for tag in soup(["script", "style", "nav", "footer"]):
        tag.decompose()
    text = re.sub(r"\s+", " ", soup.get_text()).strip()
    return text[:2000]


async def scrape_one(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    company: CompanyInput,
) -> RawCompanyData:
    async with semaphore:
        try:
            resp = await client.get(company.url)
            soup = BeautifulSoup(resp.text, "html.parser")
            return RawCompanyData(
                company=company,
                detected_scripts=_extract_scripts(soup),
                detected_headers={**_extract_headers(resp.headers), **_extract_meta(soup)},
                body_text=_extract_body_text(soup),
                scrape_status="done",
            )
        except Exception as e:
            return RawCompanyData(
                company=company,
                detected_scripts=[],
                detected_headers={},
                body_text="",
                scrape_status="failed",
                scrape_error=str(e)[:300],
            )


async def scrape_batch(
    companies: list[CompanyInput],
    concurrency: int = 15,
    timeout: int = 10,
) -> list[RawCompanyData]:
    semaphore = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        tasks = [scrape_one(client, semaphore, c) for c in companies]
        return await asyncio.gather(*tasks)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_scraper.py -v
```

Expected: all 6 tests PASS. The timeout tests may take 3s each.

- [ ] **Step 5: Commit**

```bash
git add src/scraper.py tests/test_scraper.py
git commit -m "feat: async scraper pool with tech signal extraction"
```

---

### Task 6: LLM Enricher

**Files:**
- Create: `src/enricher.py`
- Create: `tests/test_enricher.py`

**Interfaces:**
- Consumes: `CompanyInput`, `RawCompanyData`, `EnrichmentOutput` from `src.models`
- Produces:
  - `MODEL: str` = `"claude-haiku-4-5-20251001"` (importable constant)
  - `enrich_batch(items: list[tuple[CompanyInput, RawCompanyData]], api_key: str, concurrency: int) -> list[EnrichmentOutput | None]`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_enricher.py
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock
from src.enricher import enrich_one, enrich_batch, _build_prompt, MODEL
from src.models import CompanyInput, RawCompanyData, EnrichmentOutput

COMPANY = CompanyInput(
    name="Acme",
    url="https://acme.io",
    domain="acme.io",
    raw_post_text="We build fintech infrastructure for payments.",
)
RAW_DONE = RawCompanyData(
    company=COMPANY,
    detected_scripts=["https://cdn.react.dev/react.js"],
    detected_headers={"x-powered-by": "Next.js"},
    body_text="Acme is a fintech platform helping banks modernize payments.",
    scrape_status="done",
)
RAW_FAILED = RawCompanyData(
    company=COMPANY,
    detected_scripts=[],
    detected_headers={},
    body_text="",
    scrape_status="failed",
    scrape_error="timeout",
)

MOCK_TOOL_INPUT = {
    "technologies": ["React", "Next.js"],
    "industry": "FinTech",
    "company_size_estimate": "startup",
    "analyst_brief": "Acme is a fintech startup building payment infrastructure for banks.",
}


def make_mock_client() -> MagicMock:
    tool_block = MagicMock()
    tool_block.type = "tool_use"
    tool_block.input = MOCK_TOOL_INPUT

    response = MagicMock()
    response.content = [tool_block]

    client = MagicMock()
    client.messages.create = AsyncMock(return_value=response)
    return client


@pytest.mark.asyncio
async def test_enrich_one_returns_enrichment_output():
    client = make_mock_client()
    semaphore = asyncio.Semaphore(5)
    result = await enrich_one(client, semaphore, COMPANY, RAW_DONE)
    assert isinstance(result, EnrichmentOutput)
    assert result.industry == "FinTech"
    assert "React" in result.technologies
    assert len(result.analyst_brief) > 10


@pytest.mark.asyncio
async def test_enrich_one_skips_failed_scrape():
    client = make_mock_client()
    semaphore = asyncio.Semaphore(5)
    result = await enrich_one(client, semaphore, COMPANY, RAW_FAILED)
    assert result is None
    client.messages.create.assert_not_called()


def test_build_prompt_includes_key_info():
    prompt = _build_prompt(COMPANY, RAW_DONE)
    assert "Acme" in prompt
    assert "acme.io" in prompt
    assert "fintech" in prompt.lower()


def test_model_constant_is_haiku():
    assert "haiku" in MODEL.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_enricher.py -v
```

Expected: `ImportError` — `src.enricher` doesn't exist yet.

- [ ] **Step 3: Write `src/enricher.py`**

```python
import asyncio
import json

import anthropic

from .models import CompanyInput, EnrichmentOutput, RawCompanyData

MODEL = "claude-haiku-4-5-20251001"

ANALYZE_TOOL = {
    "name": "analyze_company",
    "description": "Analyze a company's tech stack and generate an analyst brief",
    "input_schema": {
        "type": "object",
        "properties": {
            "technologies": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Technologies detected (React, PostgreSQL, AWS, etc.)",
            },
            "industry": {
                "type": "string",
                "description": "Industry sector (FinTech, HealthTech, DevTools, SaaS, etc.)",
            },
            "company_size_estimate": {
                "type": "string",
                "enum": ["startup", "smb", "enterprise"],
                "description": "Estimated company size from available signals",
            },
            "analyst_brief": {
                "type": "string",
                "description": "2-3 sentence brief: what they do, tech stack, why notable",
            },
        },
        "required": ["technologies", "industry", "company_size_estimate", "analyst_brief"],
    },
}


def _build_prompt(company: CompanyInput, raw: RawCompanyData) -> str:
    return f"""Analyze this company from a Hacker News job posting:

Company: {company.name}
URL: {company.url}

Job posting excerpt:
{company.raw_post_text[:300]}

Detected script sources: {', '.join(raw.detected_scripts[:10]) or 'none'}
HTTP headers: {json.dumps(raw.detected_headers)}
Website text: {raw.body_text[:1500]}

Call analyze_company with your analysis."""


async def enrich_one(
    client: anthropic.AsyncAnthropic,
    semaphore: asyncio.Semaphore,
    company: CompanyInput,
    raw: RawCompanyData,
) -> EnrichmentOutput | None:
    if raw.scrape_status == "failed":
        return None
    prompt = _build_prompt(company, raw)
    for attempt in range(2):
        try:
            async with semaphore:
                response = await client.messages.create(
                    model=MODEL,
                    max_tokens=512,
                    tools=[ANALYZE_TOOL],
                    tool_choice={"type": "tool", "name": "analyze_company"},
                    messages=[{"role": "user", "content": prompt}],
                )
            tool_use = next(b for b in response.content if b.type == "tool_use")
            return EnrichmentOutput(**tool_use.input)
        except Exception:
            if attempt == 0:
                await asyncio.sleep(2)
    return None


async def enrich_batch(
    items: list[tuple[CompanyInput, RawCompanyData]],
    api_key: str,
    concurrency: int = 5,
) -> list[EnrichmentOutput | None]:
    semaphore = asyncio.Semaphore(concurrency)
    client = anthropic.AsyncAnthropic(api_key=api_key)
    tasks = [enrich_one(client, semaphore, company, raw) for company, raw in items]
    return await asyncio.gather(*tasks)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_enricher.py -v
```

Expected: all 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/enricher.py tests/test_enricher.py
git commit -m "feat: Claude LLM enricher with tool_use structured output"
```

---

### Task 7: Pipeline Orchestrator

**Files:**
- Create: `src/pipeline.py`

**Interfaces:**
- Consumes: all of `src.db`, `src.enricher`, `src.ingestor`, `src.scraper`, `src.models`
- Produces:
  - `run_pipeline(pool, jobs: dict[str, JobStatus], job_id: str, hn_thread_id: int, settings: Settings) -> None`

No unit tests for pipeline — it's integration-level glue. Verification is done by running it end-to-end in Task 9.

- [ ] **Step 1: Write `src/pipeline.py`**

```python
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

        outputs = await enrich_batch(enrichable_pairs, settings.anthropic_api_key, settings.llm_concurrency)

        for company_id, output in zip(enrichable_ids, outputs):
            if output is not None:
                await insert_enrichment(pool, company_id, MODEL, output)
                job.enriched += 1
            else:
                job.failed += 1

        # Mark thread done
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE hn_threads SET processed_at = NOW() WHERE hn_id = $1",
                hn_thread_id,
            )

        job.status = "done"

    except Exception:
        job.status = "failed"
        raise
```

- [ ] **Step 2: Verify import works**

```bash
uv run python -c "from src.pipeline import run_pipeline; print('OK')"
```

Expected: prints `OK`.

- [ ] **Step 3: Commit**

```bash
git add src/pipeline.py
git commit -m "feat: pipeline orchestrator connecting all three stages"
```

---

### Task 8: FastAPI Application

**Files:**
- Create: `src/api.py`

**Interfaces:**
- Consumes: `init_pool`, `run_migrations`, `get_companies`, `get_company_by_id`, `get_threads`, `get_metrics` from `src.db`; `run_pipeline` from `src.pipeline`; `JobStatus` from `src.models`; `settings` from `src.config`
- Produces: HTTP server at `http://localhost:8000` with 6 endpoints + static UI

- [ ] **Step 1: Write `src/api.py`**

```python
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
    pool = await init_pool(settings.database_url)
    migrations_path = Path(__file__).parent.parent / "migrations" / "001_initial.sql"
    await run_migrations(pool, str(migrations_path))
    yield
    await pool.close()


app = FastAPI(title="HN Tech Intel", lifespan=lifespan)


class IngestRequest(BaseModel):
    thread_id: int


@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    html_path = Path(__file__).parent.parent / "static" / "index.html"
    return HTMLResponse(html_path.read_text())


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
```

- [ ] **Step 2: Start the server**

```bash
uv run uvicorn src.api:app --reload
```

Expected: server starts on `http://localhost:8000`. No errors in startup logs.

- [ ] **Step 3: Smoke-test the API**

```bash
curl http://localhost:8000/threads
curl http://localhost:8000/metrics
curl http://localhost:8000/companies
```

Expected: all return `200` with JSON (empty arrays/objects).

- [ ] **Step 4: Commit**

```bash
git add src/api.py
git commit -m "feat: FastAPI with all 6 endpoints and lifespan DB setup"
```

---

### Task 9: Web UI

**Files:**
- Create: `static/index.html`

**Interfaces:**
- Consumes: `POST /ingest`, `GET /jobs/{job_id}`, `GET /companies?thread_id=` via Fetch API
- Produces: Single-page UI served at `http://localhost:8000`

- [ ] **Step 1: Write `static/index.html`**

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>HN Tech Intel</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, sans-serif; background: #f5f5f5; color: #111; }
    header { background: #ff6600; color: white; padding: 1rem 2rem; }
    header h1 { font-size: 1.4rem; font-weight: 700; }
    header p { font-size: 0.85rem; opacity: 0.9; margin-top: 0.2rem; }
    main { max-width: 900px; margin: 2rem auto; padding: 0 1rem; }
    .form-row { display: flex; gap: 0.5rem; margin-bottom: 1.5rem; }
    input[type="text"] {
      flex: 1; padding: 0.6rem 1rem; border: 1px solid #ddd;
      border-radius: 6px; font-size: 1rem;
    }
    button {
      padding: 0.6rem 1.4rem; background: #ff6600; color: white;
      border: none; border-radius: 6px; font-size: 1rem; cursor: pointer;
    }
    button:disabled { background: #aaa; cursor: not-allowed; }
    #progress { margin-bottom: 1.5rem; display: none; }
    .progress-bar-wrap { background: #ddd; border-radius: 4px; height: 8px; margin: 0.5rem 0; }
    .progress-bar { background: #ff6600; height: 8px; border-radius: 4px; transition: width 0.3s; }
    .counters { font-size: 0.85rem; color: #555; }
    #error { color: red; margin-bottom: 1rem; display: none; }
    .cards { display: grid; gap: 1rem; }
    .card {
      background: white; border-radius: 8px; padding: 1.2rem;
      box-shadow: 0 1px 4px rgba(0,0,0,0.08);
    }
    .card h2 { font-size: 1.1rem; margin-bottom: 0.3rem; }
    .card a { color: #ff6600; text-decoration: none; font-size: 0.85rem; }
    .badges { display: flex; flex-wrap: wrap; gap: 0.4rem; margin: 0.6rem 0; }
    .badge {
      background: #f0f0f0; border-radius: 12px;
      padding: 0.2rem 0.7rem; font-size: 0.75rem; color: #333;
    }
    .meta { font-size: 0.8rem; color: #777; margin-bottom: 0.5rem; }
    .brief { font-size: 0.9rem; line-height: 1.5; color: #444; }
  </style>
</head>
<body>
  <header>
    <h1>HN Tech Intel</h1>
    <p>Tech stack intelligence from Hacker News "Who's Hiring" threads</p>
  </header>
  <main>
    <div class="form-row">
      <input type="text" id="thread-id" placeholder="HN Thread ID (e.g. 43985731)" />
      <button id="analyze-btn" onclick="startAnalysis()">Analyze</button>
    </div>
    <div id="error"></div>
    <div id="progress">
      <div class="progress-bar-wrap">
        <div class="progress-bar" id="bar" style="width:0%"></div>
      </div>
      <div class="counters" id="counters">Starting...</div>
    </div>
    <div class="cards" id="cards"></div>
  </main>

  <script>
    let pollInterval = null;

    function showError(msg) {
      const el = document.getElementById('error');
      el.textContent = msg;
      el.style.display = 'block';
    }

    async function startAnalysis() {
      const input = document.getElementById('thread-id').value.trim();
      const threadId = parseInt(input, 10);
      if (!threadId || threadId <= 0) { showError('Enter a valid HN thread ID'); return; }

      document.getElementById('error').style.display = 'none';
      document.getElementById('cards').innerHTML = '';
      document.getElementById('progress').style.display = 'block';
      document.getElementById('analyze-btn').disabled = true;

      const res = await fetch('/ingest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ thread_id: threadId }),
      });
      if (!res.ok) { showError('Failed to start analysis'); return; }
      const { job_id } = await res.json();

      pollInterval = setInterval(() => pollJob(job_id, threadId), 2000);
    }

    async function pollJob(jobId, threadId) {
      const res = await fetch(`/jobs/${jobId}`);
      if (!res.ok) return;
      const job = await res.json();

      const pct = job.total > 0 ? Math.round((job.enriched / job.total) * 100) : 0;
      document.getElementById('bar').style.width = pct + '%';
      document.getElementById('counters').textContent =
        `Scraped: ${job.scraped}/${job.total} | Enriched: ${job.enriched}/${job.total} | Failed: ${job.failed}`;

      if (job.status === 'done') {
        clearInterval(pollInterval);
        document.getElementById('analyze-btn').disabled = false;
        await loadCompanies(threadId);
      } else if (job.status === 'failed') {
        clearInterval(pollInterval);
        document.getElementById('analyze-btn').disabled = false;
        showError('Pipeline failed. Check server logs.');
      }
    }

    async function loadCompanies(threadId) {
      const res = await fetch(`/companies?thread_id=${threadId}&limit=100`);
      const companies = await res.json();
      const container = document.getElementById('cards');
      container.innerHTML = '';

      companies.forEach(c => {
        const techs = (() => { try { return JSON.parse(c.technologies || '[]'); } catch { return []; } })();
        const card = document.createElement('div');
        card.className = 'card';
        card.innerHTML = `
          <h2>${escHtml(c.name || 'Unknown')}</h2>
          <a href="${escHtml(c.url || '#')}" target="_blank">${escHtml(c.domain || '')}</a>
          <div class="badges">${techs.map(t => `<span class="badge">${escHtml(t)}</span>`).join('')}</div>
          <div class="meta">${escHtml(c.industry || '')}${c.company_size ? ' · ' + escHtml(c.company_size) : ''}</div>
          <div class="brief">${escHtml(c.analyst_brief || 'No analyst brief available.')}</div>
        `;
        container.appendChild(card);
      });
    }

    function escHtml(str) {
      return String(str)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;')
        .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }
  </script>
</body>
</html>
```

- [ ] **Step 2: Verify the UI loads**

Open `http://localhost:8000` in a browser.

Expected: orange header "HN Tech Intel", input field + Analyze button visible.

- [ ] **Step 3: End-to-end test with real HN thread**

Find a real "Who's Hiring" thread ID on HN (search for "Ask HN: Who is hiring"). Enter the ID and click Analyze. Wait ~60-90 seconds.

Expected:
- Progress bar advances
- Cards appear when done with tech badges and analyst briefs
- `GET /metrics` shows `scrape_success_rate > 0`

- [ ] **Step 4: Run all unit tests one final time**

```bash
uv run pytest -v
```

Expected: all tests PASS.

- [ ] **Step 5: Final commit**

```bash
git add static/index.html
git commit -m "feat: single-page Web UI with progress tracking and company cards"
```

---

## Verification Checklist

After all tasks are complete:

- [ ] `uv run pytest -v` — all tests green
- [ ] `docker-compose up -d && uv run uvicorn src.api:app` — server starts without errors
- [ ] POST to `/ingest` with a real HN thread ID returns a `job_id`
- [ ] `GET /jobs/{job_id}` shows progress advancing
- [ ] `GET /companies?thread_id=X` returns companies with `analyst_brief` populated
- [ ] `GET /metrics` shows `scrape_success_rate > 0` and `companies_total > 0`
- [ ] UI at `http://localhost:8000` renders company cards with tech badges after a run
