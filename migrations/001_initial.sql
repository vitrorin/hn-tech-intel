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
