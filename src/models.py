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
