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
