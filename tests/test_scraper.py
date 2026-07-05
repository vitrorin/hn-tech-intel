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
