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
