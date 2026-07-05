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
