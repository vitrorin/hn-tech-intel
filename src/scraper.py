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
        return list(await asyncio.gather(*tasks))
