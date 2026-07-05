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
        # Skip empty domains, already-seen domains, or addresses without a TLD dot
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
    """Fetch a HN thread from the Algolia API. Returns (thread_data, title, month)."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        await asyncio.sleep(0.1)
        resp = await client.get(f"{HN_API}/{hn_id}")
        resp.raise_for_status()
        data = resp.json()
    title = data.get("title", f"HN Thread {hn_id}")
    created_at = data.get("created_at", "")
    month = created_at[:7] if created_at else ""
    return data, title, month
