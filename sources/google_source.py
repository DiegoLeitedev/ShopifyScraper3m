"""
Fonte D — Google Search Dorks
Suporta dois modos:
  1. SerpAPI (recomendado, requer SERPAPI_KEY)
  2. Scraping direto do Google (sem chave, mais lento e bloqueável)

Queries utilizadas:
  - site:myshopify.com -site:help.shopify.com
  - "powered by shopify" site:.com.br
  - inurl:myshopify.com filetype:html
"""
import os
import re
import time
from bs4 import BeautifulSoup
from rich.console import Console
from utils.http import get, get_json
from utils.helpers import normalize_domain

console = Console()

SERPAPI_KEY = os.getenv("SERPAPI_KEY", "")
SERPAPI_URL = "https://serpapi.com/search"

DORK_QUERIES = [
    'site:myshopify.com -site:help.shopify.com',
    '"powered by shopify" site:.com.br',
    'inurl:myshopify.com filetype:html',
    '"powered by shopify" loja online brasil',
]


def fetch_domains() -> list[str]:
    console.print("[cyan]→ Fonte D: Google Dorks...[/cyan]")

    if SERPAPI_KEY:
        return _via_serpapi()
    else:
        console.print("[yellow]  SERPAPI_KEY não configurada — usando scraping direto (limitado)[/yellow]")
        return _via_scraping()


# ── SerpAPI ──────────────────────────────────────────────────────────────────

def _via_serpapi() -> list[str]:
    domains = []
    for query in DORK_QUERIES:
        for start in range(0, 30, 10):  # até 3 páginas por query
            data = get_json(SERPAPI_URL, params={
                "q": query,
                "api_key": SERPAPI_KEY,
                "num": 10,
                "start": start,
                "gl": "br",
                "hl": "pt",
            })
            if not data:
                break

            results = data.get("organic_results", [])
            if not results:
                break

            for r in results:
                link = r.get("link", "")
                if link:
                    domains.append(normalize_domain(link))

            time.sleep(1)

    console.print(f"[green]✓ Google/SerpAPI: {len(domains)} domínios[/green]")
    return domains


# ── Scraping direto ───────────────────────────────────────────────────────────

_GOOGLE_URL = "https://www.google.com/search"

def _via_scraping() -> list[str]:
    domains = []
    for query in DORK_QUERIES[:2]:  # Limitar queries no modo sem chave
        resp = get(_GOOGLE_URL, params={"q": query, "num": 20, "hl": "pt-BR"})
        if not resp or resp.status_code != 200:
            continue

        soup = BeautifulSoup(resp.text, "lxml")
        for a in soup.select("a[href]"):
            href = a["href"]
            # Links de resultado do Google começam com /url?q=
            match = re.search(r"/url\?q=(https?://[^&]+)", href)
            if match:
                domains.append(normalize_domain(match.group(1)))

        time.sleep(3)  # Delay maior para evitar bloqueio

    console.print(f"[green]✓ Google/Scraping: {len(domains)} domínios[/green]")
    return domains
