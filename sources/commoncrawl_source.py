"""
Fonte B — CommonCrawl + Wayback Machine (foco .com.br)

Estratégias em cascata:
  1. CommonCrawl CDX — múltiplos índices recentes, padrões Shopify + .com.br
  2. Wayback Machine CDX — cobertura complementar de lojas BR
  3. Resolve *.myshopify.com → filtra redirects para .com.br
"""
import os
import re
import json
import concurrent.futures
import requests
from rich.console import Console
from utils.http import get
from utils.helpers import normalize_domain, is_myshopify

console = Console()

# Índices do CommonCrawl — do mais recente para o mais antigo
CC_INDEXES = [
    os.getenv("COMMONCRAWL_INDEX", "CC-MAIN-2025-05"),
    "CC-MAIN-2024-42",
    "CC-MAIN-2024-26",
    "CC-MAIN-2024-10",
]

WAYBACK_CDX = "http://web.archive.org/cdx/search/cdx"

# Padrões de URL que indicam Shopify
SHOPIFY_URL_PATTERNS = [
    "*.com.br/cdn/shop/*",
    "*.com.br/collections/*",
    "*.com.br/products/*",
    "*.com.br/cart",
    "*.com.br/account/login",
]


def fetch_domains(limit: int = 500) -> list[str]:
    console.print("[cyan]→ Fonte B: CommonCrawl + Wayback Machine (.com.br)...[/cyan]")

    found = set()

    # ── 1. CommonCrawl — múltiplos índices ────────────────────────────────────
    for index in CC_INDEXES:
        if len(found) >= limit:
            break
        batch = _query_cdx(
            f"https://index.commoncrawl.org/{index}-index",
            label=index,
            limit=limit,
        )
        found.update(batch)
        console.print(f"[dim]  {index}: +{len(batch)} ({len(found)} total)[/dim]")

    # ── 2. Wayback Machine CDX ────────────────────────────────────────────────
    if len(found) < limit:
        wb_domains = _query_wayback(limit - len(found))
        found.update(wb_domains)
        console.print(f"[dim]  Wayback Machine: +{len(wb_domains)} ({len(found)} total)[/dim]")

    # ── 3. myshopify.com → resolver redirect → .com.br ───────────────────────
    if len(found) < limit // 2:
        myshopify = _search_myshopify_br(limit * 2)
        console.print(f"[dim]  myshopify candidatos: {len(myshopify)}[/dim]")
        if myshopify:
            resolved = _resolve_br_only(myshopify)
            found.update(resolved)
            console.print(f"[dim]  myshopify → .com.br: {len(resolved)}[/dim]")

    result = list(found)[:limit]
    console.print(f"[green]✓ CommonCrawl/Wayback: {len(result)} domínios brasileiros[/green]")
    return result


# ── CommonCrawl CDX ───────────────────────────────────────────────────────────

def _query_cdx(cdx_url: str, label: str, limit: int) -> set[str]:
    found = set()
    per_pattern = max(20, limit // len(SHOPIFY_URL_PATTERNS))

    for pattern in SHOPIFY_URL_PATTERNS:
        params = {
            "url": pattern,
            "output": "json",
            "fl": "url",
            "limit": per_pattern,
            "filter": "statuscode:200",
            "collapse": "domain",
        }
        resp = get(cdx_url, params=params, timeout=40)
        if not resp or resp.status_code != 200:
            continue

        for line in resp.text.strip().split("\n"):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
                host = normalize_domain(obj.get("url", ""))
                if host.endswith(".com.br"):
                    found.add(host)
            except Exception:
                continue

    return found


# ── Wayback Machine CDX ───────────────────────────────────────────────────────

def _query_wayback(limit: int) -> set[str]:
    found = set()
    per_pattern = max(20, limit // len(SHOPIFY_URL_PATTERNS))

    for pattern in SHOPIFY_URL_PATTERNS:
        params = {
            "url": pattern,
            "output": "json",
            "fl": "original",
            "limit": per_pattern,
            "filter": "statuscode:200",
            "collapse": "urlkey",
            "matchType": "prefix",
        }
        resp = get(WAYBACK_CDX, params=params, timeout=40)
        if not resp or resp.status_code != 200:
            continue

        lines = resp.text.strip().split("\n")
        # Wayback retorna array JSON ou linhas JSON
        for line in lines:
            line = line.strip().strip("[]").strip(",")
            if not line:
                continue
            try:
                obj = json.loads(line)
                # Pode ser {"original": "..."} ou ["original"]
                if isinstance(obj, dict):
                    url = obj.get("original", "")
                elif isinstance(obj, list) and obj:
                    url = obj[0]
                else:
                    continue
                host = normalize_domain(url)
                if host.endswith(".com.br"):
                    found.add(host)
            except Exception:
                continue

    return found


# ── myshopify → .com.br ───────────────────────────────────────────────────────

def _search_myshopify_br(limit: int) -> list[str]:
    """Busca *.myshopify.com no CommonCrawl, prioriza nomes que parecem lojas BR."""
    cdx_url = f"https://index.commoncrawl.org/{CC_INDEXES[0]}-index"
    params = {
        "url": "*.myshopify.com",
        "output": "json",
        "fl": "url",
        "limit": limit,
        "filter": "statuscode:200",
        "collapse": "urlkey",
    }
    resp = get(cdx_url, params=params, timeout=40)
    if not resp or resp.status_code != 200:
        return []

    found = set()
    # Palavras comuns em nomes de lojas BR
    br_hints = re.compile(
        r"(loja|store|shop|brasil|br|moda|beleza|casa|decor|pet|saude|"
        r"bebe|kids|sport|ativo|natural|arte|farm|grao|bio)", re.I
    )

    for line in resp.text.strip().split("\n"):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            url = obj.get("url", "")
            m = re.search(r"([\w\-]+\.myshopify\.com)", url)
            if m:
                subdomain = m.group(1).lower()
                name = subdomain.replace(".myshopify.com", "")
                # Aceitar qualquer nome com mais de 3 chars
                if len(name) > 3:
                    found.add(subdomain)
        except Exception:
            continue

    return list(found)


def _resolve_br_only(myshopify_domains: list[str]) -> list[str]:
    """Resolve redirects em paralelo, retorna apenas .com.br."""
    br_domains = []

    def resolve(domain: str) -> str | None:
        try:
            resp = requests.head(
                f"https://{domain}",
                timeout=8,
                allow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            host = normalize_domain(resp.url)
            if host.endswith(".com.br") and not is_myshopify(host):
                return host
        except Exception:
            pass
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(resolve, d): d for d in myshopify_domains}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                br_domains.append(result)

    return br_domains
