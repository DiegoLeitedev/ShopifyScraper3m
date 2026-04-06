"""
Fonte A — BuiltWith Free API
GET https://api.builtwith.com/free1/api.json?KEY={key}&LOOKUP=shopify.com
Extrai domínios que usam Shopify.
"""
import os
from rich.console import Console
from utils.http import get_json
from utils.helpers import normalize_domain

console = Console()

BUILTWITH_URL = "https://api.builtwith.com/free1/api.json"


def fetch_domains() -> list[str]:
    api_key = os.getenv("BUILTWITH_API_KEY", "")
    if not api_key:
        console.print("[yellow]⚠ BUILTWITH_API_KEY não configurada — pulando Fonte A[/yellow]")
        return []

    console.print("[cyan]→ Fonte A: BuiltWith API...[/cyan]")
    data = get_json(BUILTWITH_URL, params={"KEY": api_key, "LOOKUP": "shopify.com"})
    if not data:
        console.print("[red]✗ BuiltWith: sem resposta[/red]")
        return []

    domains = []
    # A API free retorna estrutura: {"Results": [{"Result": {"Paths": [{"Domain": "..."}]}}]}
    try:
        for result in data.get("Results", []):
            paths = result.get("Result", {}).get("Paths", [])
            for path in paths:
                domain = path.get("Domain", "")
                if domain:
                    domains.append(normalize_domain(domain))
    except Exception as e:
        console.print(f"[red]✗ BuiltWith parse error: {e}[/red]")

    console.print(f"[green]✓ BuiltWith: {len(domains)} domínios[/green]")
    return domains
