"""
Fonte C — Fingerprint multi-plataforma
Detecta qual plataforma de e-commerce cada domínio usa:
  - Shopify      → /products.json com array "products"
  - Tray         → CDN tray ou meta generator
  - Nuvemshop    → CDN tiendanube/nuvemshop ou API
  - Loja Integrada → CDN lojaintegrada.com.br
  - VTEX         → CDN vtexassets ou myvtex.com
"""
import os
import time
import random
import concurrent.futures
import requests
from rich.console import Console

console = Console()

MAX_WORKERS = int(os.getenv("MAX_WORKERS", 5))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/json,*/*",
    "Accept-Language": "pt-BR,pt;q=0.9",
}

# Assinaturas de plataforma buscadas no HTML da homepage
PLATFORM_SIGNATURES = {
    "shopify": [
        "cdn.shopify.com",
        "shopify.com/s/files",
        'content="Shopify"',
        "Shopify.theme",
        "/cart.js",
    ],
    "tray": [
        "cdn.traycheckout.com.br",
        "tray.com.br",
        "static.tray.com.br",
        "loja.tray.com.br",
        'generator" content="Tray',
    ],
    "nuvemshop": [
        "d26lpennugtm8s.cloudfront.net",
        "tiendanube.com",
        "nuvemshop.com.br",
        "cdn.nuvemshop.com.br",
        'generator" content="Tiendanube',
        "LS.defined",
    ],
    "lojaintegrada": [
        "static.lojaintegrada.com.br",
        "lojaintegrada.com.br",
        "integracao.lojaintegrada",
        'generator" content="Loja Integrada',
    ],
    "vtex": [
        "vtexassets.com",
        "myvtex.com",
        "vtex.com",
        "vtexcommercestable",
        "vteximg.com.br",
    ],
    "woocommerce": [
        "wp-content/plugins/woocommerce",
        "woocommerce",
        "wp-json/wc/",
    ],
}


def detect_platform(domain: str) -> str | None:
    """
    Detecta a plataforma de e-commerce de um domínio.
    Retorna: 'shopify' | 'tray' | 'nuvemshop' | 'lojaintegrada' | 'vtex' | 'woocommerce' | None
    """
    time.sleep(random.uniform(0.2, 0.6))

    html = ""
    try:
        r = requests.get(
            f"https://{domain}",
            headers=HEADERS,
            timeout=12,
            allow_redirects=True,
            verify=False,
        )
        if r.status_code == 200:
            html = r.text
    except Exception:
        return None

    if not html:
        return None

    html_lower = html.lower()

    # Shopify: também testa /products.json (mais confiável)
    if any(sig.lower() in html_lower for sig in PLATFORM_SIGNATURES["shopify"]):
        return "shopify"

    try:
        rp = requests.get(
            f"https://{domain}/products.json?limit=1",
            headers=HEADERS,
            timeout=8,
            allow_redirects=True,
            verify=False,
        )
        if rp.status_code == 200 and '"products"' in rp.text:
            return "shopify"
    except Exception:
        pass

    # Demais plataformas
    for platform, sigs in PLATFORM_SIGNATURES.items():
        if platform == "shopify":
            continue
        if any(sig.lower() in html_lower for sig in sigs):
            return platform

    return None


def filter_active(domains: list[str], progress=None, task_id=None) -> list[tuple[str, str]]:
    """
    Retorna lista de (dominio, plataforma) para lojas ativas detectadas.
    """
    active = []

    def check(domain: str):
        platform = detect_platform(domain)
        if progress and task_id is not None:
            progress.advance(task_id)
        return (domain, platform) if platform else None

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(check, d): d for d in domains}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                active.append(result)

    return active
