"""
Cliente HTTP com retry automático, rotação de User-Agent e suporte a Playwright.
"""
import os
import time
import random
import httpx
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from fake_useragent import UserAgent
from rich.console import Console

console = Console()
ua = UserAgent()

TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 15))
DELAY = float(os.getenv("DELAY_BETWEEN_REQUESTS", 1.5))

# Headers base para simular browser real
def _headers() -> dict:
    return {
        "User-Agent": ua.random,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.exceptions.RequestException, httpx.RequestError)),
    reraise=False,
)
def get(url: str, params: dict = None, timeout: int = TIMEOUT) -> requests.Response | None:
    """GET com retry e backoff exponencial. Retorna None em caso de falha."""
    time.sleep(random.uniform(0.5, DELAY))
    try:
        resp = requests.get(
            url,
            params=params,
            headers=_headers(),
            timeout=timeout,
            allow_redirects=True,
        )
        return resp
    except Exception as e:
        console.print(f"[yellow]⚠ HTTP error {url}: {e}[/yellow]")
        return None


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=False,
)
def get_json(url: str, params: dict = None, timeout: int = TIMEOUT) -> dict | None:
    """GET que retorna JSON parseado ou None."""
    resp = get(url, params=params, timeout=timeout)
    if resp and resp.status_code == 200:
        try:
            return resp.json()
        except Exception:
            return None
    return None


def get_with_playwright(url: str, wait_selector: str = "body") -> str | None:
    """
    Fallback: usa Playwright para renderizar páginas com JavaScript pesado.
    Retorna o HTML final ou None.
    """
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(
                user_agent=ua.random,
                extra_http_headers={"Accept-Language": "pt-BR,pt;q=0.9"},
            )
            page.goto(url, timeout=20000, wait_until="domcontentloaded")
            try:
                page.wait_for_selector(wait_selector, timeout=5000)
            except Exception:
                pass
            html = page.content()
            browser.close()
            return html
    except Exception as e:
        console.print(f"[yellow]⚠ Playwright error {url}: {e}[/yellow]")
        return None
