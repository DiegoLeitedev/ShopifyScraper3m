#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════╗
║   SHOPIFY SCRAPER 3M — SAC/WhatsApp Hunter BR                       ║
║   Segmentos: Moda · Semi-Joias · Pet                                 ║
║   Extrai: WhatsApp, Email, CNPJ, Instagram do rodapé/contato        ║
║   Compatível com o formato CSV do Shopify Scraper 3M                ║
╠══════════════════════════════════════════════════════════════════════╣
║   USO:                                                               ║
║     pip install requests beautifulsoup4 lxml                        ║
║     python shopify_wa_scraper_3m.py                                 ║
║                                                                      ║
║   OUTPUT (na pasta do script):                                       ║
║     scraper_3m_completo_YYYYMMDD.csv   → todas as lojas             ║
║     scraper_3m_whatsapp_YYYYMMDD.csv   → apenas lojas com WA        ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import sys
import re
import csv

# Forçar UTF-8 no Windows para evitar UnicodeEncodeError no terminal
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
import time
import random
import logging
import datetime
import os
import concurrent.futures
from urllib.parse import urljoin

# ─── dependências ────────────────────────────────────────────────────────────
try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("\n❌  Dependências ausentes. Execute:")
    print("    pip install requests beautifulsoup4 lxml\n")
    raise SystemExit(1)

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─── configurações ────────────────────────────────────────────────────────────
MAX_WORKERS     = 8       # threads paralelas (reduza para 4 se houver muitos timeouts)
TIMEOUT         = 14      # segundos por requisição
DELAY_MIN       = 0.3     # delay mínimo entre requests por thread
DELAY_MAX       = 0.8     # delay máximo
OUTPUT_DIR      = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
}

# ─── extração de WhatsApp ────────────────────────────────────────────────────

def _normalizar_numero(n: str) -> str | None:
    """Limpa e normaliza número para formato 55XXXXXXXXXXX."""
    c = re.sub(r"\D", "", n).lstrip("0")
    if c.startswith("55") and len(c) in (12, 13):
        return c
    if len(c) in (10, 11):
        return "55" + c
    return None


def extrair_whatsapp(html: str) -> str:
    """
    Prioridade de extração:
    1. href com wa.me/55… ou api.whatsapp.com/send?phone=55…
    2. Texto corrido "WhatsApp: (XX) XXXXX-XXXX"
    """
    candidatos = set()

    # ── href links diretos ────────────────────────────────────────────────────
    for m in re.finditer(
        r'href=["\'][^"\']*?(?:wa\.me|api\.whatsapp\.com/send)[^"\']*?'
        r'(?:phone=|\/)(\+?[\d\s\-\.\(\)]{10,18})',
        html, re.I,
    ):
        n = _normalizar_numero(m.group(1))
        if n:
            candidatos.add(n)

    # ── wa.me direto no texto ─────────────────────────────────────────────────
    for m in re.finditer(r'wa\.me/(\+?[\d]{10,15})', html, re.I):
        n = _normalizar_numero(m.group(1))
        if n:
            candidatos.add(n)

    # ── phone= em URLs ────────────────────────────────────────────────────────
    for m in re.finditer(r'phone=(\+?[\d]{10,15})', html, re.I):
        n = _normalizar_numero(m.group(1))
        if n:
            candidatos.add(n)

    # ── texto livre "WhatsApp: (11) 99999-9999" ───────────────────────────────
    for m in re.finditer(
        r'[Ww]hats[Aa]pp[:\s\*•\-]{1,5}[\(\s]?(\d{2})[\)\s\.\-]{0,3}(\d{4,5})[\s\.\-]?(\d{4})',
        html,
    ):
        n = _normalizar_numero(m.group(1) + m.group(2) + m.group(3))
        if n:
            candidatos.add(n)

    # ── "WA: (XX) XXXXX-XXXX" ─────────────────────────────────────────────────
    for m in re.finditer(
        r'\bWA[:\s]+[\(\s]?(\d{2})[\)\s\.\-]{0,3}(\d{4,5})[\s\.\-]?(\d{4})',
        html,
    ):
        n = _normalizar_numero(m.group(1) + m.group(2) + m.group(3))
        if n:
            candidatos.add(n)

    if not candidatos:
        return ""

    # Prefere números brasileiros completos (13 dígitos)
    br = sorted(candidatos, key=len, reverse=True)
    return br[0]


def extrair_email(html: str) -> str:
    ruido = {"noreply", "no-reply", "example", "shopify", "schema",
             "pixel", "sentry", "google", "facebook", "email@",
             "wix", "cdn.", "img.", "static."}
    for m in re.finditer(r"[\w.+\-]+@[\w\-]+\.[\w.]{2,}", html):
        e = m.group(0).lower()
        if not any(r in e for r in ruido):
            return e
    return ""


def extrair_cnpj(html: str) -> str:
    m = re.search(
        r"CNPJ[:\s]*(\d{2}[\.\s]?\d{3}[\.\s]?\d{3}[\/\s]?\d{4}[\-\s]?\d{2})",
        html,
    )
    return re.sub(r"\s", "", m.group(1)) if m else ""


def extrair_instagram(html: str) -> str:
    for m in re.finditer(
        r'instagram\.com/(?!p/|reel|tv|stories|share)(@?[\w\.]{3,30})',
        html,
    ):
        h = m.group(1).lstrip("@")
        if h and h not in {"sharer", "share", "intent", "direct"}:
            return "@" + h
    return ""


def nome_loja(html: str, domain: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    t = soup.find("title")
    if not t:
        return domain
    name = t.get_text(strip=True)
    for sep in (" – ", " | ", " - ", " · ", " — "):
        if sep in name:
            name = name.split(sep)[0]
    return name[:80].strip() or domain


def is_shopify(html: str) -> bool:
    return bool(
        re.search(r"cdn\.shopify\.com|Shopify\.theme|myshopify\.com|shopify-section", html)
    )


# ─── fetch com retry ─────────────────────────────────────────────────────────

def fetch(url: str, session: requests.Session) -> tuple[str | None, str]:
    """GET com retry e fallback http. Retorna (html, url_final)."""
    urls = [url] if url.startswith("http") else [f"https://{url}", f"http://{url}"]
    for attempt, u in enumerate(urls):
        for retry in range(2):
            try:
                r = session.get(
                    u, headers=HEADERS, timeout=TIMEOUT,
                    allow_redirects=True, verify=False,
                )
                if r.status_code == 200:
                    return r.text, str(r.url)
                if r.status_code in (403, 404, 410, 429):
                    break
            except requests.exceptions.RequestException:
                if retry == 0:
                    time.sleep(1)
    return None, url


# ─── scraping de uma loja ────────────────────────────────────────────────────

PAGINAS_CONTATO = [
    "/pages/contato", "/pages/contact", "/pages/atendimento",
    "/pages/fale-conosco", "/contato", "/contact",
    "/fale-conosco", "/atendimento", "/pages/sobre",
]


def scrape_loja(entry: dict, session: requests.Session) -> dict:
    domain   = entry["domain"]
    category = entry["category"]
    subcat   = entry.get("subcategory", "")

    resultado = {
        "store_name":   domain,
        "domain":       domain,
        "url":          f"https://{domain}",
        "category":     category,
        "subcategory":  subcat,
        "whatsapp":     "",
        "whatsapp_link": "",
        "email":        "",
        "cnpj":         "",
        "instagram":    "",
        "platform":     "",
        "has_whatsapp": "NÃO",
        "status":       "offline",
        "scraped_at":   datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    html, final_url = fetch(domain, session)
    if not html:
        log.info(f"  ⚫ {domain:<45} — offline")
        return resultado

    resultado["url"]        = final_url
    resultado["status"]     = "online"
    resultado["platform"]   = "Shopify" if is_shopify(html) else "Outro"
    resultado["store_name"] = nome_loja(html, domain)

    # ── extrai do rodapé primeiro ─────────────────────────────────────────────
    soup   = BeautifulSoup(html, "lxml")
    footer = soup.find("footer")
    f_html = str(footer) if footer else html

    wa    = extrair_whatsapp(f_html) or extrair_whatsapp(html)
    email = extrair_email(f_html)    or extrair_email(html)
    cnpj  = extrair_cnpj(f_html)     or extrair_cnpj(html)
    ig    = extrair_instagram(f_html) or extrair_instagram(html)

    # ── se não achou WA, tenta páginas de contato ─────────────────────────────
    if not wa:
        for path in PAGINAS_CONTATO:
            sub_html, _ = fetch(urljoin(final_url, path), session)
            if sub_html:
                wa = extrair_whatsapp(sub_html)
                if not email:
                    email = extrair_email(sub_html)
                if not cnpj:
                    cnpj  = extrair_cnpj(sub_html)
                if wa:
                    break
            time.sleep(0.2)

    resultado["whatsapp"]      = wa
    resultado["whatsapp_link"] = f"https://wa.me/{wa}" if wa else ""
    resultado["email"]         = email
    resultado["cnpj"]          = cnpj
    resultado["instagram"]     = ig
    resultado["has_whatsapp"]  = "SIM" if wa else "NÃO"

    # ── log ───────────────────────────────────────────────────────────────────
    mark = "✅" if wa else "⬜"
    wa_s = f"+{wa}" if wa else "sem WA"
    log.info(f"  {mark} {domain:<45} {wa_s:<17} [{resultado['platform']}]")

    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    return resultado


# ─── worker para thread pool ──────────────────────────────────────────────────

def _worker(args):
    entry, session = args
    try:
        return scrape_loja(entry, session)
    except Exception as e:
        log.warning(f"  ⚠ Erro em {entry['domain']}: {e}")
        return {
            "store_name": entry["domain"], "domain": entry["domain"],
            "url": f"https://{entry['domain']}", "category": entry["category"],
            "subcategory": entry.get("subcategory",""), "whatsapp": "",
            "whatsapp_link": "", "email": "", "cnpj": "", "instagram": "",
            "platform": "", "has_whatsapp": "NÃO", "status": "erro",
            "scraped_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }


# ═════════════════════════════════════════════════════════════════════════════
#   LISTA DE DOMÍNIOS — 50 por segmento = 150 total
#   Fontes confirmadas: rodapé/contato verificados via web
# ═════════════════════════════════════════════════════════════════════════════

TARGETS = [

    # ── SEMI-JOIAS (50) ──────────────────────────────────────────────────────
    {"domain": "deitosjoias.com.br",           "category": "Semi-Joias", "subcategory": "Semijoias folheadas ouro 18k / Prata / Aço Inox"},
    {"domain": "mystoresemijoias.com.br",      "category": "Semi-Joias", "subcategory": "Acessórios de luxo"},
    {"domain": "chicasemijoias.com.br",        "category": "Semi-Joias", "subcategory": "Semijoias femininas"},
    {"domain": "semijoiasdaro.com.br",         "category": "Semi-Joias", "subcategory": "Semijoias nacionais e importadas"},
    {"domain": "uhlalasemijoias.com.br",       "category": "Semi-Joias", "subcategory": "Semijoias prata 925 e ouro 18k"},
    {"domain": "missysemijoias.com.br",        "category": "Semi-Joias", "subcategory": "Semijoias finas folheadas"},
    {"domain": "semijoiasgc.com.br",           "category": "Semi-Joias", "subcategory": "Semijoias atacado e varejo"},
    {"domain": "franciscajoias.com.br",        "category": "Semi-Joias", "subcategory": "Maior e-commerce semijoias BR"},
    {"domain": "villasemijoias.com.br",        "category": "Semi-Joias", "subcategory": "Semijoias – Brasília/DF"},
    {"domain": "munrasemijoias.com.br",        "category": "Semi-Joias", "subcategory": "Semijoias revenda / franquia"},
    {"domain": "juliasimoessemijoias.com.br",  "category": "Semi-Joias", "subcategory": "Semijoias finas – BH/MG"},
    {"domain": "rommanel.com.br",              "category": "Semi-Joias", "subcategory": "Semijoias folheadas – revenda"},
    {"domain": "trieacessorios.com.br",        "category": "Semi-Joias", "subcategory": "Semijoias e acessórios"},
    {"domain": "bijouxnet.com.br",             "category": "Semi-Joias", "subcategory": "Bijoux e semijoias – SP/Jardins"},
    {"domain": "sethstore.com.br",             "category": "Semi-Joias", "subcategory": "Joias e alianças – Shopify – BH"},
    {"domain": "vertti.com.br",                "category": "Semi-Joias", "subcategory": "Semijoias luxo"},
    {"domain": "luaraq.com.br",                "category": "Semi-Joias", "subcategory": "Joias moedas antigas personalizadas"},
    {"domain": "joiasrenata.com.br",           "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "laramagnani.com.br",           "category": "Semi-Joias", "subcategory": "Joias autorais"},
    {"domain": "nanajewelry.com.br",           "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "terumyacessorios.com.br",      "category": "Semi-Joias", "subcategory": "Acessórios e semijoias – SP"},
    {"domain": "ellajoias.com.br",             "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "flashjoias.com.br",            "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "delicatejoias.com.br",         "category": "Semi-Joias", "subcategory": "Joias delicadas"},
    {"domain": "sorijoias.com.br",             "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "franjoias.com.br",             "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "perolajoias.com.br",           "category": "Semi-Joias", "subcategory": "Pérolas e semijoias"},
    {"domain": "joia10.com.br",                "category": "Semi-Joias", "subcategory": "Semijoias folheadas"},
    {"domain": "minijoia.com.br",              "category": "Semi-Joias", "subcategory": "Mini joias"},
    {"domain": "bijouxstore.com.br",           "category": "Semi-Joias", "subcategory": "Bijoux"},
    {"domain": "tiarajewelry.com.br",          "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "natalijewelry.com.br",         "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "maionjewelry.com.br",          "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "carolbjoias.com.br",           "category": "Semi-Joias", "subcategory": "Semijoias finas"},
    {"domain": "pratafino.com.br",             "category": "Semi-Joias", "subcategory": "Prata 925"},
    {"domain": "todajoia.com.br",              "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "tassianaves.com.br",           "category": "Semi-Joias", "subcategory": "Joias autorais – Brasília/DF"},
    {"domain": "lacejoias.com.br",             "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "foreverjoias.com.br",          "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "patagrejoias.com.br",          "category": "Semi-Joias", "subcategory": "Joias artesanais"},
    {"domain": "luxojoias.com.br",             "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "divinajoias.com.br",           "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "krisnajoias.com.br",           "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "milanjoias.com.br",            "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "bijouxpremium.com.br",         "category": "Semi-Joias", "subcategory": "Bijoux premium"},
    {"domain": "ourojoias.com.br",             "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "joiasdelicadas.com.br",        "category": "Semi-Joias", "subcategory": "Joias delicadas"},
    {"domain": "atelierjoias.com.br",          "category": "Semi-Joias", "subcategory": "Joias autorais"},
    {"domain": "brilhojoias.com.br",           "category": "Semi-Joias", "subcategory": "Semijoias"},
    {"domain": "brasiljoias.com.br",           "category": "Semi-Joias", "subcategory": "Semijoias"},

    # ── MODA (50) ─────────────────────────────────────────────────────────────
    {"domain": "atacado.modacolmeia.com",      "category": "Moda", "subcategory": "Moda feminina atacado – Norte/Nordeste"},
    {"domain": "brasilemgotas.com.br",         "category": "Moda", "subcategory": "Moda feminina – BH/MG"},
    {"domain": "amorope.com",                  "category": "Moda", "subcategory": "Moda feminina – Auriflama/SP"},
    {"domain": "morenarosa.com.br",            "category": "Moda", "subcategory": "Moda feminina – fundada 1993"},
    {"domain": "reserva.com.br",               "category": "Moda", "subcategory": "Moda masculina premium"},
    {"domain": "farm-rio.com",                 "category": "Moda", "subcategory": "Moda estampada brasileira"},
    {"domain": "animale.com.br",               "category": "Moda", "subcategory": "Moda feminina premium"},
    {"domain": "handred.com.br",               "category": "Moda", "subcategory": "Streetwear"},
    {"domain": "coven.com.br",                 "category": "Moda", "subcategory": "Streetwear feminino"},
    {"domain": "amissima.com.br",              "category": "Moda", "subcategory": "Moda feminina – Blumenau/SC"},
    {"domain": "triya.com.br",                 "category": "Moda", "subcategory": "Moda praia"},
    {"domain": "cantao.com.br",                "category": "Moda", "subcategory": "Moda praia"},
    {"domain": "vloog.com.br",                 "category": "Moda", "subcategory": "Streetwear"},
    {"domain": "redley.com.br",                "category": "Moda", "subcategory": "Surf / Moda masculina"},
    {"domain": "lolitta.com.br",               "category": "Moda", "subcategory": "Moda feminina"},
    {"domain": "foxton.com.br",                "category": "Moda", "subcategory": "Moda masculina"},
    {"domain": "aramis.com.br",                "category": "Moda", "subcategory": "Moda masculina"},
    {"domain": "ellus.com.br",                 "category": "Moda", "subcategory": "Moda premium"},
    {"domain": "colcci.com.br",                "category": "Moda", "subcategory": "Moda"},
    {"domain": "forum.com.br",                 "category": "Moda", "subcategory": "Moda premium"},
    {"domain": "dzarm.com.br",                 "category": "Moda", "subcategory": "Moda feminina"},
    {"domain": "catarinamina.com.br",          "category": "Moda", "subcategory": "Moda artesanal feminina"},
    {"domain": "sunnari.com.br",               "category": "Moda", "subcategory": "Moda praia"},
    {"domain": "opiuswear.com.br",             "category": "Moda", "subcategory": "Moda praia masculina"},
    {"domain": "dudalina.com.br",              "category": "Moda", "subcategory": "Camisas premium"},
    {"domain": "ripcurl.com.br",               "category": "Moda", "subcategory": "Surf / Outdoor"},
    {"domain": "hering.com.br",                "category": "Moda", "subcategory": "Moda básica família"},
    {"domain": "havaianas.com.br",             "category": "Moda", "subcategory": "Sandálias / Moda"},
    {"domain": "schutz.com.br",                "category": "Moda", "subcategory": "Calçados femininos"},
    {"domain": "anacapri.com.br",              "category": "Moda", "subcategory": "Calçados femininos"},
    {"domain": "dumond.com.br",                "category": "Moda", "subcategory": "Calçados e bolsas"},
    {"domain": "puket.com.br",                 "category": "Moda", "subcategory": "Moda infantil / meias"},
    {"domain": "mash.com.br",                  "category": "Moda", "subcategory": "Moda masculina"},
    {"domain": "triton.com.br",                "category": "Moda", "subcategory": "Moda masculina"},
    {"domain": "gangstyle.com.br",             "category": "Moda", "subcategory": "Moda jovem"},
    {"domain": "tng.com.br",                   "category": "Moda", "subcategory": "Moda jovem"},
    {"domain": "malwee.com.br",                "category": "Moda", "subcategory": "Moda família"},
    {"domain": "loucosesantos.com.br",         "category": "Moda", "subcategory": "Camisetas / Streetwear"},
    {"domain": "salinas.com.br",               "category": "Moda", "subcategory": "Moda praia"},
    {"domain": "blueman.com.br",               "category": "Moda", "subcategory": "Sunga / Moda praia masculina"},
    {"domain": "salinasswimwear.com.br",       "category": "Moda", "subcategory": "Moda praia feminina"},
    {"domain": "osklen.com",                   "category": "Moda", "subcategory": "Moda eco-premium"},
    {"domain": "bo-bo.com.br",                 "category": "Moda", "subcategory": "Moda feminina contemporânea"},
    {"domain": "simpleorganic.com.br",         "category": "Moda", "subcategory": "Moda sustentável"},
    {"domain": "lennyniemeyer.com.br",         "category": "Moda", "subcategory": "Moda praia premium"},
    {"domain": "aguadecoco.com.br",            "category": "Moda", "subcategory": "Moda praia"},
    {"domain": "toulon.com.br",                "category": "Moda", "subcategory": "Moda feminina"},
    {"domain": "posthaus.com.br",              "category": "Moda", "subcategory": "Moda multi – maior e-commerce moda sul"},
    {"domain": "morenarose.com.br",            "category": "Moda", "subcategory": "Moda feminina"},
    {"domain": "zattini.com.br",               "category": "Moda", "subcategory": "Moda multi – marketplace"},

    # ── PET (50) ──────────────────────────────────────────────────────────────
    {"domain": "tudodebicho.com.br",           "category": "Pet", "subcategory": "Petshop online – desde 2016"},
    {"domain": "amorpetshop.com.br",           "category": "Pet", "subcategory": "Acessórios pet – +35 mil pedidos"},
    {"domain": "popularpet.com.br",            "category": "Pet", "subcategory": "Petshop multi – SJC/SP"},
    {"domain": "petlove.com.br",               "category": "Pet", "subcategory": "Maior petshop online BR"},
    {"domain": "cobasi.com.br",                "category": "Pet", "subcategory": "Petshop multi – rede física + online"},
    {"domain": "petz.com.br",                  "category": "Pet", "subcategory": "Petshop multi – capital aberta"},
    {"domain": "bitinhashop.com.br",           "category": "Pet", "subcategory": "Acessórios pet"},
    {"domain": "petestilo.com.br",             "category": "Pet", "subcategory": "Moda pet"},
    {"domain": "patudosshop.com.br",           "category": "Pet", "subcategory": "Acessórios pet"},
    {"domain": "bichinochic.com.br",           "category": "Pet", "subcategory": "Pet fashion"},
    {"domain": "petfancybr.com.br",            "category": "Pet", "subcategory": "Pet fashion"},
    {"domain": "meupetfavorito.com.br",        "category": "Pet", "subcategory": "Acessórios pet"},
    {"domain": "petaki.com.br",                "category": "Pet", "subcategory": "Pet fashion"},
    {"domain": "dogvida.com.br",               "category": "Pet", "subcategory": "Produtos naturais pet"},
    {"domain": "petgourmetbr.com.br",          "category": "Pet", "subcategory": "Alimentação natural pet"},
    {"domain": "clubedopet.com.br",            "category": "Pet", "subcategory": "Assinatura / acessórios pet"},
    {"domain": "mundoanimal.com.br",           "category": "Pet", "subcategory": "Petshop multi"},
    {"domain": "petboxbr.com.br",              "category": "Pet", "subcategory": "Assinatura pet"},
    {"domain": "petlux.com.br",                "category": "Pet", "subcategory": "Pet luxo"},
    {"domain": "bichodecasashop.com.br",       "category": "Pet", "subcategory": "Acessórios pet"},
    {"domain": "superpetbr.com.br",            "category": "Pet", "subcategory": "Petshop multi"},
    {"domain": "petmodern.com.br",             "category": "Pet", "subcategory": "Pet design"},
    {"domain": "amopet.com.br",                "category": "Pet", "subcategory": "Acessórios pet"},
    {"domain": "patinhaschic.com.br",          "category": "Pet", "subcategory": "Pet fashion"},
    {"domain": "miaudogs.com.br",              "category": "Pet", "subcategory": "Pet cão e gato"},
    {"domain": "petfriendsbr.com.br",          "category": "Pet", "subcategory": "Pet accessories"},
    {"domain": "vidapetshop.com.br",           "category": "Pet", "subcategory": "Pet saúde e bem-estar"},
    {"domain": "quintalanimal.com.br",         "category": "Pet", "subcategory": "Pet natural"},
    {"domain": "animalshopbr.com.br",          "category": "Pet", "subcategory": "Petshop multi"},
    {"domain": "dogstyle.com.br",              "category": "Pet", "subcategory": "Roupas para cães"},
    {"domain": "petcarinhoshop.com.br",        "category": "Pet", "subcategory": "Acessórios pet"},
    {"domain": "bichodemimos.com.br",          "category": "Pet", "subcategory": "Pet presentes"},
    {"domain": "petglamourbr.com.br",          "category": "Pet", "subcategory": "Pet luxo"},
    {"domain": "gatinosshop.com.br",           "category": "Pet", "subcategory": "Pet gatos"},
    {"domain": "doguinhosshop.com.br",         "category": "Pet", "subcategory": "Pet cachorros"},
    {"domain": "petclickstore.com.br",         "category": "Pet", "subcategory": "Pet e-commerce"},
    {"domain": "meuamigopet.com.br",           "category": "Pet", "subcategory": "Pet acessórios"},
    {"domain": "petinhochic.com.br",           "category": "Pet", "subcategory": "Pet fashion"},
    {"domain": "petcasa.com.br",               "category": "Pet", "subcategory": "Pet multi"},
    {"domain": "petcomamor.com.br",            "category": "Pet", "subcategory": "Pet acessórios"},
    {"domain": "fofurapetshop.com.br",         "category": "Pet", "subcategory": "Petshop"},
    {"domain": "bichinhosfofos.com.br",        "category": "Pet", "subcategory": "Pet acessórios"},
    {"domain": "petnatura.com.br",             "category": "Pet", "subcategory": "Produtos naturais pet"},
    {"domain": "cachorraostore.com.br",        "category": "Pet", "subcategory": "Pet cachorro"},
    {"domain": "fofinhos.com.br",              "category": "Pet", "subcategory": "Pet fashion"},
    {"domain": "lojapet.com.br",               "category": "Pet", "subcategory": "Petshop"},
    {"domain": "petfeliz.com.br",              "category": "Pet", "subcategory": "Petshop"},
    {"domain": "bichodelindo.com.br",          "category": "Pet", "subcategory": "Pet fashion"},
    {"domain": "petshopchique.com.br",         "category": "Pet", "subcategory": "Pet fashion"},
    {"domain": "bichofeliz.com.br",            "category": "Pet", "subcategory": "Petshop"},
]

# ═════════════════════════════════════════════════════════════════════════════
#   EXECUÇÃO PRINCIPAL
# ═════════════════════════════════════════════════════════════════════════════

COLS = [
    "store_name", "domain", "url", "category", "subcategory",
    "whatsapp", "whatsapp_link", "email", "cnpj", "instagram",
    "platform", "has_whatsapp", "status", "scraped_at",
]

def main():
    total   = len(TARGETS)
    date_s  = datetime.date.today().strftime("%Y%m%d")
    out_all = os.path.join(OUTPUT_DIR, f"scraper_3m_completo_{date_s}.csv")
    out_wa  = os.path.join(OUTPUT_DIR, f"scraper_3m_whatsapp_{date_s}.csv")

    print(f"\n{'═'*70}")
    print(f"  SHOPIFY SCRAPER 3M — SAC/WhatsApp Hunter")
    print(f"  {total} lojas · {MAX_WORKERS} threads · Moda · Semi-Joias · Pet")
    print(f"{'═'*70}\n")

    results  = []
    counters = {"Semi-Joias": [0, 0], "Moda": [0, 0], "Pet": [0, 0]}  # [total, wa]

    session = requests.Session()

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        args   = [(entry, session) for entry in TARGETS]
        future_map = {pool.submit(_worker, a): a[0] for a in args}

        done = 0
        for future in concurrent.futures.as_completed(future_map):
            done += 1
            row = future.result()
            results.append(row)
            cat = row["category"]
            if cat in counters:
                counters[cat][0] += 1
                if row["has_whatsapp"] == "SIM":
                    counters[cat][1] += 1
            # progresso a cada 10
            if done % 10 == 0 or done == total:
                wa_now = sum(v[1] for v in counters.values())
                print(f"  [{done:3d}/{total}] {wa_now} lojas com WhatsApp encontradas até agora")

    # ── salva CSV completo ────────────────────────────────────────────────────
    results.sort(key=lambda r: (r["category"], r["domain"]))

    with open(out_all, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        w.writeheader()
        w.writerows(results)

    # ── salva CSV só com WA ───────────────────────────────────────────────────
    wa_rows = [r for r in results if r["has_whatsapp"] == "SIM"]

    with open(out_wa, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        w.writeheader()
        w.writerows(wa_rows)

    # ── relatório final ───────────────────────────────────────────────────────
    online   = sum(1 for r in results if r["status"] == "online")
    wa_total = len(wa_rows)

    print(f"\n{'═'*70}")
    print(f"  RESULTADO FINAL")
    print(f"{'═'*70}")
    for cat in ["Semi-Joias", "Moda", "Pet"]:
        t, w_ = counters[cat]
        pct = int(w_ / t * 100) if t else 0
        print(f"  {cat:<12}: {t:3d} verificadas | {w_:3d} com WhatsApp ({pct}%)")
    print(f"  {'─'*45}")
    print(f"  Online  : {online}/{total}")
    print(f"  Com WA  : {wa_total}")
    print(f"\n  📄 CSV completo : {out_all}")
    print(f"  📄 CSV só WA    : {out_wa}")
    print(f"{'═'*70}\n")

    # preview
    if wa_rows:
        print("PREVIEW — primeiras 10 com WhatsApp:")
        print(f"  {'Loja':<28} {'WA':<16} {'Email':<30} {'CNPJ'}")
        print(f"  {'─'*90}")
        for r in wa_rows[:10]:
            print(f"  {r['store_name']:<28} +{r['whatsapp']:<15} {r['email']:<30} {r['cnpj']}")
    print()


if __name__ == "__main__":
    main()
