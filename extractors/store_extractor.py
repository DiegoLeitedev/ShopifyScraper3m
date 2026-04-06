"""
Extrator de dados de cada loja Shopify ativa.

Campos extraídos:
  [1]  dominio          — URL principal
  [2]  nome_loja        — og:site_name ou <title>
  [3]  email            — e-mails encontrados na página de contato / rodapé
  [4]  telefone         — telefones brasileiros
  [5]  whatsapp         — link wa.me ou menção a WhatsApp
  [6]  instagram        — handle @instagram
  [7]  cnpj             — CNPJ extraído do rodapé / página de contato
  [8]  razao_social     — razão social próxima ao CNPJ
  [9]  endereco         — endereço físico
  [10] cidade           — cidade
  [11] estado           — UF
  [12] produtos_count   — quantidade de produtos (/products.json count)
  [13] tem_blog         — True/False
  [14] status           — ativo | inativo | erro
  [15] data_coleta      — timestamp ISO
"""
import re
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from utils.http import get, get_json, get_with_playwright
from utils.helpers import (
    extract_cnpj, extract_emails, extract_phones,
    extract_instagram, extract_whatsapp,
)


# ── Páginas onde dados de contato costumam aparecer ──────────────────────────
CONTACT_PATHS = ["/pages/contato", "/pages/contact", "/pages/sobre",
                 "/pages/about", "/pages/quem-somos", "/contact"]
POLICY_PATHS  = ["/policies/refund-policy", "/policies/privacy-policy",
                 "/pages/politica-de-privacidade"]


def extract_store(domain: str, platform: str = "shopify") -> dict:
    base_url = f"https://{domain}"
    record = {
        "dominio": domain,
        "plataforma": platform,
        "nome_loja": None,
        "email": None,
        "telefone": None,
        "whatsapp": None,
        "instagram": None,
        "cnpj": None,
        "razao_social": None,
        "endereco": None,
        "cidade": None,
        "estado": None,
        "produtos_count": None,
        "tem_blog": False,
        "status": "ativo",
        "data_coleta": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    # ── Homepage ─────────────────────────────────────────────────────────────
    resp = get(base_url)
    if not resp or resp.status_code != 200:
        # Tentar Playwright como fallback
        html = get_with_playwright(base_url)
        if not html:
            record["status"] = "erro"
            return record
    else:
        html = resp.text

    soup = BeautifulSoup(html, "lxml")
    full_text = soup.get_text(" ", strip=True)

    # Nome da loja
    og_name = soup.find("meta", property="og:site_name")
    record["nome_loja"] = (og_name["content"].strip() if og_name and og_name.get("content")
                           else (soup.title.string.strip() if soup.title else None))

    # Dados de contato na homepage
    _fill_contact(record, full_text)

    # ── Páginas de contato ────────────────────────────────────────────────────
    for path in CONTACT_PATHS:
        if _all_filled(record):
            break
        r = get(base_url + path)
        if r and r.status_code == 200:
            page_soup = BeautifulSoup(r.text, "lxml")
            _fill_contact(record, page_soup.get_text(" ", strip=True))

    # ── Políticas (CNPJ geralmente aparece aqui) ──────────────────────────────
    if not record["cnpj"]:
        for path in POLICY_PATHS:
            r = get(base_url + path)
            if r and r.status_code == 200:
                text = BeautifulSoup(r.text, "lxml").get_text(" ", strip=True)
                cnpj = extract_cnpj(text)
                if cnpj:
                    record["cnpj"] = cnpj
                    record["razao_social"] = _extract_razao_social(text, cnpj)
                    _fill_address(record, text)
                    break

    # ── Contagem de produtos (por plataforma) ────────────────────────────────
    if platform == "shopify":
        prod_data = get_json(f"{base_url}/products.json?limit=250")
        if prod_data and "products" in prod_data:
            record["produtos_count"] = len(prod_data["products"])
    elif platform == "nuvemshop":
        prod_data = get_json(f"{base_url}/api/v1/products.json")
        if isinstance(prod_data, list):
            record["produtos_count"] = len(prod_data)
    # Tray e Loja Integrada não expõem API pública de produtos

    # ── Verificar blog ────────────────────────────────────────────────────────
    blog_path = "/blogs" if platform == "shopify" else "/blog"
    blog_resp = get(f"{base_url}{blog_path}")
    record["tem_blog"] = bool(blog_resp and blog_resp.status_code == 200
                              and "blog" in blog_resp.text.lower())

    return record


# ── Helpers internos ──────────────────────────────────────────────────────────

def _fill_contact(record: dict, text: str):
    if not record["email"]:
        emails = extract_emails(text)
        if emails:
            record["email"] = emails[0]

    if not record["telefone"]:
        phones = extract_phones(text)
        if phones:
            record["telefone"] = phones[0]

    if not record["whatsapp"]:
        record["whatsapp"] = extract_whatsapp(text)

    if not record["instagram"]:
        record["instagram"] = extract_instagram(text)

    if not record["cnpj"]:
        cnpj = extract_cnpj(text)
        if cnpj:
            record["cnpj"] = cnpj
            record["razao_social"] = _extract_razao_social(text, cnpj)
            _fill_address(record, text)


def _fill_address(record: dict, text: str):
    if record.get("endereco"):
        return
    # Busca padrão: Rua/Av + número + complemento
    match = re.search(
        r"((?:Rua|Av(?:enida)?|Al(?:ameda)?|Rod(?:ovia)?|Estr(?:ada)?|Pça|Praça)"
        r"[\w\s,\.ºª\-]{5,80}(?:CEP[\s:]?\d{5}-?\d{3})?)",
        text, re.IGNORECASE
    )
    if match:
        record["endereco"] = match.group(1).strip()

    # UF — sigla de 2 letras dos estados brasileiros
    uf_match = re.search(
        r"\b(AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)\b",
        text
    )
    if uf_match:
        record["estado"] = uf_match.group(1)

    # Cidade — tentativa simples via CEP lookup omitida para não depender de API externa
    city_match = re.search(r"[-–,]\s*([A-ZÁÉÍÓÚÂÊÔÃÕÇ][a-záéíóúâêôãõç]{2,})\s*/?\s*[A-Z]{2}\b", text)
    if city_match:
        record["cidade"] = city_match.group(1)


def _extract_razao_social(text: str, cnpj: str) -> str | None:
    """Tenta extrair razão social próxima ao CNPJ."""
    idx = text.find(cnpj)
    if idx == -1:
        return None
    # Buscar padrão LTDA / ME / EIRELI / SA nas proximidades
    snippet = text[max(0, idx - 200): idx + 50]
    match = re.search(
        r"([A-ZÁÉÍÓÚÂÊÔÃÕÇ][A-Za-záéíóúâêôãõç\s&\.]{3,60}"
        r"(?:LTDA|EIRELI|ME|EPP|S/?A|SS|COMERCIO|COMERCIAL|SERVICOS|TECNOLOGIA)[\.\,]?)",
        snippet, re.IGNORECASE
    )
    return match.group(1).strip() if match else None


def _all_filled(record: dict) -> bool:
    """Verifica se os campos principais já foram preenchidos."""
    return all([
        record["email"], record["telefone"],
        record["cnpj"], record["instagram"],
    ])
