"""
Funções auxiliares: normalização de domínios, deduplicação, validação de CNPJ.
"""
import re
from urllib.parse import urlparse


def normalize_domain(raw: str) -> str:
    """Remove schema, www, path — retorna só o host limpo."""
    raw = raw.strip().lower()
    if not raw.startswith("http"):
        raw = "https://" + raw
    parsed = urlparse(raw)
    host = parsed.netloc or parsed.path
    host = re.sub(r"^www\.", "", host)
    host = host.split("/")[0].split("?")[0].split("#")[0]
    return host


def is_myshopify(domain: str) -> bool:
    return domain.endswith(".myshopify.com")


def dedupe_domains(domains: list[str]) -> list[str]:
    seen = set()
    result = []
    for d in domains:
        n = normalize_domain(d)
        if n and n not in seen:
            seen.add(n)
            result.append(n)
    return result


# Regex para CNPJ no formato XX.XXX.XXX/XXXX-XX ou apenas 14 dígitos
_CNPJ_RE = re.compile(
    r"\b(\d{2}[\.\-]?\d{3}[\.\-]?\d{3}[\/\-]?\d{4}[\.\-]?\d{2})\b"
)

def extract_cnpj(text: str) -> str | None:
    """Retorna o primeiro CNPJ encontrado no texto, formatado."""
    match = _CNPJ_RE.search(text)
    if not match:
        return None
    raw = re.sub(r"[^\d]", "", match.group(1))
    if len(raw) == 14:
        return f"{raw[:2]}.{raw[2:5]}.{raw[5:8]}/{raw[8:12]}-{raw[12:]}"
    return None


def extract_emails(text: str) -> list[str]:
    """Retorna todos os e-mails únicos encontrados no texto."""
    pattern = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
    emails = pattern.findall(text)
    # Filtrar domínios de imagem e e-mails genéricos de exemplo
    blocked = {"example.com", "domain.com", "email.com", "seusite.com.br"}
    return list({e.lower() for e in emails if e.split("@")[-1] not in blocked})


def extract_phones(text: str) -> list[str]:
    """Retorna telefones brasileiros encontrados no texto."""
    pattern = re.compile(
        r"(?:\+55[\s\-]?)?(?:\(?\d{2}\)?[\s\-]?)(?:9\s?\d{4}|\d{4})[\s\-]?\d{4}"
    )
    phones = pattern.findall(text)
    return list({re.sub(r"\s+", " ", p).strip() for p in phones})


def extract_instagram(text: str) -> str | None:
    pattern = re.compile(
        r"(?:instagram\.com/|@)([\w\.]+)", re.IGNORECASE
    )
    m = pattern.search(text)
    if m:
        handle = m.group(1).rstrip("/")
        if handle.lower() not in ("p", "explore", "accounts", "stories"):
            return f"@{handle}"
    return None


def extract_whatsapp(text: str) -> str | None:
    """
    Extrai número de WhatsApp de diversas fontes:
    - Links wa.me/55XXXXXXXXXXX
    - api.whatsapp.com/send?phone=
    - texto com "whatsapp" seguido de número
    - padrão (XX) 9XXXX-XXXX próximo à palavra whatsapp
    """
    # 1. wa.me link — formato mais confiável
    m = re.search(r"wa\.me/(\d{10,13})", text, re.IGNORECASE)
    if m:
        return _fmt_whatsapp(m.group(1))

    # 2. api.whatsapp.com/send?phone=
    m = re.search(r"api\.whatsapp\.com/send[^\d]{0,10}phone=(\d{10,13})", text, re.IGNORECASE)
    if m:
        return _fmt_whatsapp(m.group(1))

    # 3. "whatsapp" seguido de número em até 40 chars
    m = re.search(r"whatsapp[^<\n]{0,40}?(\(?\d{2}\)?\s*9?\s*\d{4}[\s\-]?\d{4})", text, re.IGNORECASE)
    if m:
        num = re.sub(r"\D", "", m.group(1))
        if len(num) >= 10:
            return _fmt_whatsapp(num)

    # 4. Número seguido de texto "whatsapp" em até 40 chars
    m = re.search(r"(\(?\d{2}\)?\s*9?\s*\d{4}[\s\-]?\d{4})[^<\n]{0,40}?whatsapp", text, re.IGNORECASE)
    if m:
        num = re.sub(r"\D", "", m.group(1))
        if len(num) >= 10:
            return _fmt_whatsapp(num)

    return None


def _fmt_whatsapp(digits: str) -> str:
    """Formata número para (XX) XXXXX-XXXX, removendo código de país 55."""
    d = re.sub(r"\D", "", digits)
    if d.startswith("55") and len(d) >= 12:
        d = d[2:]
    if len(d) == 11:
        return f"({d[:2]}) {d[2:7]}-{d[7:]}"
    if len(d) == 10:
        return f"({d[:2]}) {d[2:6]}-{d[6:]}"
    return d
