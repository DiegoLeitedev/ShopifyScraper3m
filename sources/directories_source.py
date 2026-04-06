"""
Fonte E — Diretórios e agregadores brasileiros

Todas as fontes usam Playwright (sites JS-heavy que bloqueiam requests diretos).
Fontes confirmadas funcionando:
  1. Promobit       — 729+ lojas via __NEXT_DATA__ (pageFrom = domínio)
  2. Pelando        — lista de merchants
  3. Cuponomia      — diretório de cupons BR
  4. Afilio         — rede de afiliados BR
  5. Seeds estáticos — lojas Shopify BR conhecidas como ponto de partida
"""
import re
import json
import time
from bs4 import BeautifulSoup
from rich.console import Console
from utils.http import get_with_playwright, get
from utils.helpers import normalize_domain

console = Console()


def fetch_domains() -> list[str]:
    console.print("[cyan]→ Fonte E: Diretórios BR...[/cyan]")

    domains = set()

    scrapers = [
        ("Promobit",   _promobit),
        ("Pelando",    _pelando),
        ("Cuponomia",  _cuponomia),
        ("Afilio",     _afilio),
        ("Lomadee",    _lomadee),
        ("Seeds BR",   _seeds),
    ]

    for name, fn in scrapers:
        try:
            batch = fn()
            before = len(domains)
            domains.update(batch)
            added = len(domains) - before
            console.print(f"[dim]  {name}: {len(batch)} coletados, +{added} novos ({len(domains)} total)[/dim]")
        except Exception as e:
            console.print(f"[yellow]  {name}: erro ({e})[/yellow]")
        time.sleep(1)

    result = list(domains)
    console.print(f"[green]✓ Diretórios BR: {len(result)} domínios[/green]")
    return result


# ── Promobit — __NEXT_DATA__ com 729 lojas ────────────────────────────────────

def _promobit() -> list[str]:
    """
    Extrai campo `pageFrom` do __NEXT_DATA__ da página de lojas do Promobit.
    Confirmado: retorna 729+ lojas com domínio direto.
    """
    html = get_with_playwright("https://www.promobit.com.br/lojas/", "script#__NEXT_DATA__")
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    ndata = soup.find("script", id="__NEXT_DATA__")
    if not ndata:
        return []

    try:
        data = json.loads(ndata.string)
        stores = data["props"]["pageProps"]["stores"]
        domains = []
        for store in stores:
            domain = store.get("pageFrom", "")
            if domain:
                d = normalize_domain("https://" + domain if not domain.startswith("http") else domain)
                if d:
                    domains.append(d)
        return domains
    except Exception as e:
        console.print(f"[yellow]  Promobit parse error: {e}[/yellow]")
        return []


# ── Pelando ───────────────────────────────────────────────────────────────────

def _pelando() -> list[str]:
    """Extrai lojas da página de merchants do Pelando."""
    html = get_with_playwright("https://www.pelando.com.br/lojas", "a[href]")
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    domains = set()

    # Tentar __NEXT_DATA__
    ndata = soup.find("script", id="__NEXT_DATA__")
    if ndata:
        try:
            data = json.loads(ndata.string)
            pp = data.get("props", {}).get("pageProps", {})
            # Percorrer qualquer lista com campo url/website/domain
            for v in pp.values():
                if isinstance(v, list):
                    for item in v:
                        if isinstance(item, dict):
                            url = item.get("websiteUrl") or item.get("url") or item.get("website") or ""
                            if url:
                                d = normalize_domain(url)
                                if d and ".com" in d:
                                    domains.add(d)
        except Exception:
            pass

    # Fallback: links externos na página
    if not domains:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and "pelando" not in href and ".com" in href:
                d = normalize_domain(href)
                if d and ".com" in d:
                    domains.add(d)

    return list(domains)


# ── Cuponomia ─────────────────────────────────────────────────────────────────

def _cuponomia() -> list[str]:
    """Extrai lojas do diretório Cuponomia."""
    html = get_with_playwright("https://www.cuponomia.com.br/lojas/", "a[href*='/loja/']")
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    domains = set()

    # Links de lojas individuais — slug /loja/NOME
    loja_slugs = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"/loja/([\w\-]+)/?$", href)
        if m:
            loja_slugs.add(m.group(1))

    # Para cada slug, buscar o domínio externo na página da loja
    for slug in list(loja_slugs)[:50]:  # limitar para não demorar demais
        store_html = get_with_playwright(
            f"https://www.cuponomia.com.br/loja/{slug}/",
            "a[rel='nofollow noopener'],a[target='_blank']",
        )
        if not store_html:
            continue
        store_soup = BeautifulSoup(store_html, "lxml")
        # Link "Ir para a loja" costuma ter rel=nofollow
        for a in store_soup.find_all("a", href=True):
            href = a["href"]
            if (href.startswith("http")
                    and "cuponomia" not in href
                    and ".com" in href):
                d = normalize_domain(href)
                if d and ".com" in d:
                    domains.add(d)
                    break
        time.sleep(0.5)

    return list(domains)


# ── Afilio ────────────────────────────────────────────────────────────────────

def _afilio() -> list[str]:
    """Extrai lojas da rede de afiliados Afilio."""
    html = get_with_playwright(
        "https://www.afilio.com.br/lojas-parceiras/",
        "a[href*='.com.br']",
    )
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    domains = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("http") and "afilio" not in href and ".com.br" in href:
            d = normalize_domain(href)
            if d.endswith(".com.br"):
                domains.add(d)

    return list(domains)


# ── Lomadee — rede de afiliados com 500+ lojas BR ────────────────────────────

def _lomadee() -> list[str]:
    """Extrai lojas da rede de afiliados Lomadee (Buscapé group)."""
    html = get_with_playwright(
        "https://www.lomadee.com/lojas/",
        "a[href*='.com.br']",
    )
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    domains = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("http") and "lomadee" not in href and ".com.br" in href:
            d = normalize_domain(href)
            if d.endswith(".com.br") and "." in d:
                domains.add(d)

    # Tentar __NEXT_DATA__ também
    ndata = soup.find("script", id="__NEXT_DATA__")
    if ndata:
        try:
            data = json.loads(ndata.string)
            def _walk(obj):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if k in ("url", "website", "domain", "storeUrl") and isinstance(v, str):
                            d = normalize_domain(v)
                            if d.endswith(".com.br"):
                                domains.add(d)
                        else:
                            _walk(v)
                elif isinstance(obj, list):
                    for item in obj:
                        _walk(item)
            _walk(data)
        except Exception:
            pass

    return list(domains)


# ── Seeds estáticos — lojas Shopify BR conhecidas ────────────────────────────

def _seeds() -> list[str]:
    """
    Lista de lojas Shopify BR conhecidas como ponto de partida.
    O fingerprint (Fonte C) confirma quais ainda estão ativas.
    """
    return [
        # ── Moda / Vestuário (grandes) ─────────────────────────────────────────
        "www.tanlup.com.br",
        "www.reserva.ink",
        "www.toulon.com.br",
        "www.timberland.com.br",
        "www.lacoste.com.br",
        "www.quiksilver.com.br",
        "www.tng.com.br",
        "www.redley.com.br",
        "www.foxton.com.br",
        "www.ellus.com.br",
        "www.colcci.com.br",
        "www.oqvestir.com.br",
        "www.morena-rosa.com.br",
        "www.triton.com.br",
        "www.vix.com.br",
        # ── Moda / Vestuário (médias — mais WhatsApp) ─────────────────────────
        "www.dafiti.com.br",
        "www.youcom.com.br",
        "www.shoulder.com.br",
        "www.animale.com.br",
        "www.farm-rio.com",
        "www.mixed.com.br",
        "www.amaro.com",
        "www.zattini.com.br",
        "www.cea.com.br",
        "www.hering.com.br",
        "www.bobstore.com.br",
        "www.cantaograma.com.br",
        "www.dudalina.com.br",
        "www.zinzane.com.br",
        "www.iodice.com.br",
        "www.lolitta.com.br",
        "www.alba-rosa.com.br",
        "www.shop2gether.com.br",
        "www.gallerist.com.br",
        "www.haight.com.br",
        "www.insecta.shoes",
        "www.ahlens.com.br",
        "www.haveaianas.com.br",
        "www.goexplore.com.br",
        # ── Calçados ───────────────────────────────────────────────────────────
        "www.dmstore.com.br",
        "www.vans.com.br",
        "www.birkenstock.com.br",
        "www.osklen.com.br",
        "www.cariuma.com.br",
        "www.vizzano.com.br",
        "www.piccadilly.com.br",
        "www.usaflex.com.br",
        "www.democrata.com.br",
        "www.beira-rio.com.br",
        "www.moleca.com.br",
        "www.crocs.com.br",
        # ── Beleza / Cosméticos ────────────────────────────────────────────────
        "www.onofre.com.br",
        "www.nativaspa.com.br",
        "www.vult.com.br",
        "www.tracta.com.br",
        "www.usebeauty.com.br",
        "www.quem-disse-berenice.com.br",
        "www.souvie.com.br",
        "www.usecantu.com.br",
        "www.eudora.com.br",
        "www.o-boticario.com.br",
        "www.avon.com.br",
        "www.granado.com.br",
        "www.davines.com.br",
        "www.depil-ok.com.br",
        "www.lojamagia.com.br",
        "www.beautybox.com.br",
        "www.mercadinha-dri.com.br",
        # ── Casa / Decoração ───────────────────────────────────────────────────
        "www.etna.com.br",
        "www.area-h.com.br",
        "www.camicado.com.br",
        "www.actualdesign.com.br",
        "www.westwing.com.br",
        "www.mobly.com.br",
        "www.leroy-merlin.com.br",
        "www.telhanorte.com.br",
        "www.dicasa.com.br",
        "www.tok-stok.com.br",
        "www.etna.com.br",
        "www.betel.com.br",
        "www.decostore.com.br",
        "www.arquiteturaeinteriores.com.br",
        # ── Eletrônicos / Tech ─────────────────────────────────────────────────
        "www.supernosso.com.br",
        "www.shopclub.com.br",
        "www.kabum.com.br",
        "www.terabyteshop.com.br",
        "www.pichau.com.br",
        "www.gkinformatica.com.br",
        # ── Esportes / Outdoor ─────────────────────────────────────────────────
        "www.descente.com.br",
        "www.olympikus.com.br",
        "www.penalty.com.br",
        "www.insports.com.br",
        "www.sportsland.com.br",
        "www.corridaecia.com.br",
        "www.run4fun.com.br",
        "www.mizuno.com.br",
        "www.asics.com.br",
        "www.speedo.com.br",
        "www.gonew.com.br",
        "www.poker.com.br",
        # ── Acessórios / Joias ─────────────────────────────────────────────────
        "www.vivara.com.br",
        "www.pandora.com.br",
        "www.balaclava.com.br",
        "www.rommanel.com.br",
        "www.life-joias.com.br",
        "www.bergerson.com.br",
        "www.jequiti.com.br",
        "www.meyersjoias.com.br",
        "www.lacredor.com.br",
        # ── Alimentação / Gourmet / Suplementos ────────────────────────────────
        "www.mundo-gourmet.com.br",
        "www.qualityfood.com.br",
        "www.integralmedica.com.br",
        "www.growth.com.br",
        "www.probiotica.com.br",
        "www.max-titanium.com.br",
        "www.vitafor.com.br",
        "www.cultivar.com.br",
        "www.saudavelmente.com.br",
        "www.emporiobiomarket.com.br",
        # ── Pet ────────────────────────────────────────────────────────────────
        "www.petlove.com.br",
        "www.cobasi.com.br",
        "www.patacomoficio.com.br",
        "www.petmania.com.br",
        "www.petcenter.com.br",
        "www.petz.com.br",
        "www.petferias.com.br",
        "www.doghouse.com.br",
        # ── Infantil / Brinquedos ──────────────────────────────────────────────
        "www.puket.com.br",
        "www.lilica.com.br",
        "www.tip-top.com.br",
        "www.imaginarium.com.br",
        "www.grow.com.br",
        "www.estrela.com.br",
        "www.toymania.com.br",
        "www.babytunes.com.br",
        # ── Farmácia / Saúde ──────────────────────────────────────────────────
        "www.drogariasaopaulo.com.br",
        "www.ultrafarma.com.br",
        "www.drogaraia.com.br",
        "www.farmaciaindiana.com.br",
        "www.natue.com.br",
        "www.drogatel.com.br",
        # ── Nicho / Long tail (maior chance de WhatsApp) ──────────────────────
        "www.estrela10.com.br",
        "www.uaucosmetics.com.br",
        "www.a-moça.com.br",
        "www.nandacosmetics.com.br",
        "www.emporiocriativo.com.br",
        "www.elo7.com.br",
        "www.nuvemshop.com.br",
        "www.caprichosaimport.com.br",
        "www.atacadao-da-beleza.com.br",
        "www.modabrasileira.com.br",
        "www.lojasrenner.com.br",
        "www.marisa.com.br",
        "www.riachuelo.com.br",
        "www.lojasamaral.com.br",
        "www.atacado.mercadolivre.com.br",
    ]
