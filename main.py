"""
Shopify Active Store Scraper
════════════════════════════
Identifica lojas ativas na Shopify e extrai dados de contato + CNPJ.
Exporta resultado em CSV.

Uso:
    python main.py [--limit N] [--output arquivo.csv] [--sources a,b,c,d]

Exemplos:
    python main.py
    python main.py --limit 100 --output lojas.csv
    python main.py --sources b,c   # apenas CommonCrawl + Fingerprint
    python main.py --domains minha-lista.txt  # arquivo com 1 domínio por linha
"""
import os
import sys

# Forçar UTF-8 no Windows para evitar UnicodeEncodeError no terminal
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import argparse
import concurrent.futures
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

load_dotenv()

# ── Fontes ────────────────────────────────────────────────────────────────────
from sources.builtwith_source   import fetch_domains as builtwith_fetch
from sources.commoncrawl_source import fetch_domains as commoncrawl_fetch
from sources.google_source      import fetch_domains as google_fetch
from sources.directories_source import fetch_domains as directories_fetch
from sources.fingerprint_source import filter_active

# ── Extrator ──────────────────────────────────────────────────────────────────
from extractors.store_extractor import extract_store

# ── Helpers ───────────────────────────────────────────────────────────────────
from utils.helpers import dedupe_domains

console = Console()

MAX_WORKERS = int(os.getenv("MAX_WORKERS", 10))

CSV_COLUMNS = [
    "dominio", "plataforma", "nome_loja", "whatsapp", "telefone", "email",
    "instagram", "cnpj", "razao_social", "endereco", "cidade",
    "estado", "produtos_count", "tem_blog", "status", "data_coleta",
]


# ─────────────────────────────────────────────────────────────────────────────
def collect_candidates(sources: list[str], limit: int) -> list[str]:
    """Coleta domínios candidatos de todas as fontes habilitadas."""
    domains = []

    with console.status("[bold cyan]Coletando domínios candidatos...[/bold cyan]"):
        if "a" in sources:
            domains += builtwith_fetch()

        if "b" in sources:
            domains += commoncrawl_fetch(limit=limit)

        if "d" in sources:
            domains += google_fetch()

        if "e" in sources:
            domains += directories_fetch()

    domains = dedupe_domains(domains)
    console.print(f"\n[bold]Total candidatos únicos:[/bold] {len(domains)}")
    return domains[:limit]


def verify_candidates(domains: list[str]) -> list[tuple[str, str]]:
    """Detecta plataforma de e-commerce de cada domínio candidato."""
    if not domains:
        return []

    console.print(f"\n[bold cyan]Detectando plataforma de {len(domains)} domínios...[/bold cyan]")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Verificando lojas...", total=len(domains))
        active = filter_active(domains, progress=progress, task_id=task)

    # Contagem por plataforma
    from collections import Counter
    counts = Counter(p for _, p in active)
    summary = " | ".join(f"{p}: {n}" for p, n in counts.most_common())
    console.print(f"[green]✓ Lojas ativas: {len(active)}[/green]  [dim]{summary}[/dim]")
    return active


def extract_all(active_stores: list[tuple[str, str]]) -> list[dict]:
    """Extrai dados de todas as lojas ativas em paralelo."""
    if not active_stores:
        return []

    console.print(f"\n[bold cyan]Extraindo dados de {len(active_stores)} lojas...[/bold cyan]")
    results = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%  "),
        TextColumn("[cyan]{task.completed}/{task.total}[/cyan]"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Extraindo dados...", total=len(active_stores))

        def _extract(domain_platform):
            domain, platform = domain_platform
            data = extract_store(domain, platform=platform)
            progress.advance(task)
            return data

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(_extract, dp) for dp in active_stores]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    results.append(result)

    return results


def export_csv(records: list[dict], output_path: str):
    """Exporta registros para CSV via pandas."""
    df = pd.DataFrame(records, columns=CSV_COLUMNS)

    # Ordenar: lojas com mais dados primeiro
    df["_score"] = (
        df["whatsapp"].notna().astype(int) * 3 +  # WhatsApp vale mais para SDR
        df["email"].notna().astype(int) * 2 +
        df["telefone"].notna().astype(int) +
        df["cnpj"].notna().astype(int) +
        df["instagram"].notna().astype(int)
    )
    df = df.sort_values("_score", ascending=False).drop(columns="_score")
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    console.print(f"\n[bold green]✓ CSV exportado:[/bold green] {output_path}")
    console.print(
        f"  [dim]{len(df)} lojas | "
        f"{df['whatsapp'].notna().sum()} com WhatsApp | "
        f"{df['cnpj'].notna().sum()} com CNPJ | "
        f"{df['email'].notna().sum()} com e-mail[/dim]"
    )

    _update_visualizer(df, output_path)
    return df


def _update_visualizer(df: pd.DataFrame, csv_path: str):
    """Atualiza o visualizar.html embutindo os dados do CSV mais recente."""
    html_path = os.path.join(os.path.dirname(os.path.abspath(csv_path)), "visualizar.html")
    if not os.path.exists(html_path):
        return
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()

    csv_content = df.to_csv(index=False).replace("`", "'")
    new_block = f"// Dados embutidos (gerados pelo scraper)\nconst EMBEDDED_CSV = `{csv_content}`;\n\nloadCSV(EMBEDDED_CSV);"

    import re
    html = re.sub(
        r"// Dados embutidos.*?loadCSV\(EMBEDDED_CSV\);",
        new_block,
        html,
        flags=re.DOTALL,
    )
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    console.print(f"  [dim]visualizar.html atualizado com {len(df)} lojas[/dim]")


def print_summary(df: pd.DataFrame):
    """Exibe tabela resumo das 10 primeiras lojas no terminal."""
    table = Table(title="Top lojas encontradas", show_lines=True)
    for col in ["dominio", "plataforma", "nome_loja", "whatsapp", "email", "cnpj"]:
        table.add_column(col, no_wrap=(col in ("dominio", "whatsapp")))

    for _, row in df.head(10).iterrows():
        table.add_row(
            str(row.get("dominio") or ""),
            str(row.get("plataforma") or ""),
            str(row.get("nome_loja") or "")[:25],
            str(row.get("whatsapp") or ""),
            str(row.get("email") or ""),
            str(row.get("cnpj") or ""),
        )
    console.print(table)


# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Shopify Active Store Scraper")
    parser.add_argument("--limit", type=int, default=200,
                        help="Máximo de domínios a processar (padrão: 200)")
    parser.add_argument("--output", default=None,
                        help="Caminho do CSV de saída (padrão: lojas_shopify_YYYYMMDD.csv)")
    parser.add_argument("--sources", default="a,b,c,d,e",
                        help="Fontes a usar: a=BuiltWith, b=CommonCrawl, c=Fingerprint, "
                             "d=Google, e=DiretóriosBR (padrão: a,b,c,d,e)")
    parser.add_argument("--domains", default=None,
                        help="Arquivo .txt com domínios candidatos (1 por linha) — "
                             "pula coleta, vai direto para verificação e extração")
    parser.add_argument("--all", action="store_true",
                        help="Incluir lojas sem WhatsApp (padrão: exporta só com WhatsApp)")
    args = parser.parse_args()

    sources = [s.strip().lower() for s in args.sources.split(",")]
    output = args.output or f"lojas_shopify_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"

    console.rule("[bold cyan]Shopify Active Store Scraper[/bold cyan]")
    console.print(f"  Fontes: {sources} | Limite: {args.limit} | Saída: {output}\n")

    # ── Fase 1: Coleta de candidatos ──────────────────────────────────────────
    if args.domains:
        with open(args.domains, "r", encoding="utf-8") as f:
            candidates = [line.strip() for line in f if line.strip() and not line.startswith("#")]
        candidates = candidates[:args.limit]
        console.print(f"[cyan]→ Usando arquivo de domínios: {len(candidates)} entradas[/cyan]")
    else:
        candidates = collect_candidates(sources, args.limit)

    if not candidates:
        console.print("[red]✗ Nenhum candidato encontrado. Verifique as chaves de API.[/red]")
        sys.exit(1)

    # ── Fase 2: Verificação de lojas ativas (Fonte C) ─────────────────────────
    if "c" in sources or args.domains:
        active_stores = verify_candidates(candidates)
    else:
        active_stores = [(d, "shopify") for d in candidates]
        console.print("[yellow]⚠ Fingerprint desabilitado — assumindo Shopify[/yellow]")

    if not active_stores:
        console.print("[red]✗ Nenhuma loja ativa confirmada.[/red]")
        sys.exit(1)

    # ── Fase 3: Extração de dados ──────────────────────────────────────────────
    records = extract_all(active_stores)

    if not records:
        console.print("[red]✗ Nenhum dado extraído.[/red]")
        sys.exit(1)

    # ── Fase 4: Filtro WhatsApp ───────────────────────────────────────────────
    if not args.all:
        before = len(records)
        records = [r for r in records if r.get("whatsapp")]
        console.print(
            f"[cyan]→ Filtro WhatsApp: {len(records)}/{before} lojas com número ativo[/cyan]"
        )
        if not records:
            console.print("[red]✗ Nenhuma loja com WhatsApp encontrada. Use --all para exportar todas.[/red]")
            sys.exit(1)

    # ── Fase 5: Export CSV ────────────────────────────────────────────────────
    df = export_csv(records, output)
    print_summary(df)

    console.rule("[green]Concluído[/green]")


if __name__ == "__main__":
    main()
