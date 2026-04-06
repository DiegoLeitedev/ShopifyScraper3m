# ShopifyScraper3m

Scraper para identificar lojas ativas de e-commerce brasileiro e extrair dados de contato para prospecção.

Detecta **Shopify, Tray, Nuvemshop, Loja Integrada, VTEX e WooCommerce**. Extrai WhatsApp, e-mail, CNPJ, Instagram e gera CSV pronto para SDR.

---

## Funcionalidades

- Coleta domínios candidatos via 4 fontes (BuiltWith, CommonCrawl, Google, Diretórios BR)
- Detecta a plataforma de e-commerce por fingerprint de HTML/CDN
- Extrai dados de contato: WhatsApp, telefone, e-mail, Instagram, CNPJ, razão social
- Exporta CSV ordenado por score (lojas com mais dados primeiro)
- Dashboard visual (`visualizar.html`) com filtros e busca

## Instalação

```bash
# Clonar e instalar dependências
git clone https://github.com/DiegoLeitedev/ShopifyScraper3m.git
cd ShopifyScraper3m
pip install -r requirements.txt

# Configurar variáveis de ambiente
cp .env.example .env
# Editar .env com suas chaves de API
```

## Uso

```bash
# Execução padrão (200 domínios, todas as fontes)
python main.py

# Limitar quantidade e definir arquivo de saída
python main.py --limit 500 --output meus_leads.csv

# Usar apenas fontes específicas
python main.py --sources b,c        # CommonCrawl + Fingerprint

# Passar lista própria de domínios
python main.py --domains minha-lista.txt
```

## Fontes disponíveis

| Flag | Fonte | Requer API Key |
|------|-------|---------------|
| `a` | BuiltWith API | Sim (`BUILTWITH_API_KEY`) |
| `b` | CommonCrawl | Não |
| `c` | Fingerprint (detecção ativa) | Não |
| `d` | Google Search (SerpAPI) | Sim (`SERPAPI_KEY`) |
| `e` | Diretórios BR | Não |

## Configuração (.env)

```env
BUILTWITH_API_KEY=sua_chave_aqui
SERPAPI_KEY=sua_chave_aqui        # opcional
MAX_WORKERS=10
REQUEST_TIMEOUT=15
DELAY_BETWEEN_REQUESTS=1.5
```

## Estrutura do projeto

```
shopify-scraper/
├── main.py                    # Orquestra as 4 fases
├── sources/
│   ├── builtwith_source.py    # Fonte A
│   ├── commoncrawl_source.py  # Fonte B
│   ├── fingerprint_source.py  # Fonte C — detecta plataforma
│   ├── google_source.py       # Fonte D
│   └── directories_source.py  # Fonte E
├── extractors/
│   └── store_extractor.py     # Extrai contato e CNPJ
├── utils/
│   ├── helpers.py
│   └── http.py
├── visualizar.html            # Dashboard visual
└── .env.example
```

## CSV de saída

| Campo | Descrição |
|-------|-----------|
| `dominio` | URL da loja |
| `plataforma` | shopify / tray / nuvemshop / vtex... |
| `nome_loja` | Nome da loja |
| `whatsapp` | Número com DDD |
| `telefone` | Telefone fixo |
| `email` | E-mail de contato |
| `instagram` | @ do perfil |
| `cnpj` | CNPJ encontrado |
| `razao_social` | Razão social |
| `cidade` / `estado` | Localização |
| `produtos_count` | Qtd. de produtos |
| `status` | ativo / inativo |
