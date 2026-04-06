#!/bin/bash
# Setup completo do ambiente

echo "=== Shopify Store Scraper — Setup ==="

# 1. Instalar dependências
pip install -r requirements.txt

# 2. Instalar browsers do Playwright (necessário apenas uma vez)
playwright install chromium

# 3. Copiar .env de exemplo se não existir
if [ ! -f .env ]; then
    cp .env.example .env
    echo "✓ .env criado — edite com suas chaves de API"
else
    echo "✓ .env já existe"
fi

echo ""
echo "=== Pronto para usar ==="
echo "python main.py --help"
