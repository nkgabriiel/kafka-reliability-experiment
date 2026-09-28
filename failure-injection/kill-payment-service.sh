#!/usr/bin/env bash
# Uso: ./kill-payment-service.sh [espera_ms]
# Cria um pedido e mata o payment-service pouco depois, simulando falha
# no meio do processamento. Rode várias vezes variando espera_ms pra
# achar a janela que reproduz duplicação/perda.

set -euo pipefail

ESPERA_MS=${1:-50}
ORDER_SERVICE_URL=${ORDER_SERVICE_URL:-http://localhost:8080}

PRODUTO_ID=$(docker exec postgres psql -U tcc -d tcc_pedidos -t -c \
  "INSERT INTO estoque.estoque (id, produto_id, nome_produto, quantidade_disponivel) VALUES (gen_random_uuid(), gen_random_uuid(), 'Produto Falha', 100) RETURNING produto_id;" | tr -d ' ')
echo "Produto semeado: $PRODUTO_ID"

RESPOSTA=$(curl -s -X POST "$ORDER_SERVICE_URL/pedidos" \
  -H "Content-Type: application/json" \
  -d "{\"itens\":[{\"produtoId\":\"$PRODUTO_ID\",\"quantidade\":1,\"precoUnitario\":10.00}]}")
PEDIDO_ID=$(echo "$RESPOSTA" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
echo "Pedido criado: $PEDIDO_ID"

echo "Aguardando ${ESPERA_MS}ms antes de matar o payment-service..."
python3 -c "import time; time.sleep($ESPERA_MS/1000)" 2>/dev/null || sleep 1

echo "Matando payment-service..."
docker kill payment-service

sleep 2
echo "Reiniciando payment-service..."
docker start payment-service
sleep 3

echo ""
echo "=== Verificação ==="
echo "Pagamentos registrados para esse pedido (esperado 1; se vier >1, houve duplicação):"
docker exec postgres psql -U tcc -d tcc_pedidos -c "SELECT id, status, data_processamento FROM pagamento.pagamento WHERE pedido_id = '$PEDIDO_ID';"
echo "Status final do pedido:"
curl -s "$ORDER_SERVICE_URL/pedidos/$PEDIDO_ID"
echo