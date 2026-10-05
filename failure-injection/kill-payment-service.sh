#!/usr/bin/env bash
# Uso: ./kill-payment-service.sh [espera_ms]
# Cria um pedido e mata o payment-service pouco depois, simulando falha
# no meio do processamento. Rode várias vezes variando espera_ms pra
# achar a janela que reproduz duplicação/perda.

set -euo pipefail

ESPERA_MS=${1:-50}
ORDER_SERVICE_URL=${ORDER_SERVICE_URL:-http://localhost:8080}

seed_e_criar() {
  local nome="$1"
  local produto_id
  produto_id=$(docker exec postgres psql -q -U tcc -d tcc_pedidos -t -A -c \
    "INSERT INTO estoque.estoque (id, produto_id, nome_produto, quantidade_disponivel) VALUES (gen_random_uuid(), gen_random_uuid(), '$nome', 100) RETURNING produto_id;")
  local resposta
  resposta=$(curl -s -X POST "$ORDER_SERVICE_URL/pedidos" \
    -H "Content-Type: application/json" \
    -d "{\"itens\":[{\"produtoId\":\"$produto_id\",\"quantidade\":1,\"precoUnitario\":10.00}]}")
  echo "$resposta" | grep -o '"id":"[^"]*"' | cut -d'"' -f4
}

# Aquece o payment-service (pool de conexão, Hibernate, producer Kafka) com um
# pedido descartável, pra não deixar esse custo de "primeira mensagem" competir
# com a janela de tempo que estamos tentando medir no pedido de teste real.
echo "Aquecendo payment-service com um pedido de warm-up..."
seed_e_criar "Produto Warmup" > /dev/null
sleep 3
echo "Warm-up concluído."

PEDIDO_ID=$(seed_e_criar "Produto Falha")
echo "Pedido criado: $PEDIDO_ID"

echo "Aguardando ${ESPERA_MS}ms antes de matar o payment-service..."
sleep "$(awk "BEGIN {print $ESPERA_MS/1000}")"

echo "Matando payment-service..."
docker kill payment-service

sleep 2
echo "Reiniciando payment-service..."
docker start payment-service

# O grupo consumidor do Kafka só libera o rebalanceamento pro novo consumidor
# depois que o session timeout do consumidor "morto" expira (isso pode levar
# perto de 45s) -- por isso esperamos em ciclos, em vez de um sleep fixo curto.
echo "Aguardando o consumer group reequilibrar e reprocessar (pode levar até ~45-60s)..."
SESSION_TIMEOUT_MS=$(docker exec payment-service printenv APP_KAFKA_SESSION_TIMEOUT_MS || echo "45000")
MAX_ESPERA=$((SESSION_TIMEOUT_MS / 1000 + 20))
INTERVALO=3
decorrido=0
QTD=1
while [ "$decorrido" -lt "$MAX_ESPERA" ]; do
  sleep "$INTERVALO"
  decorrido=$((decorrido + INTERVALO))
  QTD=$(docker exec postgres psql -q -U tcc -d tcc_pedidos -t -A -c \
    "SELECT COUNT(*) FROM pagamento.pagamento WHERE pedido_id = '$PEDIDO_ID';")
  echo "  [${decorrido}s] pagamentos registrados até agora: $QTD"
  if [ "$QTD" -gt 1 ]; then
    break
  fi
done

echo ""
echo "=== Verificação ==="
echo "Pagamentos registrados para esse pedido (esperado 1; se vier >1, houve duplicação):"
docker exec postgres psql -U tcc -d tcc_pedidos -c "SELECT id, status, data_processamento FROM pagamento.pagamento WHERE pedido_id = '$PEDIDO_ID';"
echo "Status final do pedido:"
curl -s "$ORDER_SERVICE_URL/pedidos/$PEDIDO_ID"
echo