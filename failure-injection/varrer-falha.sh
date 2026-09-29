#!/usr/bin/env bash
# Ferramenta de apoio da Fase 3 — varre vários tempos de espera repetindo o
# cenário de falha (matar o payment-service em pontos diferentes) e salva
# os resultados num CSV. NÃO é o script de métricas da Fase 10 (esse aqui
# só ajuda a achar a janela de tempo que reproduz duplicação/perda; o da
# Fase 10 vai processar uma auditoria completa de correlation ID).
#
# Uso: ./varrer-falha.sh [repeticoes_por_valor] [valores_ms...]
# Ex.:  ./varrer-falha.sh 3 0 2 5 10 20 30 50

set -euo pipefail

REPETICOES=${1:-3}
if [ "$#" -gt 0 ]; then shift; fi

if [ "$#" -eq 0 ]; then
  VALORES=(0 2 5 8 10 15 20 30 50)
else
  VALORES=("$@")
fi

ORDER_SERVICE_URL=${ORDER_SERVICE_URL:-http://localhost:8080}
mkdir -p results
CSV="results/fase3-varredura-$(date +%Y%m%d-%H%M%S).csv"

echo "execucao,espera_ms,pedido_id,produto_id,qtd_pagamentos,status_final,duplicado" > "$CSV"

execucao=0
for espera_ms in "${VALORES[@]}"; do
  for ((r = 1; r <= REPETICOES; r++)); do
    execucao=$((execucao + 1))
    echo "=== Execução $execucao (espera=${espera_ms}ms, repetição $r/$REPETICOES) ==="

    produto_id=$(docker exec postgres psql -q -U tcc -d tcc_pedidos -t -A -c \
      "INSERT INTO estoque.estoque (id, produto_id, nome_produto, quantidade_disponivel) VALUES (gen_random_uuid(), gen_random_uuid(), 'Produto Varredura', 100) RETURNING produto_id;")

    resposta=$(curl -s -X POST "$ORDER_SERVICE_URL/pedidos" \
      -H "Content-Type: application/json" \
      -d "{\"itens\":[{\"produtoId\":\"$produto_id\",\"quantidade\":1,\"precoUnitario\":10.00}]}")
    pedido_id=$(echo "$resposta" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)

    python3 -c "import time; time.sleep($espera_ms/1000)" 2>/dev/null || sleep 1

    docker kill payment-service > /dev/null 2>&1
    sleep 2
    docker start payment-service > /dev/null 2>&1
    sleep 3

    qtd_pagamentos=$(docker exec postgres psql -q -U tcc -d tcc_pedidos -t -A -c \
      "SELECT COUNT(*) FROM pagamento.pagamento WHERE pedido_id = '$pedido_id';")

    status_final=$(curl -s "$ORDER_SERVICE_URL/pedidos/$pedido_id" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)

    duplicado="nao"
    if [ "$qtd_pagamentos" -gt 1 ]; then
      duplicado="sim"
    fi

    echo "$execucao,$espera_ms,$pedido_id,$produto_id,$qtd_pagamentos,$status_final,$duplicado" >> "$CSV"
    echo "  -> pagamentos=$qtd_pagamentos, status=$status_final, duplicado=$duplicado"
  done
done

echo ""
echo "Resultados salvos em: $CSV"
echo ""
echo "Execuções com duplicação:"
grep ",sim$" "$CSV" || echo "  nenhuma duplicação capturada nessa rodada"