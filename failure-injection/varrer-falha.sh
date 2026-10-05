#!/usr/bin/env bash
# Ferramenta de apoio da Fase 3 — varre vários tempos de espera repetindo o
# cenário de falha (matar o payment-service em pontos diferentes) e salva
# os resultados num CSV. NÃO é o script de métricas da Fase 10 (esse aqui
# só ajuda a achar a janela de tempo que reproduz duplicação/perda; o da
# Fase 10 vai processar uma auditoria completa de correlation ID).
#
# Uso: ./varrer-falha.sh [repeticoes_por_valor] [valores_ms...]
# Ex.:  ./varrer-falha.sh 5 0 1 2 3 5 8 10 15 20
#
# O CSV é gravado em results/<VERSAO>/varredura-atraso<N>ms-<data>.csv (VERSAO=v1
# por padrão). O atraso do ack e o session timeout são lidos do container em
# execução e gravados em colunas, então cada arquivo descreve a própria
# configuração. A espera máxima pela reentrega é session.timeout + 20s.

set -euo pipefail

REPETICOES=${1:-3}
if [ "$#" -gt 0 ]; then shift; fi

if [ "$#" -eq 0 ]; then
  VALORES=(0 2 5 8 10 15 20 30 50)
else
  VALORES=("$@")
fi

ORDER_SERVICE_URL=${ORDER_SERVICE_URL:-http://localhost:8080}
VERSAO=${VERSAO:-v1}

ATRASO_ACK_MS=$(docker exec payment-service printenv APP_DEMO_ATRASO_ACK_MS || echo "?")
SESSION_TIMEOUT_MS=$(docker exec payment-service printenv APP_KAFKA_SESSION_TIMEOUT_MS || echo "45000")
MAX_ESPERA=$((SESSION_TIMEOUT_MS / 1000 + 20))
INTERVALO=2

RESULTS_DIR="results/$VERSAO"
mkdir -p "$RESULTS_DIR"
CSV="$RESULTS_DIR/varredura-atraso${ATRASO_ACK_MS}ms-$(date +%Y%m%d-%H%M%S).csv"

echo "Configuração: atraso_ack=${ATRASO_ACK_MS}ms, session_timeout=${SESSION_TIMEOUT_MS}ms, espera máxima=${MAX_ESPERA}s"
echo "CSV: $CSV"

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

echo "execucao,espera_ms,atraso_ack_ms,session_timeout_ms,pedido_id,qtd_pagamentos,status_final,duplicado" > "$CSV"

total=$(( ${#VALORES[@]} * REPETICOES ))
duplicadas=0
execucao=0
for espera_ms in "${VALORES[@]}"; do
  for ((r = 1; r <= REPETICOES; r++)); do
    execucao=$((execucao + 1))
    echo "=== Execução $execucao/$total (espera=${espera_ms}ms, repetição $r/$REPETICOES) ==="

    # payment-service acabou de reiniciar (ou está iniciando agora); aquece antes
    # do pedido que vamos medir, senão o custo de "primeira mensagem" (pool de
    # conexão, Hibernate, producer Kafka) mascara o tempo que queremos medir.
    seed_e_criar "Produto Warmup" > /dev/null
    sleep 3

    pedido_id=$(seed_e_criar "Produto Varredura")

    sleep "$(awk "BEGIN {print $espera_ms/1000}")"

    docker kill payment-service > /dev/null 2>&1
    docker start payment-service > /dev/null 2>&1

    # o consumer group só reentrega depois que o session timeout do consumidor
    # "morto" expira; consulta em ciclos e para assim que detectar duplicação.
    decorrido=0
    qtd_pagamentos=1
    while [ "$decorrido" -lt "$MAX_ESPERA" ]; do
      sleep "$INTERVALO"
      decorrido=$((decorrido + INTERVALO))
      qtd_pagamentos=$(docker exec postgres psql -q -U tcc -d tcc_pedidos -t -A -c \
        "SELECT COUNT(*) FROM pagamento.pagamento WHERE pedido_id = '$pedido_id';")
      if [ "$qtd_pagamentos" -gt 1 ]; then
        break
      fi
    done

    status_final=$(curl -s "$ORDER_SERVICE_URL/pedidos/$pedido_id" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)

    duplicado="nao"
    if [ "$qtd_pagamentos" -gt 1 ]; then
      duplicado="sim"
      duplicadas=$((duplicadas + 1))
    fi

    echo "$execucao,$espera_ms,$ATRASO_ACK_MS,$SESSION_TIMEOUT_MS,$pedido_id,$qtd_pagamentos,$status_final,$duplicado" >> "$CSV"
    echo "  -> pagamentos=$qtd_pagamentos, status=$status_final, duplicado=$duplicado (${decorrido}s)"
  done
done

echo ""
echo "Resultados salvos em: $CSV"
echo "Execuções com duplicação: $duplicadas de $total"
