#!/usr/bin/env bash
# Ferramenta de apoio da Fase 3 — varre o TAMANHO da janela vulnerável (o atraso
# entre processar e confirmar o offset, ATRASO_ACK_MS) e mede quantas execuções
# duplicam em cada tamanho. Para cada atraso, recria o payment-service com o
# valor e chama o varrer-falha.sh (que grava um CSV por atraso em results/<VERSAO>/).
#
# Por que variar a janela e não a espera: com a janela natural (poucos ms) o
# docker kill externo chega depois do ack (o kill leva centenas de ms e o
# processamento termina antes de o curl voltar), então a taxa natural não é
# mensurável por esse método. Aqui medimos como a duplicação cresce com a janela.
#
# Uso: ./varrer-janela.sh [repeticoes] [atrasos_ms...]
# Ex.:  ./varrer-janela.sh 5 0 50 100 200 400 800
#
# Variáveis opcionais: ESPERA_MS (padrão 0), SESSION_TIMEOUT_MS (6000), HEARTBEAT_MS (2000), VERSAO (v1)

set -euo pipefail

REPETICOES=${1:-5}
if [ "$#" -gt 0 ]; then shift; fi

if [ "$#" -eq 0 ]; then
  ATRASOS=(0 50 100 200 400 800)
else
  ATRASOS=("$@")
fi

ESPERA_MS=${ESPERA_MS:-0}
SESSION_TIMEOUT_MS=${SESSION_TIMEOUT_MS:-6000}
HEARTBEAT_MS=${HEARTBEAT_MS:-2000}

cd "$(dirname "$0")/.."

RESUMO=$(mktemp)
trap 'rm -f "$RESUMO"' EXIT

for atraso in "${ATRASOS[@]}"; do
  echo "##### Janela (atraso do ack) = ${atraso}ms #####"

  ATRASO_ACK_MS=$atraso SESSION_TIMEOUT_MS=$SESSION_TIMEOUT_MS HEARTBEAT_MS=$HEARTBEAT_MS \
    docker compose up -d payment-service > /dev/null 2>&1

  # espera o consumer entrar no grupo e receber as partições
  pronto=0
  for ((i = 0; i < 60; i++)); do
    if docker logs payment-service 2>&1 | grep -q "partitions assigned"; then
      pronto=1
      break
    fi
    sleep 2
  done
  if [ "$pronto" -ne 1 ]; then
    echo "payment-service não ficou pronto a tempo (atraso=${atraso}ms); abortando." >&2
    exit 1
  fi

  saida=$(mktemp)
  bash failure-injection/varrer-falha.sh "$REPETICOES" "$ESPERA_MS" | tee "$saida"
  echo "atraso ${atraso}ms -> $(grep 'Execuções com duplicação' "$saida")" >> "$RESUMO"
  rm -f "$saida"
done

# devolve o payment-service à configuração padrão (atraso 0, session timeout 45s)
docker compose up -d payment-service > /dev/null 2>&1

echo ""
echo "===== Resumo (espera=${ESPERA_MS}ms, ${REPETICOES} repetições por tamanho de janela) ====="
cat "$RESUMO"
