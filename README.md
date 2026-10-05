# TCC: Garantias de Entrega em Sistemas Orientados a Eventos

## Objetivo

Este projeto é o experimento prático de um Trabalho de Conclusão de Curso sobre **garantias de entrega de mensagens** (delivery guarantees) em arquiteturas orientadas a eventos baseadas em Apache Kafka.

### Problema de pesquisa

Sistemas distribuídos que se comunicam via mensageria assíncrona estão sujeitos, por natureza, a falhas parciais: quedas de broker, falhas de rede, crashes de serviço no meio do processamento de uma mensagem. Sem tratamento explícito, essas falhas tipicamente resultam em **duplicação de eventos** (reprocessamento) ou **perda de eventos** (mensagens nunca publicadas ou nunca processadas), comprometendo a consistência do domínio de negócio — por exemplo, um pagamento cobrado duas vezes ou uma baixa de estoque que nunca ocorre.

Este trabalho investiga, de forma empírica, o quanto cada técnica de garantia de entrega (idempotência de producer, idempotência de consumer, transactional outbox, retry com dead-letter queue, transações Kafka) reduz a taxa de duplicação e perda observada sob diferentes cenários de falha injetada, e a que custo (latência, complexidade, throughput).

### Abordagem experimental

O sistema modela um fluxo de e-commerce simplificado — **criação de pedido → processamento de pagamento → baixa de estoque** — implementado em múltiplas versões incrementais:

- **Versão 0:** monólito transacional (baseline de controle, sem mensageria)
- **Versão 1:** três serviços distribuídos via Kafka, sem nenhuma proteção (baseline de falha)
- **Versão 2:** producer idempotente
- **Versão 3:** idempotência de consumer
- **Versão 4:** transactional outbox pattern
- **Versão 5:** retry com backoff exponencial + dead-letter queue
- **Versão 6:** transações Kafka (exactly-once real, read-process-write)

Cada versão é submetida à mesma matriz de cenários de falha injetada (queda de broker, queda de serviço, latência de rede, partição de rede, falha aleatória de aplicação, falha de banco de dados), sob carga controlada, e as métricas de duplicação, perda e tempo de recuperação são comparadas entre versões.


## Stack

Java 21, Spring Boot 3.x, PostgreSQL, Apache Kafka (modo KRaft), Docker Compose, Testcontainers, Toxiproxy, k6.

## Estrutura do repositório

```
.
├── docker-compose.yml       # Kafka (KRaft) + PostgreSQL
├── docker/postgres/         # script de inicialização (schemas)
├── monolito-pedidos/        # versão 0 (baseline monolítico)
├── order-service/
├── payment-service/
├── stock-service/
├── failure-injection/       # scripts de falha e carga
└── results/                 # dados coletados dos experimentos
```

## Subindo o ambiente

```bash
docker-compose up -d
```

Isso sobe:
- **Kafka** (modo KRaft, sem Zookeeper) — acessível em `localhost:29092` a partir do host, e em `kafka:9092` a partir de outros containers na rede `tcc-network`.
- **PostgreSQL** — acessível em `localhost:5433` (usuário/senha/banco: `tcc`/`tcc`/`tcc_pedidos`), com os schemas `pedido`, `pagamento` e `estoque` já criados na inicialização. A porta publicada é `5433` (não a padrão `5432`) porque nesta máquina já existe um PostgreSQL nativo do Windows escutando em `5432`, que entraria em conflito com o do Docker.

Para validar que o ambiente subiu corretamente:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec postgres psql -U tcc -d tcc_pedidos -c "\dn"
```

## Fase 3 — Resultado de controle (Versão 1: baseline sem proteção)

**Objetivo:** reproduzir, de forma controlada, a duplicação de efeito de negócio quando um consumidor cai no meio do processamento. Esse é o "resultado de controle" que justifica as proteções das próximas versões.

### Mecanismo

O `payment-service` consome `pedidos.criados`, grava o `Pagamento` no seu schema e publica `pagamentos.processados`. O offset da mensagem só é commitado no Kafka **depois** que o listener termina (semântica *at-least-once*). Se o processo morrer entre "já gravei o pagamento" e "confirmei o offset", o Kafka não sabe que a mensagem foi tratada e a **reentrega** quando o serviço volta, e o pagamento é gravado uma segunda vez. Como a Versão 1 não tem idempotência no producer, nem deduplicação no consumer, nem outbox, nada impede isso.

O pedido ainda fecha como `CONCLUIDO`, então a falha é silenciosa: só aparece contando as linhas de `pagamento.pagamento`.

### Cenário testado

`failure-injection/kill-payment-service.sh [espera_ms]`:

1. aquece o `payment-service` com um pedido descartável (a primeira mensagem após um restart é lenta e mascara o tempo medido);
2. cria o pedido de teste via `POST /pedidos`;
3. espera `espera_ms` e executa `docker kill payment-service` (SIGKILL, sem desligamento limpo);
4. reinicia o container e consulta a cada 3s (até 60s) quantas linhas existem em `pagamento.pagamento` para o pedido.

A reentrega não é imediata: o consumer group só reatribui as partições depois que o *session timeout* do consumidor morto expira. Nas execuções, a segunda linha apareceu cerca de **40s** depois do restart.

`failure-injection/varrer-falha.sh [repeticoes] [valores_ms...]` repete o cenário para vários tempos de espera e grava um CSV em `results/`.

### Configuração do experimento

O listener do `payment-service` usa `ack-mode=manual` e permite inserir um atraso entre o processamento e o `ack`, controlado por `app.demo.atraso-ack-ms` (variável `ATRASO_ACK_MS` no `docker-compose`):

| Modo | Configuração | Janela vulnerável |
|---|---|---|
| Natural | `ATRASO_ACK_MS=0` (padrão) | alguns milissegundos |
| Amplificado | `ATRASO_ACK_MS=800` | 800 ms |

```bash
ATRASO_ACK_MS=800 docker compose up -d payment-service
```

### Resultado (modo amplificado, atraso de 800 ms)

Dados brutos: [`results/v1/varredura-atraso800ms-20261005.csv`](results/v1/varredura-atraso800ms-20261005.csv). Foram 9 tempos de espera × 3 repetições = 27 execuções.

| Espera (ms) | Execuções | Com duplicação | Linhas em `pagamento.pagamento` por pedido | Status final |
|---|---|---|---|---|
| 0 | 3 | 3 | 2 | CONCLUIDO |
| 2 | 3 | 3 | 2 | CONCLUIDO |
| 5 | 3 | 3 | 2 | CONCLUIDO |
| 8 | 3 | 3 | 2 | CONCLUIDO |
| 10 | 3 | 3 | 2 | CONCLUIDO |
| 15 | 3 | 3 | 2 | CONCLUIDO |
| 20 | 3 | 3 | 2 | CONCLUIDO |
| 30 | 3 | 3 | 2 | CONCLUIDO |
| 50 | 3 | 3 | 2 | CONCLUIDO |

**27 de 27 execuções duplicaram**, sempre com exatamente 2 linhas por pedido (uma reentrega por falha).

### Conclusão

Na Versão 1, uma queda do consumidor entre o processamento e o commit do offset **duplica o efeito de negócio** (dois pagamentos para o mesmo pedido), sem nenhum erro visível. O resultado confirma o mecanismo *at-least-once* descrito acima e é o ponto de comparação para as próximas versões.

### Limitações e pendências

- **A janela foi alargada de propósito.** Todos os tempos testados (0 a 50 ms) caem dentro dos 800 ms, por isso o resultado é 100% e a varredura não diferencia nenhum deles. O resultado prova o mecanismo, não a frequência natural da falha. A fronteira da janela (esperas próximas ou acima de 800 ms) ainda não foi testada.
- **O modo natural (`ATRASO_ACK_MS=0`) ainda não foi medido.**
- **O cenário de perda não foi reproduzido.** Ele exigiria derrubar o `order-service` entre gravar o pedido e publicar o evento.
- **O efeito no `stock-service` não foi verificado.** A mensagem duplicada também é publicada de novo e o estoque pode ser reservado duas vezes.
- **O baseline difere ligeiramente da V1 original:** o listener usa ack manual e não tem `@Transactional`, para que a gravação já esteja commitada quando o atraso começa.
