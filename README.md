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
- **PostgreSQL** — acessível em `localhost:5432` (usuário/senha/banco: `tcc`/`tcc`/`tcc_pedidos`), com os schemas `pedido`, `pagamento` e `estoque` já criados na inicialização.

Para validar que o ambiente subiu corretamente:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec postgres psql -U tcc -d tcc_pedidos -c "\dn"
```
