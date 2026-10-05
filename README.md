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

O `payment-service` consome `pedidos.criados`, grava o `Pagamento` no seu schema e publica `pagamentos.processados`. O offset da mensagem só é commitado no Kafka **depois** que o listener termina (semântica *at-least-once*). Se o processo morrer entre "já gravei o pagamento" e "confirmei o offset" (a **janela vulnerável**), o Kafka não sabe que a mensagem foi tratada e a **reentrega** quando o serviço volta, e o pagamento é gravado uma segunda vez. Como a Versão 1 não tem idempotência no producer, nem deduplicação no consumer, nem outbox, nada impede isso.

O pedido ainda fecha como `CONCLUIDO`, então a falha é silenciosa: só aparece contando as linhas de `pagamento.pagamento`.

### Cenário testado

`failure-injection/kill-payment-service.sh [espera_ms]`:

1. aquece o `payment-service` com um pedido descartável (a primeira mensagem após um restart é lenta e mascara o tempo medido);
2. cria o pedido de teste via `POST /pedidos`;
3. espera `espera_ms` e executa `docker kill payment-service` (SIGKILL, sem desligamento limpo);
4. reinicia o container e consulta periodicamente quantas linhas existem em `pagamento.pagamento` para o pedido.

A reentrega não é imediata: o consumer group só reatribui as partições depois que o *session timeout* do consumidor morto expira (cerca de 40 s com o padrão de 45 s).

Scripts de apoio:

- `failure-injection/varrer-falha.sh [repeticoes] [valores_ms...]` repete o cenário para vários tempos de espera e grava um CSV em `results/<versao>/`, com o atraso do ack e o session timeout registrados em colunas.
- `failure-injection/varrer-janela.sh [repeticoes] [atrasos_ms...]` varre o tamanho da janela vulnerável: para cada atraso, recria o `payment-service` com ele e chama o `varrer-falha.sh`.

### Configuração do experimento

O listener do `payment-service` usa `ack-mode=manual` e permite inserir um atraso entre o processamento e o `ack`, controlado por `app.demo.atraso-ack-ms` (variável `ATRASO_ACK_MS` no `docker-compose`). O session timeout do consumer também é configurável (`SESSION_TIMEOUT_MS` e `HEARTBEAT_MS`):

| Modo | Configuração | Janela vulnerável |
|---|---|---|
| Natural | `ATRASO_ACK_MS=0` (padrão) | alguns milissegundos |
| Amplificado | `ATRASO_ACK_MS=800` | 800 ms |

```bash
ATRASO_ACK_MS=800 docker compose up -d payment-service
```

Nas varreduras de tamanho de janela o session timeout foi reduzido para 6 s, só para detectar o consumidor morto mais rápido (a reentrega passou de ~40 s para 10–14 s). Isso não altera o mecanismo: o offset não foi commitado de qualquer forma, e o resultado do modo amplificado se manteve.

### Resultado 1: janela amplificada (800 ms)

Dados brutos: [`results/v1/varredura-atraso800ms-20261005.csv`](results/v1/varredura-atraso800ms-20261005.csv). 9 tempos de espera × 3 repetições = 27 execuções, com o session timeout padrão (45 s).

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

### Resultado 2: duplicação em função do tamanho da janela

Espera fixa em 0 ms, 5 repetições por tamanho de janela (30 execuções), session timeout de 6 s. Os resultados são contagens (k de n), não percentuais: com n = 5 uma taxa estimada seria pouco estável.

| Janela (atraso do ack) | Execuções com duplicação | Dados brutos |
|---|---|---|
| 0 ms (natural) | 0 de 5 | [csv](results/v1/varredura-atraso0ms-20261005-042530.csv) |
| 50 ms | 0 de 5 | [csv](results/v1/varredura-atraso50ms-20261005-042842.csv) |
| 100 ms | 0 de 5 | [csv](results/v1/varredura-atraso100ms-20261005-043156.csv) |
| 200 ms | 0 de 5 | [csv](results/v1/varredura-atraso200ms-20261005-043507.csv) |
| 400 ms | **3 de 5** | [csv](results/v1/varredura-atraso400ms-20261005-043822.csv) |
| 800 ms | **5 de 5** | [csv](results/v1/varredura-atraso800ms-20261005-044052.csv) |

A duplicação aparece quando a janela é grande o suficiente e cresce com ela: nenhuma ocorrência até 200 ms, ocorrência parcial em 400 ms e em todas as execuções em 800 ms.

### O modo natural foi testado, mas não é mensurável com `docker kill` externo

A linha de 0 ms da tabela acima (0 de 5) **não** deve ser lida como "a V1 não duplica no modo natural". Ela reflete o limite do método de medição:

- O `payment-service` termina de processar a mensagem **antes** de o script receber a resposta do `POST` (nas medições, dezenas de milissegundos antes), e a janela natural entre gravar e confirmar o offset dura poucos milissegundos.
- O script só começa a contar a espera depois que o `POST` retorna, e o próprio comando `docker kill` levou cerca de 470 ms para executar numa medição.
- Portanto o `kill` chega depois do `ack`, e não há como acertar a janela natural com esse método, qualquer que seja o tempo de espera. O padrão 0% até 200 ms e 3 de 5 em 400 ms é consistente com o `kill` atingindo o processo algumas centenas de milissegundos depois do processamento.

Medir a taxa natural exigiria injetar a falha de dentro do processo (ou com controle de tempo na escala de milissegundos), o que está fora do escopo desta fase.

### Conclusão

Na Versão 1, uma queda do consumidor entre o processamento e o commit do offset **duplica o efeito de negócio** (dois pagamentos para o mesmo pedido), sem nenhum erro visível, e a ocorrência cresce com o tamanho dessa janela. O mecanismo está demonstrado; a frequência natural não pôde ser medida e fica registrada como limitação. Esse resultado é o ponto de comparação para as próximas versões.

### Limitações e pendências

- **A taxa natural de duplicação não foi medida** (ver a seção anterior). Os resultados provam o mecanismo e a dependência do tamanho da janela, não a frequência da falha em produção.
- **Amostra pequena:** 5 execuções por tamanho de janela (3 por espera no resultado 1). A transição entre 200 ms e 800 ms não foi detalhada.
- **O cenário de perda não foi reproduzido.** Ele exigiria derrubar o `order-service` entre gravar o pedido e publicar o evento.
- **O efeito no `stock-service` não foi verificado.** A mensagem duplicada também é publicada de novo e o estoque pode ser reservado duas vezes.
- **O baseline difere ligeiramente da V1 original:** o listener usa ack manual e não tem `@Transactional`, para que a gravação já esteja commitada quando o atraso começa.
