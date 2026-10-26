#!/usr/bin/env python3
"""Bateria única de experimentos: a MESMA carga, as MESMAS falhas e as MESMAS métricas para cada versão.

Para cada (versão, cenário) os três serviços são recriados com a versão e os ganchos do cenário; depois,
para cada repetição, a bateria cria pedidos em taxa fixa, injeta a falha (se houver), espera a recuperação
e lê o resultado de FORA do sistema (banco de dados e API). Cada repetição vira uma linha do CSV.

Só usa a biblioteca padrão do Python. Exemplos:
    py benchmark/bateria.py                                  # perfil rápido, versões v1 a v5
    py benchmark/bateria.py --versoes v3 --cenarios kill_publicacao --repeticoes 1
"""
import argparse
import csv
import json
import os
import random
import re
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
URL_PEDIDOS = "http://localhost:8080/pedidos"
PRODUTO_VENENO = "99999999-9999-9999-9999-999999999999"
ESTOQUE_INICIAL = 1000
SERVICOS = ["order-service", "payment-service", "stock-service"]
COMPOSE = ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.override.yml",
           "-f", "docker-compose.toxiproxy.yml"]
URL_TOXIPROXY = "http://localhost:8474"
TOPICOS_DLQ = ["pedidos.criados.dlq", "pagamentos.processados.dlq", "estoque.processado.dlq"]

# Ambiente igual para todos os cenários e versões (session timeout curto para a reentrega ser rápida).
AMBIENTE_BASE = {"SESSION_TIMEOUT_MS": "6000", "HEARTBEAT_MS": "1000", "LISTENER_CONCURRENCY": "3"}

# Cada cenário define os ganchos de falha (env), a carga (pedidos e pedidos/s) e uma ação de falha opcional:
#   kill    -> docker kill + start do serviço alvo        parar   -> derruba o alvo por `duracao` s e o religa
#   restart -> docker restart do serviço alvo (parada limpa + rebalanceamento)
# `espera_pos` é quanto esperar depois da ação terminar para o sistema se recuperar.
CENARIOS = {
    "kill_ack": dict(
        descricao="queda do pagamento na janela do ack (gravou, ainda não confirmou)",
        env={"ATRASO_ACK_MS": "2000"}, pedidos=20, taxa=2, espera_max=35, espera_pos=22,
        acao=dict(tipo="kill", alvo="payment-service", em=5.0)),
    "kill_publicacao": dict(
        descricao="queda do pagamento entre o commit e a publicação",
        env={"ATRASO_PUBLICACAO_PAYMENT_MS": "2000"}, pedidos=20, taxa=2, espera_max=35, espera_pos=22,
        acao=dict(tipo="kill", alvo="payment-service", em=5.0)),
    "poison": dict(
        descricao="um pedido que falha sempre (poison message) no meio de pedidos normais",
        env={"PRODUTO_ENVENENADO": PRODUTO_VENENO}, pedidos=30, taxa=5, veneno_indice=5, espera_max=25),
    "falha_transitoria": dict(
        descricao="50% das chamadas do pagamento falham de forma aleatória",
        env={"FAILURE_RATE": "50"}, pedidos=40, taxa=5, espera_max=30),
    "queda_broker": dict(
        descricao="queda do broker Kafka por 8 s",
        env={}, pedidos=30, taxa=3, espera_max=70, espera_pos=40,
        acao=dict(tipo="parar", alvo="kafka", em=4.0, duracao=8)),
    "queda_banco": dict(
        descricao="queda do PostgreSQL por 8 s",
        env={}, pedidos=30, taxa=3, espera_max=70, espera_pos=35,
        acao=dict(tipo="parar", alvo="postgres", em=4.0, duracao=8)),
    "restart_consumer": dict(
        descricao="restart do consumer de pagamento no meio da carga (rebalanceamento)",
        env={}, pedidos=30, taxa=3, espera_max=45, espera_pos=25,
        acao=dict(tipo="restart", alvo="payment-service", em=5.0)),
    "latencia_rede": dict(
        descricao="latência de 1,5 s na rede entre os serviços e o Kafka, por 12 s",
        env={"KAFKA_BOOTSTRAP": "toxiproxy:19092"}, pedidos=30, taxa=3, espera_max=60, espera_pos=25,
        acao=dict(tipo="toxico", proxy="kafka", em=3.0, duracao=12,
                  toxicos=[dict(type="latency", stream="downstream", attributes=dict(latency=1500))])),
    "particao_rede": dict(
        descricao="partição de rede entre os serviços e o Kafka por 15 s",
        env={"KAFKA_BOOTSTRAP": "toxiproxy:19092", "ATRASO_ACK_MS": "1000"}, pedidos=30, taxa=3,
        espera_max=80, espera_pos=40,
        acao=dict(tipo="toxico", proxy="kafka", em=4.0, duracao=15,
                  toxicos=[dict(type="timeout", stream="downstream", attributes=dict(timeout=0)),
                           dict(type="timeout", stream="upstream", attributes=dict(timeout=0))])),
    "particao_banco": dict(
        descricao="partição de rede entre os serviços e o PostgreSQL por 8 s",
        env={"DB_HOSTPORT": "toxiproxy:15432"}, pedidos=30, taxa=3, espera_max=70, espera_pos=35,
        acao=dict(tipo="toxico", proxy="postgres", em=4.0, duracao=8,
                  toxicos=[dict(type="timeout", stream="downstream", attributes=dict(timeout=0)),
                           dict(type="timeout", stream="upstream", attributes=dict(timeout=0))])),
}
AQUECIMENTO = 2          # pedidos descartados antes de cada repetição (custo da primeira mensagem)
TENTATIVAS_ERRO = 2      # quantas vezes tentar uma repetição que falha por erro de infraestrutura
SEMENTE = 42             # o instante da falha é sorteado por (cenário, repetição), igual para todas as versões
JITTER_ACAO = 1.5        # o instante da falha varia, no máximo, este tanto (s) para mais ou para menos
USAR_SORTEIO = True      # --instante-fixo desliga o sorteio
TENTATIVAS_VALIDADE = 2  # vezes extras que uma repetição inválida (a falha não atingiu a janela) é refeita

# Como saber que a falha atingiu o ponto que o cenário quer exercitar (o artigo só aceita "0 duplicados" assim):
#   em_voo      -> pelo menos 1 mensagem estava na janela (processada e ainda sem ack/publicação) quando o serviço morreu
#   veneno      -> o pedido envenenado foi tentado mais de uma vez
#   retentativa -> pelo menos 1 pedido precisou de nova tentativa
#   acao        -> basta a ação de falha ter sido executada
VALIDADE = {"kill_ack": "em_voo", "kill_publicacao": "em_voo", "poison": "veneno", "falha_transitoria": "retentativa"}
JANELA_MS = {"kill_ack": 2000, "kill_publicacao": 2000}
EXIGE_EM_VOO = {}        # cenários em_voo que NÃO exigem mensagem na janela (ex.: janela de 0 ms, onde o esperado é errar)
ASSENTAR = 4              # segundos extras antes de ler o resultado, para efeitos tardios aparecerem

# V0 (monólito, controle): um único serviço síncrono, sem Kafka, no schema public. Só rodam os cenários que
# fazem sentido sem broker, e a queda do "pagamento" vira a queda do próprio monólito.
URL_MONOLITO = "http://localhost:8090/pedidos"
CENARIOS_V0 = ["kill_ack", "kill_publicacao", "queda_banco", "restart_consumer"]
TABELAS_V0 = dict(pedido="public.pedido", pagamento="public.pagamento", estoque="public.estoque",
                  item="public.item_pedido")
TABELAS = dict(pedido="pedido.pedido", pagamento="pagamento.pagamento", estoque="estoque.estoque",
               item="pedido.item_pedido")


def tabelas(versao):
    return TABELAS_V0 if versao == "v0" else TABELAS


def url_pedidos(versao):
    return URL_MONOLITO if versao == "v0" else URL_PEDIDOS


def alvo_da_acao(versao, alvo):
    return "monolito-pedidos" if versao == "v0" and alvo == "payment-service" else alvo

PERFIS = {
    "rapido": dict(cenarios=["kill_ack", "kill_publicacao", "poison", "falha_transitoria"], repeticoes=3),
    "completo": dict(cenarios=list(CENARIOS), repeticoes=5),
}


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def executar(cmd, env=None, check=True):
    r = subprocess.run(cmd, cwd=RAIZ, env=env, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if check and r.returncode != 0:
        raise RuntimeError(f"falha em {' '.join(cmd)}:\n{r.stdout}\n{r.stderr}")
    return r


def sql(consulta):
    r = executar(["docker", "exec", "postgres", "psql", "-q", "-U", "tcc", "-d", "tcc_pedidos",
                  "-At", "-F", ",", "-c", consulta])
    return [linha.split(",") for linha in r.stdout.strip().splitlines() if linha.strip()]


def lista_sql(ids):
    return ",".join(f"'{i}'" for i in ids)


# ---------------------------------------------------------------- infraestrutura

def toxiproxy(metodo, caminho, corpo=None):
    dados = json.dumps(corpo).encode() if corpo is not None else None
    req = urllib.request.Request(URL_TOXIPROXY + caminho, data=dados, method=metodo,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            texto = resp.read()
            return json.loads(texto) if texto else None
    except urllib.error.HTTPError as e:
        if e.code == 409:     # proxy já existe
            return None
        raise


def preparar_proxies():
    """Kafka e Postgres ficam atrás do Toxiproxy (porta de listener extra do Kafka); só são usados se o cenário pedir."""
    for _ in range(30):
        try:
            toxiproxy("GET", "/proxies")
            break
        except OSError:
            time.sleep(1)
    toxiproxy("POST", "/proxies", dict(name="kafka", listen="0.0.0.0:19092", upstream="kafka:9094", enabled=True))
    toxiproxy("POST", "/proxies", dict(name="postgres", listen="0.0.0.0:15432", upstream="postgres:5432", enabled=True))


def subir_infra():
    log("subindo kafka, postgres e toxiproxy")
    executar(COMPOSE + ["up", "-d", "--wait", "kafka", "postgres", "toxiproxy"])
    preparar_proxies()


def garantir_infra():
    """Depois de uma queda de broker ou de banco, confirma que os dois voltaram e estão saudáveis."""
    executar(COMPOSE + ["up", "-d", "--wait", "kafka", "postgres", "toxiproxy"])


def recriar_servicos(versao, cenario):
    env = os.environ.copy()
    env.update(AMBIENTE_BASE)
    env.update(CENARIOS[cenario]["env"])
    env["VERSAO"] = versao
    if versao == "v0":
        executar(["docker", "stop", *SERVICOS], check=False)
        executar(COMPOSE + ["--profile", "v0", "up", "-d", "--force-recreate", "monolito-pedidos"], env=env)
    else:
        executar(["docker", "stop", "monolito-pedidos"], check=False)
        executar(COMPOSE + ["up", "-d", "--force-recreate", *SERVICOS], env=env)
    aguardar_prontos(versao)


def aguardar_prontos(versao, limite=150):
    """Microsserviços: cada consumer precisa ter recebido as partições desde o último início.
    Monólito: a aplicação precisa ter subido."""
    servicos = ["monolito-pedidos"] if versao == "v0" else SERVICOS
    marca = "Started MonolitoPedidosApplication" if versao == "v0" else "partitions assigned"
    fim = time.monotonic() + limite
    while time.monotonic() < fim:
        prontos = 0
        for servico in servicos:
            inicio = executar(["docker", "inspect", "-f", "{{.State.StartedAt}}", servico]).stdout.strip()
            logs = executar(["docker", "logs", "--since", inicio, servico], check=False)
            if marca in (logs.stdout + logs.stderr):
                prontos += 1
        if prontos == len(servicos):
            return
        time.sleep(2)
    raise RuntimeError("serviços não ficaram prontos a tempo")


def dlq_dos_pedidos(ids):
    """Quantos dos pedidos desta repetição estão em alguma DLQ. Lê só mensagens commitadas e conta pelo id do
    pedido (contar offsets erraria com producer transacional, cujas marcas de commit também ocupam offsets)."""
    encontrados = set()
    for topico in TOPICOS_DLQ:
        r = executar(["docker", "exec", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
                      "--bootstrap-server", "localhost:9092", "--topic", topico], check=False)
        if r.returncode != 0 or not any(int(l.rsplit(":", 1)[1]) > 0 for l in r.stdout.strip().splitlines()):
            continue
        r = executar(["docker", "exec", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
                      "--bootstrap-server", "localhost:9092", "--topic", topico, "--from-beginning",
                      "--timeout-ms", "4000", "--isolation-level", "read_committed"], check=False)
        for pid in ids:
            if pid in r.stdout:
                encontrados.add(pid)
    return len(encontrados)


def executar_acao(acao):
    alvo = acao.get("alvo")
    if acao["tipo"] == "kill":
        executar(["docker", "kill", alvo], check=False)
        executar(["docker", "start", alvo], check=False)
    elif acao["tipo"] == "parar":
        executar(["docker", "kill", alvo], check=False)
        time.sleep(acao["duracao"])
        executar(["docker", "start", alvo], check=False)
    elif acao["tipo"] == "restart":
        executar(["docker", "restart", alvo], check=False)
    elif acao["tipo"] == "toxico":
        nomes = []
        for i, toxico in enumerate(acao["toxicos"]):
            nome = f"t{i}"
            toxiproxy("POST", f"/proxies/{acao['proxy']}/toxics", dict(toxico, name=nome))
            nomes.append(nome)
        time.sleep(acao["duracao"])
        for nome in nomes:
            toxiproxy("DELETE", f"/proxies/{acao['proxy']}/toxics/{nome}")


# ---------------------------------------------------------------- uma repetição

def criar_pedido(versao, produto):
    """Devolve (id, status devolvido pela API). O monólito já responde com o pedido concluído."""
    corpo = json.dumps({"itens": [{"produtoId": produto, "quantidade": 1, "precoUnitario": 10.00}]}).encode()
    req = urllib.request.Request(url_pedidos(versao), data=corpo, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        dados = json.loads(resp.read())
        return dados["id"], dados.get("status")


def status_do_pedido(versao, pedido_id):
    try:
        with urllib.request.urlopen(f"{url_pedidos(versao)}/{pedido_id}", timeout=5) as resp:
            return json.loads(resp.read())["status"]
    except (urllib.error.URLError, OSError, KeyError, ValueError):
        return None


def tentar_criar(versao, produto):
    try:
        return criar_pedido(versao, produto)
    except (urllib.error.URLError, OSError, KeyError, ValueError):
        return None


def aquecer(versao):
    """Pedidos descartados antes da repetição: o custo da primeira mensagem (pool de conexões, Hibernate, producer
    Kafka) não deve distorcer o tempo medido. Uma falha aqui é só registrada e não invalida a repetição."""
    if AQUECIMENTO <= 0:
        return
    tab = tabelas(versao)
    try:
        produto = str(uuid.uuid4())
        sql(f"INSERT INTO {tab['estoque']} (id, produto_id, nome_produto, quantidade_disponivel) "
            f"VALUES (gen_random_uuid(), '{produto}', 'aquecimento', {ESTOQUE_INICIAL})")
        ids = set()
        for _ in range(AQUECIMENTO):
            criado = tentar_criar(versao, produto)
            if criado:
                ids.add(criado[0])
        if versao == "v0":          # o monólito responde de forma síncrona
            return
        fim = time.monotonic() + 20
        while ids and time.monotonic() < fim:
            ids = {i for i in ids if status_do_pedido(versao, i) != "CONCLUIDO"}
            if ids:
                time.sleep(0.5)
    except Exception as e:  # noqa: BLE001
        log(f"    aquecimento falhou ({e}); seguindo sem ele")


RE_TS = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)Z")


def instante_da_falha(nome, numero, base):
    """Instante (s) em que a falha é injetada. Sorteado por (cenário, repetição), sem a versão na semente: na mesma
    repetição, todas as versões levam a falha no mesmo ponto."""
    if not USAR_SORTEIO:
        return base
    return max(1.0, base + random.Random(f"{SEMENTE}-{nome}-{numero}").uniform(-JITTER_ACAO, JITTER_ACAO))


def finalizado_em(servico):
    """Quando o container morreu pela última vez (State.FinishedAt), medido pelo Docker: é o instante real da queda,
    já descontada a latência do docker kill."""
    texto = executar(["docker", "inspect", "-f", "{{.State.FinishedAt}}", servico]).stdout.strip()
    return datetime.fromisoformat(texto.replace("Z", "+00:00"))


def linhas_do_pagamento(desde):
    """(instante, linha) do log do payment-service desde `desde`; as linhas do Spring começam com o horário em UTC."""
    r = executar(["docker", "logs", "--since", desde.isoformat(), "payment-service"], check=False)
    linhas = []
    for linha in (r.stdout + r.stderr).splitlines():
        m = RE_TS.match(linha)
        if m:
            linhas.append((datetime.fromisoformat(m.group(1) + "+00:00"), linha))
    return linhas


def avaliar_validade(versao, nome, cfg, ctx):
    """Devolve (valida, sinal). `sinal` é a evidência de que a falha atingiu a janela (ver VALIDADE)."""
    if cfg.get("acao") and ctx["acao_fim"] is None:
        return False, ""                        # a ação de falha nem terminou
    if versao == "v0":
        return True, ""                         # o monólito não tem os logs do pagamento; vale se a ação rodou
    tipo = VALIDADE.get(nome, "acao")
    if tipo == "acao":
        return True, ""
    linhas = linhas_do_pagamento(ctx["inicio_utc"] - timedelta(seconds=2))
    if tipo == "em_voo":
        morte = ctx["t_morte"]
        if morte is None:
            return False, ""
        janela = timedelta(milliseconds=JANELA_MS[nome])
        em_voo = sum(1 for t, l in linhas if "Pagamento processado" in l and morte - janela <= t <= morte)
        return (em_voo >= 1 or not EXIGE_EM_VOO.get(nome, True)), em_voo
    if tipo == "veneno":
        alvo = ctx["id_veneno"]
        tentativas = sum(1 for _, l in linhas if "Processando pagamento" in l and alvo and alvo in l)
        return tentativas >= 2, tentativas
    por_pedido = Counter()
    for _, l in linhas:
        if "Processando pagamento" in l:
            for pid in ctx["ids"]:
                if pid in l:
                    por_pedido[pid] += 1
                    break
    retentados = sum(1 for n in por_pedido.values() if n >= 2)
    return retentados >= 1, retentados


def repeticao_segura(versao, nome, numero):
    """Roda a repetição. Erro de infraestrutura: restaura o estado e tenta de novo; esgotadas as tentativas, devolve uma
    linha com o erro (que o gráfico ignora), para não perder as horas já rodadas. Repetição inválida (a falha não
    atingiu a janela): é refeita até TENTATIVAS_VALIDADE vezes; se continuar inválida, é registrada como tal."""
    erros = 0
    execucoes = 0
    while True:
        try:
            linha = repeticao(versao, nome, numero)
        except Exception as e:  # noqa: BLE001 - qualquer falha de infraestrutura deve ser contida aqui
            erros += 1
            erro = f"{type(e).__name__}: {e}".replace(chr(10), " ")[:200]
            log(f"    repetição {numero} falhou (tentativa {erros}/{TENTATIVAS_ERRO}): {erro}")
            if erros >= TENTATIVAS_ERRO:
                return dict(versao=versao, cenario=nome, repeticao=numero, erro=erro,
                            inicio=datetime.now().isoformat(timespec="seconds"))
            try:
                recriar_servicos(versao, nome)
            except Exception as e2:  # noqa: BLE001
                log(f"    não foi possível restaurar os serviços: {e2}")
            continue
        execucoes += 1
        linha["tentativas"] = execucoes
        if linha.get("valida", "sim") != "nao" or execucoes > TENTATIVAS_VALIDADE:
            return linha
        log(f"    repetição {numero} inválida (a falha não atingiu a janela; sinal={linha.get('sinal_falha')}); refazendo")


def carregar_feitos(caminho):
    """Repetições já concluídas sem erro em um CSV anterior (para --retomar)."""
    with open(caminho, encoding="utf-8") as f:
        return {(r["versao"], r["cenario"], int(r["repeticao"])) for r in csv.DictReader(f) if not r.get("erro")}


def repeticao(versao, nome, numero):
    cfg = CENARIOS[nome]
    tab = tabelas(versao)
    garantir_infra()
    aguardar_prontos(versao)
    aquecer(versao)
    produto = str(uuid.uuid4())
    sql(f"INSERT INTO {tab['estoque']} (id, produto_id, nome_produto, quantidade_disponivel) "
        f"VALUES (gen_random_uuid(), '{produto}', 'bateria', {ESTOQUE_INICIAL})")

    pedidos = {}          # id -> dict(t_post, veneno, concluido_em)
    emitidos = 0
    criacao_falhou = 0
    acao = cfg.get("acao")
    acao_inicio = acao_fim = None
    thread_acao = None
    inicio = time.monotonic()
    fim_emissao = None
    taxa, total = cfg["taxa"], cfg["pedidos"]
    veneno_indice = cfg.get("veneno_indice")
    inicio_utc = datetime.now(timezone.utc)
    instante = instante_da_falha(nome, numero, acao["em"]) if acao else None
    acao_inicio_abs = None
    t_morte = None

    def rodar_acao():
        nonlocal acao_fim, t_morte
        executar_acao(dict(acao, alvo=alvo_da_acao(versao, acao["alvo"])) if "alvo" in acao else acao)
        if acao["tipo"] in ("kill", "restart") and versao != "v0":
            t_morte = finalizado_em(acao["alvo"])
        acao_fim = time.monotonic() - inicio

    while True:
        agora = time.monotonic() - inicio
        while emitidos < total and agora >= emitidos / taxa:
            veneno = veneno_indice is not None and emitidos == veneno_indice
            t_antes = time.monotonic()
            criado = tentar_criar(versao, PRODUTO_VENENO if veneno else produto)
            if criado is None:
                criacao_falhou += 1
            else:
                pid, status_resposta = criado
                # o monólito é síncrono: a resposta já traz o pedido concluído
                concluido = time.monotonic() if status_resposta == "CONCLUIDO" else None
                pedidos[pid] = dict(t_post=t_antes, veneno=veneno, concluido_em=concluido)
            emitidos += 1
            if emitidos == total:
                fim_emissao = agora
        if acao and acao_inicio is None and agora >= instante:
            acao_inicio = agora
            acao_inicio_abs = time.monotonic()
            thread_acao = threading.Thread(target=rodar_acao)
            thread_acao.start()

        for pid, p in pedidos.items():
            if p["concluido_em"] is None and status_do_pedido(versao, pid) == "CONCLUIDO":
                p["concluido_em"] = time.monotonic()

        agora = time.monotonic() - inicio
        if fim_emissao is not None:
            normais_ok = all(p["concluido_em"] for p in pedidos.values() if not p["veneno"])
            acao_ok = not acao or (acao_fim is not None and agora >= acao_fim + cfg["espera_pos"])
            if (normais_ok and acao_ok) or agora >= fim_emissao + cfg["espera_max"]:
                break
        time.sleep(0.4)

    if thread_acao:
        thread_acao.join()
    time.sleep(ASSENTAR)

    ids = list(pedidos)
    lista = lista_sql(ids) if ids else "''"
    status = {i: s for i, s in sql(f"SELECT id, status FROM {tab['pedido']} WHERE id IN ({lista})")}
    pagamentos = {i: int(n) for i, n in sql(
        f"SELECT pedido_id, COUNT(*) FROM {tab['pagamento']} WHERE pedido_id IN ({lista}) GROUP BY pedido_id")}
    estoque_final = int(sql(f"SELECT quantidade_disponivel FROM {tab['estoque']} WHERE produto_id = '{produto}'")[0][0])
    # todos os pedidos concluídos que mexeram neste produto, mesmo os que o cliente não ficou sabendo (ex.: queda
    # do monólito depois do commit e antes da resposta), para não confundir resposta perdida com reserva duplicada
    concluidos_do_produto = int(sql(
        f"SELECT COUNT(DISTINCT p.id) FROM {tab['pedido']} p JOIN {tab['item']} i ON i.pedido_id = p.id "
        f"WHERE i.produto_id = '{produto}' AND p.status = 'CONCLUIDO'")[0][0])

    concluidos = sum(1 for i in ids if status.get(i) == "CONCLUIDO")
    perdidos = sum(1 for i in ids if status.get(i) not in ("CONCLUIDO", "CANCELADO"))
    duplicados = sum(1 for i in ids if pagamentos.get(i, 0) > 1)
    pagamentos_extra = sum(max(0, n - 1) for n in pagamentos.values())
    estoque_extra = max(0, (ESTOQUE_INICIAL - estoque_final) - concluidos_do_produto)
    latencias = sorted(p["concluido_em"] - p["t_post"] for p in pedidos.values() if p["concluido_em"])
    ultimo = max((p["concluido_em"] for p in pedidos.values() if p["concluido_em"]), default=None)
    # tempo de recuperação: do início da falha até o último pedido normal concluir (vazio se algum nunca concluiu)
    normais = [p["concluido_em"] for p in pedidos.values() if not p["veneno"]]
    recuperacao = ""
    if acao_inicio_abs is not None and normais and all(normais):
        recuperacao = round(max(0.0, max(normais) - acao_inicio_abs), 2)
    valida, sinal = avaliar_validade(versao, nome, cfg, dict(
        inicio_utc=inicio_utc, acao_fim=acao_fim, t_morte=t_morte, ids=ids,
        id_veneno=next((pid for pid, p in pedidos.items() if p["veneno"]), None)))

    def pct(valores, q):
        return round(valores[min(len(valores) - 1, int(q * len(valores)))], 2) if valores else ""

    return dict(
        versao=versao, cenario=nome, repeticao=numero, pedidos=len(ids), criacao_falhou=criacao_falhou,
        concluidos=concluidos,
        perdidos=perdidos, duplicados=duplicados, pagamentos_extra=pagamentos_extra,
        estoque_extra=estoque_extra, dlq=0 if versao == "v0" else dlq_dos_pedidos(ids),
        lat_p50_s=pct(latencias, 0.5), lat_p95_s=pct(latencias, 0.95),
        lat_max_s=round(latencias[-1], 2) if latencias else "",
        tempo_total_s=round(ultimo - inicio, 2) if ultimo else "",
        falha_em_s=round(instante, 2) if instante else "", recuperacao_s=recuperacao,
        sinal_falha=sinal, valida="sim" if valida else "nao",
        inicio=datetime.now().isoformat(timespec="seconds"),
    )


# ---------------------------------------------------------------- orquestração

COLUNAS = ["versao", "cenario", "repeticao", "pedidos", "criacao_falhou", "concluidos", "perdidos", "duplicados",
           "pagamentos_extra", "estoque_extra", "dlq", "lat_p50_s", "lat_p95_s", "lat_max_s",
           "tempo_total_s", "falha_em_s", "recuperacao_s", "sinal_falha", "valida", "tentativas", "inicio", "erro"]


def main():
    global USAR_SORTEIO, SEMENTE
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--perfil", choices=PERFIS, default="rapido")
    ap.add_argument("--versoes", nargs="+", default=["v1", "v2", "v3", "v4", "v5"],
                    help="v0 (monólito) a v6")
    ap.add_argument("--cenarios", nargs="+", choices=CENARIOS)
    ap.add_argument("--repeticoes", type=int)
    ap.add_argument("--saida", help="pasta de saída (padrão: results/bateria/<perfil>-<data>)")
    ap.add_argument("--sem-build", action="store_true", help="não reconstrói as imagens")
    ap.add_argument("--manter", action="store_true", help="não derruba a infraestrutura no final")
    ap.add_argument("--instante-fixo", action="store_true",
                    help="não sorteia o instante da falha (usa sempre o ponto base do cenário)")
    ap.add_argument("--semente", type=int, default=SEMENTE, help="semente do sorteio do instante da falha")
    ap.add_argument("--retomar", action="store_true",
                    help="continua uma rodada interrompida: exige --saida apontando para a pasta existente")
    a = ap.parse_args()
    USAR_SORTEIO = not a.instante_fixo
    SEMENTE = a.semente

    perfil = PERFIS[a.perfil]
    cenarios = a.cenarios or perfil["cenarios"]
    repeticoes = a.repeticoes or perfil["repeticoes"]
    if a.retomar and not a.saida:
        ap.error("--retomar exige --saida com a pasta da rodada interrompida")
    saida = Path(a.saida) if a.saida else RAIZ / "results" / "bateria" / f"{a.perfil}-{datetime.now():%Y%m%d-%H%M%S}"
    saida.mkdir(parents=True, exist_ok=True)
    caminho = saida / "bateria.csv"
    retomando = a.retomar and caminho.exists()
    if not retomando:
        (saida / "config.json").write_text(json.dumps(dict(
            perfil=a.perfil, versoes=a.versoes, cenarios={c: CENARIOS[c] for c in cenarios},
            repeticoes=repeticoes, ambiente_base=AMBIENTE_BASE, estoque_inicial=ESTOQUE_INICIAL,
            aquecimento=AQUECIMENTO, tentativas_erro=TENTATIVAS_ERRO, tentativas_validade=TENTATIVAS_VALIDADE,
            sorteio_do_instante=USAR_SORTEIO, semente=SEMENTE, jitter_s=JITTER_ACAO), indent=2, ensure_ascii=False), encoding="utf-8")
    feitos = carregar_feitos(caminho) if retomando else set()
    log(f"saída: {saida}" + (f" (retomando: {len(feitos)} repetições já concluídas)" if retomando else ""))

    if not a.sem_build:
        log("construindo imagens (uma vez; o código é o mesmo para todas as versões)")
        executar(COMPOSE + ["--profile", "v0", "build"])
    subir_infra()

    with open(caminho, "a" if retomando else "w", newline="", encoding="utf-8") as f:
        escritor = csv.DictWriter(f, fieldnames=COLUNAS, restval="")
        if not retomando:
            escritor.writeheader()
        for versao in a.versoes:
            for cenario in cenarios:
                if versao == "v0" and cenario not in CENARIOS_V0:
                    log(f"=== v0 / {cenario}: não se aplica ao monólito (sem Kafka), pulando")
                    continue
                faltam = [n for n in range(1, repeticoes + 1) if (versao, cenario, n) not in feitos]
                if not faltam:
                    log(f"=== {versao} / {cenario}: já concluído, pulando")
                    continue
                log(f"=== {versao} / {cenario}: {CENARIOS[cenario]['descricao']}")
                recriar_servicos(versao, cenario)
                for n in faltam:
                    linha = repeticao_segura(versao, cenario, n)
                    escritor.writerow(linha)
                    f.flush()
                    if linha.get("erro"):
                        log(f"    rep {n}/{repeticoes}: ERRO registrado ({linha['erro']})")
                        continue
                    log(f"    rep {n}/{repeticoes}: concluidos={linha['concluidos']}/{linha['pedidos']} "
                        f"criacao_falhou={linha['criacao_falhou']} perdidos={linha['perdidos']} "
                        f"duplicados={linha['duplicados']} estoque_extra={linha['estoque_extra']} "
                        f"dlq={linha['dlq']} p95={linha['lat_p95_s']}s valida={linha['valida']} sinal={linha['sinal_falha']}")
    if not a.manter:
        executar(COMPOSE + ["--profile", "v0", "down", "-v"], check=False)
    log(f"concluído: {caminho}")


if __name__ == "__main__":
    main()
