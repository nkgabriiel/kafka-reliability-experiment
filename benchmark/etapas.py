#!/usr/bin/env python3
"""Roda as etapas de verificação e as matrizes em sequência, guardando UM CSV por etapa.

Cada etapa grava numa pasta própria os dados brutos (CSV), o log da execução e, nas etapas da bateria, os gráficos e
a tabela. Assim os gráficos podem ser refeitos a qualquer momento a partir do CSV (py benchmark/grafico.py CSV
--saida PASTA), sem rodar nada de novo. Um índice com o resultado de cada etapa fica em results/etapas.csv.

Etapas:
    suite          suíte de testes de integração (6 testes)
    geral-a        teste geral A: V0, V5b, V6 e V6b nos 11 cenários principais, 1 repetição
    geral-b        teste geral B: as mesmas versões nos cenários de sensibilidade, 1 repetição
    geral-c        teste geral C: cenários de infraestrutura nas V2, V3 e V4, 1 repetição
    matriz         matriz completa: 11 cenários, 9 versões, n = 5
    sensibilidade  matriz de sensibilidade: 11 cenários, 9 versões, n = 5
Grupos: verificacao (suite, geral-a, geral-b, geral-c), matriz (matriz, sensibilidade) e todas.

Exemplos:
    py benchmark/etapas.py --listar                         # mostra as etapas e a duração estimada
    py benchmark/etapas.py --etapas verificacao             # a verificação inteira, em sequência
    py benchmark/etapas.py --etapas matriz --retomar        # continua uma etapa interrompida
"""
import argparse
import csv
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import resumo_testes  # noqa: E402

RAIZ = Path(__file__).resolve().parent.parent
BENCH = RAIZ / "benchmark"
INDICE = RAIZ / "results" / "etapas.csv"
COLUNAS_INDICE = ["inicio", "etapa", "resultado", "duracao_min", "linhas", "invalidas", "erros", "pasta"]
INFRA = ["queda_broker", "queda_banco", "restart_consumer", "latencia_rede", "particao_rede", "particao_banco",
         "queda_pedidos"]
NOVAS = ["--versoes", "v0", "v5b", "v6", "v6b"]

ETAPAS = {
    "suite": dict(tipo="suite", pasta="results/verificacao/suite",
                  descricao="suíte de testes de integração (6 testes)", estimativa="cerca de 7 min"),
    "geral-a": dict(tipo="bateria", pasta="results/verificacao/geral-a",
                    args=["--perfil", "completo", *NOVAS, "--repeticoes", "1"],
                    descricao="teste geral A: V0, V5b, V6 e V6b nos 11 cenários principais, 1 repetição"),
    "geral-b": dict(tipo="bateria", pasta="results/verificacao/geral-b",
                    args=["--perfil", "sensibilidade", *NOVAS, "--repeticoes", "1"],
                    descricao="teste geral B: V0, V5b, V6 e V6b nos cenários de sensibilidade, 1 repetição"),
    "geral-c": dict(tipo="bateria", pasta="results/verificacao/geral-c",
                    args=["--perfil", "completo", "--versoes", "v2", "v3", "v4", "--cenarios", *INFRA,
                          "--repeticoes", "1"],
                    descricao="teste geral C: cenários de infraestrutura nas V2, V3 e V4, 1 repetição"),
    "matriz": dict(tipo="bateria", pasta="results/bateria/matriz-completa", args=["--perfil", "completo"],
                   descricao="matriz completa: 11 cenários, 9 versões, n = 5"),
    "sensibilidade": dict(tipo="bateria", pasta="results/bateria/matriz-sensibilidade",
                          args=["--perfil", "sensibilidade"],
                          descricao="matriz de sensibilidade: 11 cenários, 9 versões, n = 5"),
}
GRUPOS = {"verificacao": ["suite", "geral-a", "geral-b", "geral-c"], "matriz": ["matriz", "sensibilidade"],
          "todas": ["suite", "geral-a", "geral-b", "geral-c", "matriz", "sensibilidade"]}


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def expandir(nomes):
    """Troca grupos pelas etapas, sem repetir, mantendo a ordem."""
    saida = []
    for nome in nomes:
        for etapa in GRUPOS.get(nome, [nome]):
            if etapa not in ETAPAS:
                sys.exit(f"etapa desconhecida: {etapa} (use --listar)")
            if etapa not in saida:
                saida.append(etapa)
    return saida


def estimativa(etapa):
    cfg = ETAPAS[etapa]
    if cfg["tipo"] == "suite":
        return cfg["estimativa"]
    r = subprocess.run([sys.executable, str(BENCH / "bateria.py"), *cfg["args"], "--estimar"], capture_output=True,
                       text=True, encoding="utf-8", errors="replace")
    return r.stdout.strip().split("= ", 1)[-1] or "?"


def rodar(cmd, cwd, arquivo_log):
    """Roda o comando mostrando a saída na tela e guardando-a no log da etapa. Devolve o código de saída."""
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    with open(arquivo_log, "a", encoding="utf-8") as f:
        p = subprocess.Popen(cmd, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                             encoding="utf-8", errors="replace")
        for linha in p.stdout:
            print(linha, end="", flush=True)
            f.write(linha)
        return p.wait()


def contar(csv_bateria):
    """(linhas, repetições inválidas, repetições com erro) de um CSV da bateria."""
    if not csv_bateria.exists():
        return 0, 0, 0
    with open(csv_bateria, encoding="utf-8") as f:
        linhas = list(csv.DictReader(f))
    return len(linhas), sum(1 for r in linhas if r.get("valida") == "nao"), sum(1 for r in linhas if r.get("erro"))


def registrar(etapa, resultado, inicio, pasta, linhas=0, invalidas=0, erros=0):
    INDICE.parent.mkdir(parents=True, exist_ok=True)
    novo = not INDICE.exists()
    with open(INDICE, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUNAS_INDICE)
        if novo:
            w.writeheader()
        w.writerow(dict(inicio=inicio.isoformat(timespec="seconds"), etapa=etapa, resultado=resultado,
                        duracao_min=round((datetime.now() - inicio).total_seconds() / 60, 1), linhas=linhas,
                        invalidas=invalidas, erros=erros, pasta=str(pasta.relative_to(RAIZ))))


def etapa_suite(pasta):
    mvn = RAIZ / "integration-tests" / ("mvnw.cmd" if os.name == "nt" else "mvnw")
    codigo = rodar([str(mvn), "test"], RAIZ / "integration-tests", pasta / "etapa.log")
    relatorios = RAIZ / "integration-tests" / "target" / "surefire-reports"
    linhas = resumo_testes.resumir(relatorios)
    if linhas:
        resumo_testes.gravar(linhas, pasta / "integracao.csv")
        copia = pasta / "relatorios"
        copia.mkdir(exist_ok=True)
        for xml in relatorios.glob("TEST-*.xml"):
            shutil.copy2(xml, copia / xml.name)
    passaram = sum(1 for l in linhas if l["resultado"] == "passou")
    log(f"suíte: {passaram} de {len(linhas)} testes passaram")
    return codigo == 0 and bool(linhas) and passaram == len(linhas), len(linhas), 0, len(linhas) - passaram


def etapa_bateria(nome, cfg, pasta, retomar, construida, graficos):
    cmd = [sys.executable, str(BENCH / "bateria.py"), *cfg["args"], "--saida", str(pasta)]
    if retomar and (pasta / "bateria.csv").exists():
        cmd.append("--retomar")
    if construida:
        cmd.append("--sem-build")
    codigo = rodar(cmd, RAIZ, pasta / "etapa.log")
    linhas, invalidas, erros = contar(pasta / "bateria.csv")
    if linhas and graficos:
        r = subprocess.run([sys.executable, str(BENCH / "grafico.py"), str(pasta / "bateria.csv"), "--saida",
                            str(pasta)], capture_output=True, text=True, encoding="utf-8", errors="replace")
        log("gráficos e tabela gerados" if r.returncode == 0 else f"gráficos não gerados: {r.stderr.strip()[-200:]}")
    return codigo == 0 and linhas > 0, linhas, invalidas, erros


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--etapas", nargs="+", default=["verificacao"], help="etapas ou grupos (padrão: verificacao)")
    ap.add_argument("--listar", action="store_true", help="mostra as etapas e a duração estimada, e sai")
    ap.add_argument("--retomar", action="store_true", help="continua as etapas da bateria que foram interrompidas")
    ap.add_argument("--continuar-se-falhar", action="store_true", help="segue para a próxima etapa mesmo se uma falhar")
    ap.add_argument("--sem-graficos", action="store_true", help="não gera gráficos e tabela ao fim de cada etapa")
    a = ap.parse_args()
    etapas = expandir(a.etapas)

    if a.listar:
        for e in etapas:
            print(f"{e:14} {estimativa(e):>22}   {ETAPAS[e]['descricao']}   -> {ETAPAS[e]['pasta']}")
        return
    if subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
        sys.exit("O Docker não está rodando. Abra o Docker Desktop e tente de novo.")

    construida = False
    resumo = []
    for e in etapas:
        cfg = ETAPAS[e]
        pasta = RAIZ / cfg["pasta"]
        if cfg["tipo"] == "bateria" and (pasta / "bateria.csv").exists() and not a.retomar:
            sys.exit(f"{cfg['pasta']}/bateria.csv já existe. Use --retomar para continuar essa etapa, ou mova a pasta.")
        pasta.mkdir(parents=True, exist_ok=True)
        log(f"=== etapa {e}: {cfg['descricao']}")
        inicio = datetime.now()
        if cfg["tipo"] == "suite":
            ok, linhas, invalidas, erros = etapa_suite(pasta)
        else:
            ok, linhas, invalidas, erros = etapa_bateria(e, cfg, pasta, a.retomar, construida, not a.sem_graficos)
            construida = True
        registrar(e, "ok" if ok else "falhou", inicio, pasta, linhas, invalidas, erros)
        resumo.append((e, ok, linhas, invalidas, erros))
        log(f"=== etapa {e}: {'OK' if ok else 'FALHOU'} ({linhas} linhas, {invalidas} inválidas, {erros} com erro)")
        if not ok and not a.continuar_se_falhar:
            log("interrompido: corrija e rode de novo (com --retomar, se for uma etapa da bateria)")
            break
    print("\nResumo das etapas:")
    for e, ok, linhas, invalidas, erros in resumo:
        print(f"  {'OK    ' if ok else 'FALHOU'} {e:14} linhas={linhas} inválidas={invalidas} erros={erros}")
    print(f"Índice: {INDICE}")
    sys.exit(0 if all(ok for _, ok, *_ in resumo) else 1)


if __name__ == "__main__":
    main()
