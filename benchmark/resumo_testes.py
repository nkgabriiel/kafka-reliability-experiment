#!/usr/bin/env python3
"""Resume em um CSV os relatórios do Surefire (a suíte de testes de integração).

Cada teste vira uma linha: classe, teste, resultado (passou, falhou, erro, ignorado), tempo e, quando o teste
imprime, as linhas [METRICA] (ex.: o atraso medido no teste do retry). Guardar o CSV (e os XMLs originais) permite
documentar o resultado da suíte sem rodá-la de novo.

    py benchmark/resumo_testes.py                       # lê integration-tests/target/surefire-reports
    py benchmark/resumo_testes.py PASTA --saida arquivo.csv
"""
import argparse
import csv
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
PADRAO = RAIZ / "integration-tests" / "target" / "surefire-reports"
COLUNAS = ["data", "classe", "teste", "resultado", "tempo_s", "mensagem", "metricas"]


def metricas_de(saida):
    """As linhas [METRICA] que o teste imprimiu (o Surefire as guarda dentro do testcase)."""
    return " | ".join(l.strip() for l in saida.splitlines() if "[METRICA]" in l)


def resumir(pasta):
    """Lê os TEST-*.xml da pasta e devolve uma lista de dicts, um por teste."""
    linhas = []
    for arquivo in sorted(Path(pasta).glob("TEST-*.xml")):
        suite = ET.parse(arquivo).getroot()
        saida_suite = suite.findtext("system-out") or ""
        data = datetime.fromtimestamp(arquivo.stat().st_mtime).isoformat(timespec="seconds")
        casos = list(suite.iter("testcase"))
        for caso in casos:
            resultado, mensagem = "passou", ""
            for marca, nome in (("failure", "falhou"), ("error", "erro"), ("skipped", "ignorado")):
                filho = caso.find(marca)
                if filho is not None:
                    resultado = nome
                    mensagem = " ".join((filho.get("message") or filho.text or "").split())[:300]
                    break
            linhas.append(dict(
                data=data, classe=(caso.get("classname") or suite.get("name") or "").rsplit(".", 1)[-1],
                teste=caso.get("name"), resultado=resultado, tempo_s=caso.get("time"), mensagem=mensagem,
                metricas=metricas_de((caso.findtext("system-out") or "") + saida_suite)))
    return linhas


def gravar(linhas, destino):
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    with open(destino, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUNAS)
        w.writeheader()
        w.writerows(linhas)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("pasta", nargs="?", default=str(PADRAO), help="pasta dos relatórios do Surefire")
    ap.add_argument("--saida", default=str(RAIZ / "results" / "verificacao" / "suite" / "integracao.csv"))
    a = ap.parse_args()
    linhas = resumir(a.pasta)
    if not linhas:
        sys.exit(f"nenhum relatório TEST-*.xml em {a.pasta}")
    gravar(linhas, a.saida)
    for l in linhas:
        print(f"{l['resultado']:8} {l['tempo_s']:>8} s  {l['classe']}.{l['teste']}")
    print(f"\n{sum(l['resultado'] == 'passou' for l in linhas)} de {len(linhas)} passaram -> {a.saida}")


if __name__ == "__main__":
    main()
