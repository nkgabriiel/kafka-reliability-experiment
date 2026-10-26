#!/usr/bin/env python3
"""Gera a tabela-resumo e os gráficos a partir do CSV da bateria (benchmark/bateria.py).

Saídas, na mesma pasta do CSV:
    1_mapa_de_calor.png   a visão geral: % de pedidos com problema, por versão e cenário
    1b_mapa_de_calor_estrito.png   a mesma visão, contando o que foi para a DLQ como não concluído
    2_detalhe_por_cenario.png   o que é o problema em cada caso (duplicado, perdido, perdido mas preservado na DLQ)
    3_custo_latencia.png  o custo: latência (mediana e máximo) por versão e cenário
    5_recuperacao.png     tempo de recuperação: da falha até o último pedido concluir
    6_varredura_janela.png, 7_varredura_taxa_falha.png, 8_varredura_retry.png   famílias de sensibilidade
    4_disponibilidade.png  quantos pedidos o sistema rejeitou na criação (o custo de disponibilidade)
    tabela_resumo.csv / tabela_resumo.md   os números por trás dos gráficos

Requer matplotlib (pip install matplotlib). Uso:
    py benchmark/grafico.py                       # usa o CSV mais recente em results/bateria/
    py benchmark/grafico.py results/bateria/rapido-20261020-031500/bateria.csv
"""
import argparse
import csv
import statistics
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Patch

RAIZ = Path(__file__).resolve().parent.parent

VERSOES = ["v0", "v1", "v2", "v3", "v4", "v5", "v5b", "v6", "v6b"]
NOME_VERSAO = {"v0": "V0\nmonólito\n(controle)", "v1": "V1\nsem proteção", "v2": "V2\n+ producer\nidempotente",
               "v3": "V3\n+ dedupe\nno consumer", "v4": "V4\n+ outbox", "v5": "V5\n+ retry\ne DLQ",
               "v5b": "V5b\n(variante)\n+ cancela\no pedido", "v6": "V6\n+ transações\nKafka",
               "v6b": "V6b\n(variante)\n+ transações\ne outbox"}
CENARIOS = ["kill_ack", "kill_publicacao", "poison", "falha_transitoria", "queda_broker", "queda_banco",
            "restart_consumer", "latencia_rede", "particao_rede", "particao_banco", "queda_pedidos"]
NOME_CENARIO = {"kill_ack": "Queda do pagamento\nna janela do ack",
                "kill_publicacao": "Queda entre o commit\ne a publicação",
                "poison": "Poison message\n(falha sempre)",
                "falha_transitoria": "Falha transitória\n(50% das chamadas)",
                "queda_broker": "Queda do broker\nKafka (8 s)",
                "queda_banco": "Queda do banco\nPostgreSQL (8 s)",
                "restart_consumer": "Restart do consumer\n(rebalanceamento)",
                "latencia_rede": "Latência de rede\n(Kafka, 1,5 s)",
                "particao_rede": "Partição de rede\n(Kafka, 15 s)",
                "particao_banco": "Partição de rede\n(PostgreSQL, 8 s)",
                "queda_pedidos": "Queda do serviço de\npedidos (entrada)"}

# Famílias de sensibilidade: uma curva por versão (eixo x categórico, escala não proporcional)
FAMILIAS = [
    dict(arquivo="6_varredura_janela.png", titulo="Pedidos com problema conforme o tamanho da janela do ack",
         rotulo_x="janela do ack (ms; escala não proporcional)", metrica="problema", rotulo_y="% dos pedidos",
         pontos=[("kill_ack_janela_0", "0"), ("kill_ack_janela_200", "200"), ("kill_ack_janela_400", "400"),
                 ("kill_ack_janela_800", "800"), ("kill_ack", "2000")]),
    dict(arquivo="7_varredura_taxa_falha.png", titulo="Pedidos que não concluem conforme a taxa de falha passageira",
         rotulo_x="falha por tentativa (%; escala não proporcional)", metrica="estrito",
         rotulo_y="% dos pedidos (inclui a DLQ)",
         pontos=[("falha_transitoria_5", "5"), ("falha_transitoria_15", "15"), ("falha_transitoria_30", "30"),
                 ("falha_transitoria", "50")]),
]
RETRY = dict(arquivo="8_varredura_retry.png", versoes=["v5", "v6"],
             pontos=[("retry_1", "1"), ("falha_transitoria", "3"), ("retry_5", "5"), ("retry_9", "9")])
EXTRAS = ["poison_10"]

# Cores: superfície e tinta neutras; categorias nos slots 1 a 3 da paleta validada (azul, laranja, verde-água)
SUPERFICIE, TINTA, TINTA_2, GRADE = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
COR_DUPLICADO, COR_PERDIDO, COR_DLQ, COR_CANCELADO = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
COR_LATENCIA = "#2a78d6"
SEQUENCIAL = LinearSegmentedColormap.from_list("problema", ["#fdf1ea", "#f4a47f", "#eb6834", "#a8400f", "#5b230a"])


def carregar(caminho):
    with open(caminho, encoding="utf-8") as f:
        return [r for r in csv.DictReader(f) if not r.get("erro")]


def numero(valor):
    return float(valor) if valor not in ("", None) else None


def agregar(linhas):
    """Média (e faixa min-max) das repetições de cada (cenário, versão), em % dos pedidos."""
    grupos = {}
    for r in linhas:
        grupos.setdefault((r["cenario"], r["versao"]), []).append(r)
    resumo = {}
    for chave, todas in grupos.items():
        rs = [r for r in todas if r.get("valida") != "nao"]   # a falha não atingiu a janela: não conta
        if not rs:
            continue
        def pct(f):
            return [100.0 * f(r) / float(r["pedidos"]) for r in rs]
        dup = pct(lambda r: float(r["duplicados"]))
        sem_rastro = pct(lambda r: max(0.0, float(r["perdidos"]) - float(r["dlq"])))
        preservados = pct(lambda r: min(float(r["perdidos"]), float(r["dlq"])))
        problema = [d + s_ for d, s_ in zip(dup, sem_rastro)]
        canc = pct(lambda r: float(r.get("cancelados") or 0))
        estrito = pct(lambda r: float(r["duplicados"]) + float(r["perdidos"]) + float(r.get("cancelados") or 0))
        lat = [numero(r["lat_p95_s"]) for r in rs if numero(r["lat_p95_s"]) is not None]
        def media(coluna):
            valores = [numero(r.get(coluna)) for r in rs if numero(r.get(coluna)) is not None]
            return statistics.mean(valores) if valores else None
        resumo[chave] = dict(
            reps=len(rs), pedidos=statistics.mean(float(r["pedidos"]) for r in rs),
            duplicados=statistics.mean(float(r["duplicados"]) for r in rs),
            perdidos=statistics.mean(float(r["perdidos"]) for r in rs),
            dlq=statistics.mean(float(r["dlq"]) for r in rs),
            estoque_extra=statistics.mean(float(r["estoque_extra"]) for r in rs),
            dup=statistics.mean(dup), sem_rastro=statistics.mean(sem_rastro),
            preservados=statistics.mean(preservados), problema=statistics.mean(problema),
            problema_min=min(problema), problema_max=max(problema),
            estrito=statistics.mean(estrito), canc=statistics.mean(canc),
            cancelados=statistics.mean(float(r.get("cancelados") or 0) for r in rs),
            reps_problema=sum(1 for x in problema if x > 0), reps_estrito=sum(1 for x in estrito if x > 0),
            lat_p95=statistics.mean(lat) if lat else None,
            lat_p50=media("lat_p50_s"), lat_max=media("lat_max_s"), recuperacao=media("recuperacao_s"),
            reps_invalidas=len(todas) - len(rs),
            rejeitados=statistics.mean(
                100.0 * float(r.get("criacao_falhou") or 0)
                / (float(r["pedidos"]) + float(r.get("criacao_falhou") or 0)) for r in rs),
        )
    return resumo


def estilo():
    plt.rcParams.update({
        "figure.facecolor": SUPERFICIE, "axes.facecolor": SUPERFICIE, "savefig.facecolor": SUPERFICIE,
        "text.color": TINTA, "axes.labelcolor": TINTA_2, "xtick.color": TINTA_2, "ytick.color": TINTA_2,
        "axes.edgecolor": GRADE, "font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
    })


def versoes_presentes(resumo):
    return [v for v in VERSOES if any(k[1] == v for k in resumo)]


def cenarios_presentes(resumo):
    return [c for c in CENARIOS if any(k[0] == c for k in resumo)]


def mapa_de_calor(resumo, saida, nreps, chave="problema", chave_reps="reps_problema",
                  arquivo="1_mapa_de_calor.png", titulo="Pedidos com problema em cada versão, sob o mesmo teste",
                  subtitulo="Duplicados ou perdidos sem rastro, em % dos pedidos (média das repetições). Quanto mais claro, melhor.",
                  nota="Pedido perdido que ficou na DLQ não conta como problema (foi preservado). A V0 não duplica nem perde, mas rejeita pedidos na queda (figura 4)."):
    vs, cs = versoes_presentes(resumo), cenarios_presentes(resumo)
    matriz = [[resumo.get((c, v), {}).get(chave) for v in vs] for c in cs]
    topo = max([x for linha in matriz for x in linha if x is not None] + [1.0])
    largura = max(11.5, 1.9 * len(vs) + 3.4)
    altura = 0.95 * len(cs) + 2.3
    fig, ax = plt.subplots(figsize=(largura, altura))
    for i, c in enumerate(cs):
        for j, v in enumerate(vs):
            valor = matriz[i][j]
            if valor is None:
                if v == "v0":     # o monólito não tem Kafka: esses cenários não se aplicam a ele
                    ax.add_patch(plt.Rectangle((j, len(cs) - 1 - i), 1, 1, facecolor="#ecebe7",
                                               edgecolor=SUPERFICIE, linewidth=3))
                    ax.text(j + 0.5, len(cs) - 1 - i + 0.5, "n/a", ha="center", va="center", fontsize=12,
                            color=TINTA_2)
                continue
            cor = SEQUENCIAL(valor / topo)
            ax.add_patch(plt.Rectangle((j, len(cs) - 1 - i), 1, 1, facecolor=cor, edgecolor=SUPERFICIE, linewidth=3))
            claro = (0.299 * cor[0] + 0.587 * cor[1] + 0.114 * cor[2]) > 0.55
            tinta = TINTA if claro else "#ffffff"
            ax.text(j + 0.5, len(cs) - 1 - i + 0.57, f"{valor:.0f}%" if valor >= 9.5 else f"{valor:.1f}%",
                    ha="center", va="center", fontsize=14, fontweight="bold", color=tinta)
            r = resumo[(cs[i], vs[j])]
            ax.text(j + 0.5, len(cs) - 1 - i + 0.2, f"falhou em {r[chave_reps]} de {r['reps']} rep.",
                    ha="center", va="center", fontsize=7.5, color=tinta)
    ax.set_xlim(0, len(vs))
    ax.set_ylim(0, len(cs))
    ax.set_xticks([j + 0.5 for j in range(len(vs))], [NOME_VERSAO[v] for v in vs], fontsize=9)
    ax.set_yticks([len(cs) - 1 - i + 0.5 for i in range(len(cs))], [NOME_CENARIO[c] for c in cs], fontsize=9.5)
    ax.xaxis.tick_top()
    ax.tick_params(length=0)
    for lado in ax.spines.values():
        lado.set_visible(False)
    fig.suptitle(titulo, x=0.02, ha="left",
                 fontsize=15, fontweight="bold", y=1 - 0.1 / altura)
    fig.text(0.02, 1 - 0.62 / altura, subtitulo, fontsize=10, color=TINTA_2)
    fig.text(0.02, 0.16 / altura, f"Até {nreps} repetições por célula. {nota}", fontsize=8.5, color=TINTA_2)
    fig.subplots_adjust(top=1 - 1.75 / altura, bottom=0.6 / altura, left=2.7 / largura, right=0.98)
    fig.savefig(saida / arquivo, dpi=160)
    plt.close(fig)


def detalhe(resumo, saida):
    vs, cs = versoes_presentes(resumo), cenarios_presentes(resumo)
    colunas = 2 if len(cs) > 1 else 1
    linhas = (len(cs) + 1) // 2
    fig, eixos = plt.subplots(linhas, colunas, figsize=(6.6 * colunas, 3.9 * linhas), squeeze=False)
    for ax in eixos.flat[len(cs):]:
        ax.set_visible(False)
    for ax, c in zip(eixos.flat, cs):
        base = [0.0] * len(vs)
        for chave, cor in (("dup", COR_DUPLICADO), ("sem_rastro", COR_PERDIDO), ("preservados", COR_DLQ),
                           ("canc", COR_CANCELADO)):
            altura = [resumo.get((c, v), {}).get(chave, 0.0) for v in vs]
            ax.bar(range(len(vs)), altura, bottom=base, width=0.52, color=cor, edgecolor=SUPERFICIE, linewidth=1.6)
            base = [b + h for b, h in zip(base, altura)]
        for i, total in enumerate(base):
            texto = "n/a" if (c, vs[i]) not in resumo else (f"{total:.1f}%" if total else "0%")
            ax.text(i, total + 0.02 * max(max(base), 1), texto, ha="center", va="bottom", fontsize=9,
                    color=TINTA_2 if texto == "n/a" else TINTA)
        ax.set_xticks(range(len(vs)), [NOME_VERSAO[v] for v in vs], fontsize=7)
        ax.set_ylim(0, max(max(base), 1) * 1.25)
        ax.set_ylabel("% dos pedidos")
        ax.set_title(NOME_CENARIO[c].replace("\n", " "), loc="left", fontsize=11, fontweight="bold")
        ax.yaxis.grid(True, color=GRADE, linewidth=0.8)
        ax.set_axisbelow(True)
    fig.legend(handles=[Patch(color=COR_DUPLICADO, label="duplicados (mais de 1 pagamento)"),
                        Patch(color=COR_PERDIDO, label="perdidos sem rastro (nunca concluem)"),
                        Patch(color=COR_DLQ, label="perdidos, mas preservados na DLQ"),
                        Patch(color=COR_CANCELADO, label="cancelados por falha técnica (V5b)")],
               loc="lower center", ncol=4, frameon=False, fontsize=8.5)
    fig.suptitle("O que acontece com os pedidos afetados", x=0.02, ha="left", fontsize=15, fontweight="bold")
    fig.tight_layout(rect=(0, 0.06, 1, 0.95))
    fig.savefig(saida / "2_detalhe_por_cenario.png", dpi=160)
    plt.close(fig)


def barras_simples(resumo, saida, chave, titulo, rotulo_y, arquivo, formato, chave_max=None):
    """Uma barra por versão, um painel por cenário (formas de uma série só: sem legenda, o título diz o que é).
    Com chave_max, o valor máximo aparece anotado abaixo do valor da barra."""
    vs, cs = versoes_presentes(resumo), cenarios_presentes(resumo)
    colunas = 2 if len(cs) > 1 else 1
    linhas = (len(cs) + 1) // 2
    fig, eixos = plt.subplots(linhas, colunas, figsize=(6.6 * colunas, 3.6 * linhas), squeeze=False)
    for ax in eixos.flat[len(cs):]:
        ax.set_visible(False)
    for ax, c in zip(eixos.flat, cs):
        brutos = [resumo.get((c, v), {}).get(chave) for v in vs]
        valores = [b or 0.0 for b in brutos]
        maximos = [resumo.get((c, v), {}).get(chave_max) if chave_max else None for v in vs]
        ax.bar(range(len(vs)), valores, width=0.52, color=COR_LATENCIA)
        for i, valor in enumerate(valores):
            if (c, vs[i]) not in resumo:
                texto = "n/a"
            elif brutos[i] is None:
                texto = "sem dado"
            else:
                texto = formato.format(valor)
                if maximos[i] is not None:
                    texto += chr(10) + "(máx " + formato.format(maximos[i]) + ")"
            ax.text(i, valor + 0.02 * max(max(valores), 1), texto, ha="center", va="bottom", fontsize=8.5,
                    color=TINTA_2 if texto in ("n/a", "sem dado") else TINTA)
        ax.set_xticks(range(len(vs)), [NOME_VERSAO[v] for v in vs], fontsize=7)
        ax.set_ylim(0, max(max(valores), 1) * (1.4 if chave_max else 1.25))
        ax.set_ylabel(rotulo_y)
        ax.set_title(NOME_CENARIO[c].replace(chr(10), " "), loc="left", fontsize=11, fontweight="bold")
        ax.yaxis.grid(True, color=GRADE, linewidth=0.8)
        ax.set_axisbelow(True)
    fig.suptitle(titulo, x=0.02, ha="left", fontsize=15, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(saida / arquivo, dpi=160)
    plt.close(fig)


def curva_por_versao(resumo, saida, fam):
    """Um painel por versão, com a mesma escala vertical, para comparar o formato da curva entre versões."""
    vs = [v for v in versoes_presentes(resumo) if any((c, v) in resumo for c, _ in fam["pontos"])]
    if not vs or sum(1 for c, _ in fam["pontos"] if any((c, v) in resumo for v in vs)) < 2:
        return                      # uma curva precisa de pelo menos dois pontos medidos
    ymax = max([resumo[(c, v)][fam["metrica"]] for v in vs for c, _ in fam["pontos"] if (c, v) in resumo] + [1.0]) * 1.2
    colunas = min(4, len(vs))
    linhas = (len(vs) + colunas - 1) // colunas
    fig, eixos = plt.subplots(linhas, colunas, figsize=(3.6 * colunas + 0.8, 3.0 * linhas + 1.0), squeeze=False,
                              sharey=True)
    for ax in eixos.flat[len(vs):]:
        ax.set_visible(False)
    for ax, v in zip(eixos.flat, vs):
        xs = [i for i, (c, _) in enumerate(fam["pontos"]) if (c, v) in resumo]
        ys = [resumo[(fam["pontos"][i][0], v)][fam["metrica"]] for i in xs]
        ax.plot(xs, ys, color=COR_LATENCIA, linewidth=2, marker="o", markersize=7, markeredgecolor=SUPERFICIE,
                markeredgewidth=2)
        if xs:
            ax.text(xs[-1], ys[-1] + 0.03 * ymax, f"{ys[-1]:.1f}%", ha="center", va="bottom", fontsize=8.5)
        ax.set_xticks(range(len(fam["pontos"])), [rot for _, rot in fam["pontos"]], fontsize=8)
        ax.set_xlim(-0.4, len(fam["pontos"]) - 0.6)
        ax.set_ylim(0, ymax)
        ax.set_title(NOME_VERSAO[v].replace(chr(10), " "), fontsize=9.5, fontweight="bold", loc="left")
        ax.yaxis.grid(True, color=GRADE, linewidth=0.8)
        ax.set_axisbelow(True)
    fig.supxlabel(fam["rotulo_x"], fontsize=9, color=TINTA_2)
    fig.supylabel(fam["rotulo_y"], fontsize=9, color=TINTA_2)
    fig.suptitle(fam["titulo"], x=0.02, ha="left", fontsize=14, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    fig.savefig(saida / fam["arquivo"], dpi=160)
    plt.close(fig)


def curva_retry(resumo, saida):
    """Quanto vai para a DLQ e quanto custa em latência, conforme o número de retentativas (só V5 e V6)."""
    pontos = RETRY["pontos"]
    vs = [v for v in RETRY["versoes"] if any((c, v) in resumo for c, _ in pontos)]
    if not vs or sum(1 for c, _ in pontos if any((c, v) in resumo for v in vs)) < 2:
        return                      # uma curva precisa de pelo menos dois pontos medidos
    cores = {"v5": COR_DUPLICADO, "v6": COR_PERDIDO}      # slots 1 e 2 da paleta
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.5, 4.4))
    for chave, ax, rotulo, formato in (("estrito", ax1, "% dos pedidos que foram para a DLQ", "{:.1f}%"),
                                       ("lat_max", ax2, "latência máxima (s)", "{:.0f} s")):
        for v in vs:
            xs = [i for i, (c, _) in enumerate(pontos) if (c, v) in resumo and resumo[(c, v)][chave] is not None]
            ys = [resumo[(pontos[i][0], v)][chave] for i in xs]
            # a V6 vai tracejada e com marcador quadrado: se as duas curvas coincidirem, ambas continuam visíveis
            ax.plot(xs, ys, color=cores[v], linewidth=2, linestyle="--" if v == "v6" else "-",
                    marker="s" if v == "v6" else "o", markersize=7, markeredgecolor=SUPERFICIE,
                    markeredgewidth=2, label=NOME_VERSAO[v].replace(chr(10), " "))
            if xs:
                ax.text(xs[-1] + 0.06, ys[-1], formato.format(ys[-1]), va="center", fontsize=8.5)
        ax.set_xticks(range(len(pontos)), [rot for _, rot in pontos])
        ax.set_xlim(-0.3, len(pontos) - 0.4)
        ax.set_xlabel("retentativas do retry (escala não proporcional)", fontsize=9, color=TINTA_2)
        ax.set_ylabel(rotulo, fontsize=9, color=TINTA_2)
        ax.set_ylim(bottom=0)
        ax.yaxis.grid(True, color=GRADE, linewidth=0.8)
        ax.set_axisbelow(True)
    ax1.legend(frameon=False, fontsize=9)
    fig.suptitle("Retry: o que vai para a DLQ e o que custa, conforme o número de retentativas (falha de 50%)",
                 x=0.02, ha="left", fontsize=13, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    fig.savefig(saida / RETRY["arquivo"], dpi=160)
    plt.close(fig)


def todos_os_cenarios(resumo):
    """Principais, mais os de sensibilidade, na ordem de apresentação (a tabela mostra todos)."""
    ordem = list(CENARIOS) + [c for fam in FAMILIAS for c, _ in fam["pontos"]] + [c for c, _ in RETRY["pontos"]] + EXTRAS
    vistos = []
    for c in ordem:
        if c not in vistos and any(k[0] == c for k in resumo):
            vistos.append(c)
    return vistos


def fmt(valor):
    return f"{valor:.1f}" if valor is not None else ""


def tabela(resumo, saida):
    cabecalho = ["cenario", "versao", "reps", "pedidos", "duplicados", "perdidos", "dlq", "cancelados", "estoque_extra",
                 "problema_pct", "reps_com_problema", "rejeitados_pct", "lat_p50_s", "lat_max_s", "recuperacao_s",
                 "reps_invalidas"]
    linhas = []
    for c in todos_os_cenarios(resumo):
        for v in versoes_presentes(resumo):
            r = resumo.get((c, v))
            if r:
                linhas.append([c, v, r["reps"], f"{r['pedidos']:.0f}", f"{r['duplicados']:.1f}", f"{r['perdidos']:.1f}",
                               f"{r['dlq']:.1f}", f"{r['cancelados']:.1f}", f"{r['estoque_extra']:.1f}", f"{r['problema']:.1f}",
                               f"{r['reps_problema']}/{r['reps']}", f"{r['rejeitados']:.1f}",
                               fmt(r['lat_p50']), fmt(r['lat_max']), fmt(r['recuperacao']), r['reps_invalidas']])
    with open(saida / "tabela_resumo.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cabecalho)
        w.writerows(linhas)
    md = ["| " + " | ".join(cabecalho) + " |", "|" + "---|" * len(cabecalho)]
    md += ["| " + " | ".join(str(x) for x in linha) + " |" for linha in linhas]
    (saida / "tabela_resumo.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print("\n".join(md))


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("csv", nargs="*", help="um ou mais CSVs da bateria (juntados); padrão: o mais recente em results/bateria/")
    ap.add_argument("--saida", help="pasta de saída (padrão: a pasta do primeiro CSV)")
    a = ap.parse_args()
    if a.csv:
        caminhos = [Path(c) for c in a.csv]
    else:
        candidatos = sorted((RAIZ / "results" / "bateria").glob("*/bateria.csv"), key=lambda p: p.stat().st_mtime)
        if not candidatos:
            sys.exit("nenhum bateria.csv encontrado em results/bateria/")
        caminhos = [candidatos[-1]]
    linhas = [r for c in caminhos for r in carregar(c)]
    resumo = agregar(linhas)
    saida = Path(a.saida) if a.saida else caminhos[0].parent
    saida.mkdir(parents=True, exist_ok=True)
    nreps = max(r["reps"] for r in resumo.values())
    estilo()
    mapa_de_calor(resumo, saida, nreps)
    mapa_de_calor(resumo, saida, nreps, chave="estrito", chave_reps="reps_estrito",
                  arquivo="1b_mapa_de_calor_estrito.png",
                  titulo="Pedidos que não concluíram com sucesso, contando a DLQ e os cancelados",
                  subtitulo="Duplicados, perdidos (inclusive os na DLQ) ou cancelados por falha técnica, em % dos pedidos. Quanto mais claro, melhor.",
                  nota="Leitura mais dura que a figura 1: a DLQ guarda a mensagem e o cancelamento dá estado final, mas o pedido continua sem concluir.")
    detalhe(resumo, saida)
    barras_simples(resumo, saida, "lat_p50", "Custo: tempo para o pedido concluir (mediana e máximo)", "segundos",
                   "3_custo_latencia.png", "{:.1f} s", chave_max="lat_max")
    barras_simples(resumo, saida, "recuperacao", "Tempo de recuperação: da falha até o último pedido concluir",
                   "segundos", "5_recuperacao.png", "{:.0f} s")
    barras_simples(resumo, saida, "rejeitados", "Disponibilidade: pedidos rejeitados na criação (% das tentativas)",
                   "% das tentativas", "4_disponibilidade.png", "{:.0f}%")
    for fam in FAMILIAS:
        curva_por_versao(resumo, saida, fam)
    curva_retry(resumo, saida)
    tabela(resumo, saida)
    print(f"\ngráficos e tabela gravados em {saida}")


if __name__ == "__main__":
    main()
