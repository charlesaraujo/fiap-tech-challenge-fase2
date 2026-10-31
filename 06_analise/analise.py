"""Teste estatístico e gráficos a partir do dataset analítico exportado (dados_fase3/). Não precisa da AWS.

1. Wilcoxon pareado (postos sinalizados): a taxa de alfabetização da rede municipal mudou de 2023 para 2024?
   Pareamos por município (o mesmo município nos dois anos) e usamos um teste não paramétrico porque as
   variações não seguem uma normal e municípios pequenos têm taxas muito ruidosas. Usamos a aproximação
   normal com correção de empates e de continuidade (com n perto de 4.800 ela é praticamente exata) e
   descartamos as diferenças zero, como no método original. Tamanho do efeito: r = z / √n. Como teste de
   sensibilidade, repetimos só com municípios que têm n efetivo ≥ 30 nos dois anos.
2. Gráficos em PNG em 06_analise/graficos/ e os números citados no vídeo em 06_analise/numeros.json.

Uso: python 06_analise/analise.py
"""

import json
import math
import os

import matplotlib.pyplot as plt
import pandas as pd

PASTA = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.join(PASTA, "..")
SAIDA = os.path.join(PASTA, "graficos")
# Cores: azul para a série principal, laranja para a segunda série e cinza para o neutro.
AZUL, LARANJA, CINZA, TEXTO, TEXTO2, GRADE = "#2a78d6", "#eb6834", "#898781", "#0b0b0b", "#52514e", "#e1e0d9"
BOM, CRITICO = "#0ca30c", "#d03b3b"


def wilcoxon(x: pd.Series, y: pd.Series) -> dict:
    d = (y - x).dropna()
    d = d[d != 0]
    n = len(d)
    postos = d.abs().rank()
    w_mais = postos[d > 0].sum()
    media = n * (n + 1) / 4
    empates = postos.value_counts()
    var = n * (n + 1) * (2 * n + 1) / 24 - ((empates**3 - empates).sum()) / 48
    z = (w_mais - media - math.copysign(0.5, w_mais - media)) / math.sqrt(var)
    return {"n": n, "w_mais": w_mais, "z": z, "p": math.erfc(abs(z) / math.sqrt(2)), "r": z / math.sqrt(n),
            "mediana_pp": d.median(), "pct_melhorou": (d > 0).mean() * 100}


def tabela(nome: str) -> pd.DataFrame:
    return pd.read_parquet(os.path.join(RAIZ, "dados_fase3", f"{nome}.parquet"))


def estilo(ax, titulo: str, subtitulo: str = "") -> None:
    ax.set_title(titulo, loc="left", fontsize=13, color=TEXTO, pad=22 if subtitulo else 8, fontweight="bold")
    if subtitulo:
        ax.text(0, 1.02, subtitulo, transform=ax.transAxes, fontsize=10, color=TEXTO2)
    for lado in ("top", "right"):
        ax.spines[lado].set_visible(False)
    for lado in ("left", "bottom"):
        ax.spines[lado].set_color("#c3c2b7")
    ax.tick_params(colors=TEXTO2, labelsize=9)
    ax.grid(axis="y", color=GRADE, linewidth=0.6)
    ax.set_axisbelow(True)


def salvar(fig, nome: str) -> None:
    fig.savefig(os.path.join(SAIDA, nome), dpi=200, bbox_inches="tight", facecolor="#fcfcfb")
    plt.close(fig)


def main() -> None:
    os.makedirs(SAIDA, exist_ok=True)
    ml = tabela("ml_features")  # features de 2023 e alvo de 2024, rede municipal

    # Mesma medida nos dois anos: taxa oficial de 2023 x taxa oficial de 2024 (o alvo).
    todos = wilcoxon(ml.taxa_oficial, ml.alvo_taxa_ano_seguinte)
    res = tabela("fato_resultado")
    mun = res[(res.nivel == "municipio") & (res.rede == 3)].pivot(index="id_localidade", columns="ano",
                                                                  values="n_efetivo")
    grandes = mun[(mun[2023] >= 30) & (mun[2024] >= 30)].index
    robusto = wilcoxon(*[ml.set_index("id_municipio").loc[lambda d: d.index.isin(grandes), c]
                         for c in ("taxa_oficial", "alvo_taxa_ano_seguinte")])
    for nome, r in (("todos os municípios", todos), ("n efetivo ≥ 30 nos dois anos", robusto)):
        print(f"Wilcoxon 2023→2024, rede municipal, {nome}: n={r['n']}, mediana {r['mediana_pp']:+.2f} p.p., "
              f"{r['pct_melhorou']:.1f}% melhoraram, z={r['z']:.2f}, p={r['p']:.2e}, r={r['r']:.2f}")

    # 1. Distribuição da variação por município
    d = ml.alvo_taxa_ano_seguinte - ml.taxa_oficial
    fig, ax = plt.subplots(figsize=(9, 4.6))
    ax.hist(d.clip(-40, 40), bins=80, color=AZUL, edgecolor="#fcfcfb", linewidth=0.5)
    ax.axvline(0, color=CINZA, linewidth=1)
    ax.axvline(todos["mediana_pp"], color=LARANJA, linewidth=2)
    rotulo_mediana = f"mediana {todos['mediana_pp']:+.1f} p.p.".replace(".", ",", 1)
    ax.text(todos["mediana_pp"] + 1, ax.get_ylim()[1] * 0.92, rotulo_mediana,
            color=TEXTO, fontsize=10)
    ax.set_xlabel("variação da taxa de alfabetização 2023 → 2024 (p.p., cortada em ±40)", color=TEXTO2)
    ax.set_ylabel("municípios", color=TEXTO2)
    estilo(ax, "A maioria dos municípios melhorou de 2023 para 2024",
           f"{todos['pct_melhorou']:.0f}% dos {todos['n']:,} municípios subiram. Wilcoxon pareado: ".replace(",", ".")
           + f"p < 0,001, mas efeito pequeno (r = {todos['r']:.2f})".replace(".", ","))
    salvar(fig, "01_variacao_municipios.png")

    # 2. Brasil: trajetória x meta do ano x meta pactuada 2030 (cenário linear, não previsão)
    ev = tabela("evolucao").query("nivel == 'brasil'").sort_values("ano")
    mt = tabela("fato_meta").query("nivel == 'brasil'").sort_values("ano")
    fig, ax = plt.subplots(figsize=(9, 4.6))
    ax.plot(ev.ano, ev.taxa, color=AZUL, linewidth=2, marker="o", markersize=7, label="taxa oficial (pública)")
    ultimo = ev.iloc[-1]
    ax.plot([ultimo.ano, 2030], [ultimo.taxa, min(ultimo.cenario_linear_2030, 100)], color=AZUL, linewidth=2,
            linestyle=(0, (3, 3)), label="cenário linear, não previsão (ritmo médio de 2023→25)")
    ax.plot(mt.ano, mt.meta_do_ano, color=LARANJA, linewidth=0, marker="D", markersize=7, label="meta do ano")
    ax.plot([2030], [80], color=LARANJA, marker="*", markersize=14, linewidth=0, label="meta pactuada 2030 (80%)")
    meta_ano = dict(zip(mt.ano, mt.meta_do_ano))
    for a, t in zip(ev.ano, ev.taxa):
        acima = meta_ano.get(a, 101) < t  # rótulo do lado oposto ao losango da meta
        ax.annotate(f"{t:.1f}".replace(".", ","), (a, t), textcoords="offset points", xytext=(0, 10 if acima else -18),
                    ha="center", color=TEXTO,
                    fontsize=9)
    ax.set_xticks(range(2023, 2031))
    ax.set_ylim(40, 100)
    ax.set_ylabel("% de alunos alfabetizados", color=TEXTO2)
    ax.legend(frameon=False, fontsize=9, labelcolor=TEXTO2, loc="upper left")
    estilo(ax, "Brasil: 55,9% → 66,0% em dois anos",
           "Rede pública, 2º ano. 2024 abaixo da meta do ano (59,2 x 59,9); 2025 acima (66,0 x 64,0).")
    salvar(fig, "02_brasil_trajetoria_meta.png")

    # 3. Meta x resultado 2024 nos municípios (status sempre com rótulo)
    st = tabela("fato_meta").query("nivel == 'municipio' and ano == 2024").status_meta.value_counts()
    rotulos = {"atingiu": ("✓ atingiu", BOM), "nao_atingiu": ("✗ não atingiu", CRITICO),
               "sem_meta": ("sem meta", CINZA)}
    fig, ax = plt.subplots(figsize=(9, 3.2))
    for i, (k, (rot, cor)) in enumerate(rotulos.items()):
        v = st.get(k, 0)
        ax.barh(i, v, color=cor, height=0.6)
        ax.text(v + 30, i, f"{rot}: {v:,} ({v / st.sum() * 100:.0f}%)".replace(",", "."), va="center",
                color=TEXTO, fontsize=10)
    ax.set_yticks([])
    ax.invert_yaxis()
    ax.set_xlim(0, st.max() * 1.35)
    ax.set_xlabel("municípios (rede municipal)", color=TEXTO2)
    estilo(ax, "Meta de 2024: pouco mais da metade dos municípios atingiu")
    ax.grid(axis="x", color=GRADE, linewidth=0.6)
    ax.grid(axis="y", visible=False)
    salvar(fig, "03_meta_x_resultado_2024.png")

    # 4. Validação cruzada: taxa ponderada dos microdados x taxa oficial
    v = res[(res.nivel == "municipio") & (res.ano == 2024) & res.taxa_oficial.notna()]
    dentro = ((v.taxa_oficial >= v.ic95_inferior) & (v.taxa_oficial <= v.ic95_superior)).mean() * 100
    fig, ax = plt.subplots(figsize=(6.2, 6))
    ax.plot([0, 100], [0, 100], color=CINZA, linewidth=1)
    ax.scatter(v.taxa_oficial, v.taxa_calculada, s=9, color=AZUL, alpha=0.5, linewidths=0)
    ax.set_xlabel("taxa oficial (INEP, %)", color=TEXTO2)
    ax.set_ylabel("taxa calculada dos microdados (%)", color=TEXTO2)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    estilo(ax, "Validação cruzada 2024",
           f"{dentro:.2f}% dos {len(v):,} pares município × rede com o oficial dentro do IC 95%"
           .replace(",", "X").replace(".", ",").replace("X", "."))
    salvar(fig, "04_validacao_cruzada_2024.png")
    print(f"gráficos em {os.path.relpath(SAIDA, RAIZ)}/; oficial dentro do IC: {dentro:.2f}% de {len(v)}")

    # Números citados no vídeo: saem daqui para que a fala e o repositório digam a mesma coisa.
    numeros = {
        "wilcoxon": {k: float(v) for k, v in todos.items()},
        "wilcoxon_n30": {k: float(v) for k, v in robusto.items()},
        "brasil": {int(a): float(t) for a, t in zip(ev.ano, ev.taxa)},
        "brasil_meta": {int(a): float(m) for a, m in zip(mt.ano, mt.meta_do_ano) if pd.notna(m)},
        "cenario_2030": float(min(ev.iloc[-1].cenario_linear_2030, 100)),
        "meta_2024_municipios": {k: int(st.get(k, 0)) for k in rotulos},
        "validacao_dentro_ic_pct": round(dentro, 2),
        "validacao_pares": int(len(v)),
        "ml_features_municipios": int(len(ml)),
    }
    with open(os.path.join(PASTA, "numeros.json"), "w", encoding="utf-8") as f:
        json.dump(numeros, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
