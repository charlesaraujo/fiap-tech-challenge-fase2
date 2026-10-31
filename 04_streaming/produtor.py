"""Produtor do streaming: simula resultados da edição 2025 chegando no Kinesis (alunos SIM-*).

Manda N eventos a uma taxa fixa. Uma parte é reenvio do mesmo aluno, como aconteceria num retry do
sistema de origem, e alguns eventos vêm inválidos; assim dá para ver o consumidor deduplicando e
mandando para a quarentena. Os municípios são reais, tirados da Silver, para que sigla_uf e regiao
façam sentido.

Uso: python 04_streaming/produtor.py --eventos 600 --por-segundo 20
"""

import argparse
import json
import os
import random
import sys
import time

import boto3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "apoio"))
import athena  # noqa: E402

STREAM = "tc-fase2-eventos"


def municipios() -> list[tuple]:
    cliente = boto3.client("athena")
    r = athena.executar(cliente, "SELECT DISTINCT id_municipio, sigla_uf, regiao FROM tc_fase2_silver.alunos "
                                 "WHERE ano = 2024 AND origem = 'oficial' LIMIT 50")
    linhas = cliente.get_query_results(QueryExecutionId=r["id"])["ResultSet"]["Rows"][1:]
    return [tuple(c["VarCharValue"] for c in linha["Data"]) for linha in linhas]


def evento(n: int, mun: tuple) -> dict:
    alfabetizado = random.random() < 0.6
    return {"ano": 2025, "id_aluno": f"SIM-{n:06d}", "id_municipio": mun[0], "sigla_uf": mun[1], "regiao": mun[2],
            "id_escola": f"SIM-ESC-{n % 40:03d}", "rede": 3, "serie": 2, "caderno": "1", "presente": True,
            "prova_preenchida": True, "avaliado": True, "alfabetizado": alfabetizado,
            "proficiencia": round(random.uniform(745, 900) if alfabetizado else random.uniform(500, 744), 1),
            "peso_aluno": 1.0}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--eventos", type=int, default=600)
    p.add_argument("--por-segundo", type=int, default=20)
    p.add_argument("--duplicados", type=float, default=0.1, help="fração reenviada")
    p.add_argument("--invalidos", type=int, default=5)
    a = p.parse_args()
    random.seed(42)
    muns = municipios()
    kinesis = boto3.client("kinesis")
    enviados = []
    n = 0
    while n < a.eventos:
        lote = []
        for _ in range(a.por_segundo):
            if enviados and random.random() < a.duplicados:
                e = dict(random.choice(enviados))  # reenvio de um aluno já enviado
            else:
                e = evento(len(enviados), random.choice(muns))
                enviados.append(e)
            if n < a.invalidos:
                e = {**e, "id_aluno": f"SIM-INV-{n}", "proficiencia": 9999.0}
            lote.append({**e, "enviado_em": time.time()})
            n += 1
        kinesis.put_records(StreamName=STREAM, Records=[
            {"Data": json.dumps(e).encode(), "PartitionKey": e["id_aluno"]} for e in lote])
        time.sleep(1)
    print(json.dumps({"enviados": n, "alunos_distintos": len(enviados), "reenvios": n - len(enviados) - a.invalidos,
                      "invalidos": a.invalidos}))


if __name__ == "__main__":
    main()
