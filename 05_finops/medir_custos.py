"""Mede o consumo real do projeto na conta do Learner Lab e projeta quanto custaria por mês.

De onde vêm os números: tamanho de cada camada no S3; histórico do workgroup tc-fase2 no Athena (bytes
escaneados e cobrados por consulta, já com o mínimo de 10 MB que o Athena cobra); métricas de Lambda e
Step Functions no CloudWatch. Os preços são os da tabela pública de us-east-1 (seção FinOps do README).
A saída é uma tabela em markdown.

Uso: python 05_finops/medir_custos.py > 05_finops/medicao.md
"""

import datetime as dt
import os
from collections import defaultdict

import boto3

BUCKET = os.environ["S3_BUCKET"]
MB, GB, TB = 1024**2, 1024**3, 1024**4
PRECO = {"athena_tb": 5.0, "s3_gb_mes": 0.023, "lambda_gb_s": 0.0000166667, "lambda_req": 0.20 / 1e6,
         "sfn_transicao": 0.025 / 1000, "kinesis_shard_h": 0.015, "alarme_mes": 0.10}
MINIMO_ATHENA = 10 * MB


def s3_por_camada() -> dict:
    tam = defaultdict(int)
    for pagina in boto3.client("s3").get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
        for o in pagina.get("Contents", []):
            tam[o["Key"].split("/")[0]] += o["Size"]
    return dict(tam)


def athena_consultas() -> list[dict]:
    athena = boto3.client("athena")
    ids = [i for p in athena.get_paginator("list_query_executions").paginate(WorkGroup="tc-fase2")
           for i in p["QueryExecutionIds"]]
    saida = []
    for i in range(0, len(ids), 50):
        for q in athena.batch_get_query_execution(QueryExecutionIds=ids[i:i + 50])["QueryExecutions"]:
            if q["Status"]["State"] != "SUCCEEDED" and "DataScannedInBytes" not in q.get("Statistics", {}):
                continue
            b = q["Statistics"].get("DataScannedInBytes", 0)
            saida.append({"inicio": q["Status"]["SubmissionDateTime"], "bytes": b,
                          "cobrado": max(b, MINIMO_ATHENA), "tipo": q.get("StatementType", "")})
    return saida


def cw_soma(namespace: str, metrica: str, dims: list, stat: str = "Sum") -> float:
    fim = dt.datetime.now(dt.timezone.utc)
    r = boto3.client("cloudwatch").get_metric_statistics(
        Namespace=namespace, MetricName=metrica, Dimensions=dims, StartTime=fim - dt.timedelta(days=14),
        EndTime=fim, Period=86400, Statistics=[stat])
    return sum(p[stat] for p in r["Datapoints"])


def main() -> None:
    s3 = s3_por_camada()
    qs = athena_consultas()
    sfn_arn = (f"arn:aws:states:{boto3.session.Session().region_name}:"
               f"{boto3.client('sts').get_caller_identity()['Account']}:stateMachine:tc-fase2")
    transicoes = cw_soma("AWS/States", "ExecutionsStarted", [{"Name": "StateMachineArn", "Value": sfn_arn}])
    lam = {f: (cw_soma("AWS/Lambda", "Invocations", [{"Name": "FunctionName", "Value": f}]),
               cw_soma("AWS/Lambda", "Duration", [{"Name": "FunctionName", "Value": f}]) / 1000)
           for f in ("tc-fase2-sql", "tc-fase2-consumidor")}

    total_b = sum(q["bytes"] for q in qs)
    total_cobrado = sum(q["cobrado"] for q in qs)
    print(f"## Medição (conta do Learner Lab, {boto3.session.Session().region_name})\n")
    print(f"Medido em {dt.date.today():%d/%m/%Y}.\n")
    print("| Recurso | Quantidade medida | Custo no período |\n| --- | --- | --- |")
    for camada, b in sorted(s3.items()):
        print(f"| S3 `{camada}/` | {b / MB:,.1f} MB | US$ {b / GB * PRECO['s3_gb_mes']:.4f}/mês |")
    print(f"| Athena: {len(qs)} consultas | {total_b / GB:,.2f} GB escaneados; {total_cobrado / GB:,.2f} GB cobrados "
          f"(mínimo de 10 MB por consulta) | US$ {total_cobrado / TB * PRECO['athena_tb']:.4f} |")
    for f, (inv, seg) in lam.items():
        custo = seg * 0.25 * PRECO["lambda_gb_s"] + inv * PRECO["lambda_req"]
        print(f"| Lambda `{f}` | {inv:.0f} invocações, {seg:,.0f} s a 256 MB | US$ {custo:.4f} |")
    print(f"| Step Functions | {transicoes:.0f} execuções × ~16 transições | "
          f"US$ {transicoes * 16 * PRECO['sfn_transicao']:.4f} |")

    # Projeção: uma execução por dia, com o perfil da execução de referência (consultas entre início e fim).
    print("\n### Projeção mensal (1 execução por dia, 30 dias)\n")
    print("| Item | Base | US$/mês |\n| --- | --- | --- |")
    s3_total = sum(s3.values())
    print(f"| S3 | {s3_total / MB:,.0f} MB armazenados (cresce ~70 MB por extração nova na Bronze) | "
          f"{s3_total / GB * PRECO['s3_gb_mes']:.3f} |")
    ex = [e for e in boto3.client("stepfunctions").list_executions(stateMachineArn=sfn_arn, statusFilter="SUCCEEDED")
          ["executions"]][0]
    ref = [q for q in qs if ex["startDate"] <= q["inicio"] <= ex["stopDate"]]
    ref_cobrado = sum(q["cobrado"] for q in ref)
    print(f"| Athena | execução de referência `{ex['name']}`: {len(ref)} consultas, "
          f"{sum(q['bytes'] for q in ref) / MB:,.1f} MB escaneados, {ref_cobrado / MB:,.0f} MB cobrados | "
          f"{30 * ref_cobrado / TB * PRECO['athena_tb']:.3f} |")
    seg = (ex["stopDate"] - ex["startDate"]).total_seconds()
    print(f"| Lambda + Step Functions | {seg:.0f} s de Lambda e ~16 transições por execução | "
          f"{30 * (seg * 0.25 * PRECO['lambda_gb_s'] + 16 * PRECO['sfn_transicao']):.3f} |")
    print(f"| Alarme CloudWatch | 1 alarme | {PRECO['alarme_mes']:.2f} |")
    print("| Scheduler, SNS (e-mail), Glue Catalog | dentro do nível gratuito | 0,00 |")
    print(f"| Kinesis (se ficasse ligado) | 1 shard × 730 h | {730 * PRECO['kinesis_shard_h']:.2f} |")


if __name__ == "__main__":
    main()
