"""Roda arquivos .sql no Athena, um comando de cada vez, na ordem em que aparecem.

Antes de enviar, troca ${VAR} pelos valores: BUCKET vem do .env; RUN_ID é a extração da Bronze que
vamos processar (se não for informado, pegamos o último manifesto em bronze/_manifestos/); EXEC_ID
identifica esta execução e é gerado quando não vem; outras variáveis podem ser passadas com --var.
O script espera cada consulta terminar e para no primeiro erro. Para cada comando imprime os bytes
escaneados, que usamos depois no levantamento de custos.

Todas as consultas vão para o workgroup tc-fase2, que guarda os resultados em
s3://BUCKET/athena-results/ e limita os bytes lidos por consulta.

Exemplos:
    python apoio/athena.py sql/bronze/01_tabelas.sql
    python apoio/athena.py sql/silver/*.sql --var RUN_ID=a3e3036c5911
    python apoio/athena.py -q "select count(*) from tc_fase2_bronze.alunos"
"""

import argparse
import json
import os
import re
import time
import uuid

import boto3

WORKGROUP = "tc-fase2"
LIMITE_BYTES = 1024**3  # 1 GiB por consulta; a fonte inteira tem uns 70 MB em Parquet


def garantir_workgroup(athena, bucket: str) -> None:
    if WORKGROUP in [w["Name"] for w in athena.list_work_groups()["WorkGroups"]]:
        return
    athena.create_work_group(
        Name=WORKGROUP,
        Configuration={
            "ResultConfiguration": {"OutputLocation": f"s3://{bucket}/athena-results/"},
            "EnforceWorkGroupConfiguration": True,
            "PublishCloudWatchMetricsEnabled": True,
            "BytesScannedCutoffPerQuery": LIMITE_BYTES,
        },
        Tags=[{"Key": "projeto", "Value": "tc-fase2"}],
    )


def executar(athena, sql: str) -> dict:
    qid = athena.start_query_execution(QueryString=sql, WorkGroup=WORKGROUP)["QueryExecutionId"]
    while True:
        q = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        estado = q["Status"]["State"]
        if estado in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if estado != "SUCCEEDED":
        raise SystemExit(f"{estado}: {q['Status'].get('StateChangeReason')}\n{sql}")
    linhas = 0
    if q.get("StatementType") in ("DML", "DDL"):  # INSERT, MERGE e CTAS devolvem as linhas escritas
        linhas = athena.get_query_results(QueryExecutionId=qid, MaxResults=1).get("UpdateCount", 0)
    return {"id": qid, "bytes": q["Statistics"].get("DataScannedInBytes", 0), "linhas": linhas}


def ultimo_run_id(bucket: str) -> str:
    objetos = boto3.client("s3").list_objects_v2(Bucket=bucket, Prefix="bronze/_manifestos/")["Contents"]
    objetos = [o for o in objetos if o["Key"].endswith(".json")]
    ultimo = max(objetos, key=lambda o: o["LastModified"])["Key"]
    return re.search(r"run_id=(\w+)\.json", ultimo).group(1)


def comandos(caminho: str, variaveis: dict) -> list[str]:
    texto = re.sub(r"\$\{(\w+)\}", lambda m: variaveis[m.group(1)], open(caminho, encoding="utf-8").read())
    texto = re.sub(r"--[^\n]*", "", texto)  # tira os comentários antes, porque podem ter ';'
    return [c.strip() for c in texto.split(";") if c.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("arquivos", nargs="*")
    parser.add_argument("-q", "--query", help="executa uma consulta e imprime o resultado")
    parser.add_argument("--var", action="append", default=[], help="NOME=valor")
    args = parser.parse_args()
    bucket = os.environ["S3_BUCKET"]
    variaveis = {"BUCKET": bucket, **dict(v.split("=", 1) for v in args.var)}
    if "RUN_ID" not in variaveis and any("${RUN_ID}" in open(a, encoding="utf-8").read() for a in args.arquivos):
        variaveis["RUN_ID"] = ultimo_run_id(bucket)
    variaveis.setdefault("EXEC_ID", uuid.uuid4().hex[:12])
    if args.arquivos:
        print(json.dumps({k: v for k, v in variaveis.items() if k != "BUCKET"}))
    athena = boto3.client("athena", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    garantir_workgroup(athena, bucket)

    for caminho in args.arquivos:
        for sql in comandos(caminho, variaveis):
            r = executar(athena, sql)
            print(json.dumps({"arquivo": caminho, "sql": " ".join(sql.split())[:80], **r}, ensure_ascii=False))

    if args.query:
        r = executar(athena, args.query)
        linhas = athena.get_query_results(QueryExecutionId=r["id"])["ResultSet"]["Rows"]
        for linha in linhas:
            print(" | ".join(c.get("VarCharValue", "∅") for c in linha["Data"]))
        print(f"-- {r['bytes']} bytes escaneados")


if __name__ == "__main__":
    main()
