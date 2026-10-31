"""Lambda da orquestração: executa arquivos .sql no Athena usando o mesmo runner do uso local (athena.py).

Evento: {"arquivos": ["sql/silver/02_qualidade.sql", ...], "vars": {"RUN_ID": ..., "EXEC_ID": ...},
         "etapa": "Qualidade", "entrada": "SELECT count(*) ..."}
Sem RUN_ID, usamos o último manifesto da Bronze; sem EXEC_ID, geramos um. A função devolve as variáveis,
para a próxima etapa usar as mesmas, e os bytes escaneados. Cada etapa grava uma linha em
ops.etapa_metrics (status, duração, consultas, bytes, linhas de entrada contadas pela consulta "entrada"
e linhas escritas segundo o Athena), inclusive quando falha. Um erro de SQL vira exceção com o nome do
arquivo; a Step Function cai no Catch, registra a falha e manda o alerta.
"""

import json
import os
import time
import uuid

import athena


def handler(event, _context):
    bucket = os.environ["S3_BUCKET"]
    variaveis = {"BUCKET": bucket, **event.get("vars", {})}
    if "ERRO" in variaveis:  # o Cause do Catch chega como JSON com errorMessage
        try:
            variaveis["ERRO"] = json.loads(variaveis["ERRO"])["errorMessage"]
        except (ValueError, KeyError, TypeError):
            pass
        variaveis["ERRO"] = variaveis["ERRO"][:1000]
    variaveis = {k: str(v).replace("'", "''") for k, v in variaveis.items()}  # os valores vão dentro de literais SQL
    if "RUN_ID" not in variaveis:  # com setdefault, o S3 seria listado mesmo com RUN_ID informado
        variaveis["RUN_ID"] = athena.ultimo_run_id(bucket)
    variaveis.setdefault("EXEC_ID", uuid.uuid4().hex[:12])

    cliente = athena.boto3.client("athena")
    athena.garantir_workgroup(cliente, bucket)
    inicio, consultas, total, escritas, entrada = time.monotonic(), 0, 0, 0, None
    try:
        for caminho in event.get("arquivos", []):
            for sql in athena.comandos(caminho, variaveis):
                try:
                    r = athena.executar(cliente, sql)
                except SystemExit as e:
                    raise RuntimeError(f"{caminho}: {e}"[:2000]) from None
                consultas, total, escritas = consultas + 1, total + r["bytes"], escritas + r["linhas"]
        if event.get("entrada"):
            sql = event["entrada"].replace("${RUN_ID}", variaveis["RUN_ID"])
            r = athena.executar(cliente, sql)
            entrada = int(cliente.get_query_results(QueryExecutionId=r["id"])["ResultSet"]["Rows"][1]["Data"][0]
                          ["VarCharValue"])
    except RuntimeError as e:
        registrar(cliente, variaveis, event, "falhou", consultas, total, entrada, escritas, inicio, str(e))
        raise
    registrar(cliente, variaveis, event, "sucesso", consultas, total, entrada, escritas, inicio, "")
    return {"vars": {k: variaveis[k] for k in ("RUN_ID", "EXEC_ID")}, "bytes": total}


def registrar(cliente, v, event, status, consultas, total, entrada, escritas, inicio, erro) -> None:
    if not event.get("etapa"):
        return
    erro = erro[:500].replace("'", "''")
    athena.executar(cliente, f"""INSERT INTO tc_fase2_ops.etapa_metrics VALUES (
  '{v["EXEC_ID"]}', '{v["RUN_ID"]}', '{event["etapa"]}', '{status}', {consultas}, {total},
  {"NULL" if entrada is None else entrada}, {escritas}, {time.monotonic() - inicio:.1f}, NULLIF('{erro}', ''),
  CAST(current_timestamp AS timestamp(6)))""")
