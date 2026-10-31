"""Consumidor do streaming: recebe um micro-lote do Kinesis, valida e faz MERGE na Silver (origem = 'simulado').

- Evento inválido (chave nula, rede que não está no `dicionario` da Silver ou proficiência fora de 400–1100)
  vai para a quarentena.
- Duplicado dentro do lote: fica o último de cada (ano, id_aluno). Duplicado entre lotes: o MERGE pela
  chave atualiza a linha em vez de inserir outra, e só mexe em linhas simuladas (t.origem = 'simulado'),
  nunca nas oficiais.
- A latência de cada evento vai do enviado_em do produtor até o MERGE confirmado; p50 e p95 de cada lote
  ficam em ops.streaming_metrics.
"""

import base64
import json
import statistics
import time
import uuid

import athena

COLUNAS = ["ano", "id_aluno", "id_municipio", "sigla_uf", "regiao", "id_escola", "rede", "serie", "caderno",
           "presente", "prova_preenchida", "avaliado", "alfabetizado", "proficiencia", "peso_aluno"]


def literal(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"


_redes: set[int] | None = None


def redes_do_dicionario(cliente) -> set[int]:
    """Códigos de rede válidos, lidos uma vez por container, da mesma tabela que o batch usa."""
    global _redes
    if _redes is None:
        r = athena.executar(cliente, "SELECT chave FROM tc_fase2_silver.dicionario "
                                     "WHERE id_tabela = 'alunos' AND nome_coluna = 'rede'")
        linhas = cliente.get_query_results(QueryExecutionId=r["id"])["ResultSet"]["Rows"][1:]
        _redes = {int(linha["Data"][0]["VarCharValue"]) for linha in linhas}
    return _redes


def motivo(e: dict, redes: set[int]) -> str | None:
    if not e.get("id_aluno") or not e.get("id_municipio") or e.get("ano") is None:
        return "chave_nula"
    if e.get("rede") not in redes:
        return "rede_fora_do_dicionario"
    if e.get("avaliado") and not 400 <= (e.get("proficiencia") or 0) <= 1100:
        return "proficiencia_fora_da_faixa"
    return None


def handler(event, _context):
    lote = uuid.uuid4().hex[:12]
    eventos = [json.loads(base64.b64decode(r["kinesis"]["data"])) for r in event["Records"]]
    cliente = athena.boto3.client("athena")
    redes = redes_do_dicionario(cliente)
    invalidos = [(e, m) for e in eventos if (m := motivo(e, redes))]
    validos = {}
    for e in eventos:
        if not motivo(e, redes):
            validos[(e["ano"], e["id_aluno"])] = e  # se repetir, fica o último

    if invalidos:
        linhas = ",\n".join(
            f"('stream', {literal(lote)}, 'alunos_stream', {literal(m)}, 'bloqueante', "
            f"{literal(str(e.get('ano')) + '|' + str(e.get('id_aluno')))}, {literal(json.dumps(e))}, "
            f"CAST(current_timestamp AS timestamp(6)))" for e, m in invalidos)
        athena.executar(cliente, f"INSERT INTO tc_fase2_silver.quarentena VALUES {linhas}")

    if validos:
        valores = ",\n".join("(" + ", ".join(literal(e.get(c)) for c in COLUNAS) + ")" for e in validos.values())
        atualiza = ", ".join(f"{c} = s.{c}" for c in COLUNAS[2:])
        athena.executar(cliente, f"""
MERGE INTO tc_fase2_silver.alunos t
USING (SELECT * FROM (VALUES {valores}) AS v ({", ".join(COLUNAS)})) s
ON t.ano = s.ano AND t.id_aluno = s.id_aluno
WHEN MATCHED AND t.origem = 'simulado' THEN UPDATE SET {atualiza},
  run_id = 'stream-{lote}', atualizado_em = CAST(current_timestamp AS timestamp(6))
WHEN NOT MATCHED THEN INSERT ({", ".join(COLUNAS)}, origem, run_id, atualizado_em)
VALUES ({", ".join("s." + c for c in COLUNAS)}, 'simulado', 'stream-{lote}',
  CAST(current_timestamp AS timestamp(6)))""")

    agora = time.time()
    lat = sorted(agora - e["enviado_em"] for e in eventos)
    p50 = statistics.median(lat)
    p95 = lat[min(len(lat) - 1, int(0.95 * len(lat)))]
    duplicados = len(eventos) - len(invalidos) - len(validos)
    athena.executar(cliente, f"""INSERT INTO tc_fase2_ops.streaming_metrics VALUES (
  '{lote}', {len(eventos)}, {duplicados}, {len(invalidos)}, {len(validos)},
  {p50:.3f}, {p95:.3f}, {lat[-1]:.3f}, CAST(current_timestamp AS timestamp(6)))""")
    return {"lote": lote, "recebidos": len(eventos), "gravados": len(validos), "p50": p50, "p95": p95}
