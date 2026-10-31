#!/usr/bin/env bash
# Exporta a Gold para a Fase 3. O UNLOAD grava Parquet no S3 (export/fase3/run_id=.../exec_id=.../), e depois
# baixamos uma cópia para dados_fase3/, um arquivo por tabela (ml_features.parquet, fato_meta.parquet...).
# A cópia local fica no repositório e continua disponível depois que a conta do Learner Lab acabar.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
saida=$(.venv/bin/python apoio/athena.py sql/gold/02_exportar.sql)
echo "$saida"
ids=$(echo "$saida" | head -1 |
  .venv/bin/python -c 'import json,sys; v=json.load(sys.stdin); print(v["RUN_ID"], v["EXEC_ID"])')
read -r run_id exec_id <<< "$ids"
tmp=$(mktemp -d)
aws s3 sync "s3://$S3_BUCKET/export/fase3/run_id=$run_id/exec_id=$exec_id/" "$tmp" --only-show-errors
mkdir -p dados_fase3
for pasta in "$tmp"/*/; do
  tabela=$(basename "$pasta")
  .venv/bin/python -c 'import sys, pyarrow.parquet as pq; pq.write_table(pq.read_table(sys.argv[1]), sys.argv[2])' \
    "$pasta" "dados_fase3/$tabela.parquet"
  echo "dados_fase3/$tabela.parquet"
done
echo "origem: run_id=$run_id exec_id=$exec_id"
