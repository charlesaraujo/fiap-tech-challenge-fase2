"""Copia as 7 tabelas da Base dos Dados (BigQuery) para a camada Bronze, sem nenhuma transformação.

Cada execução escreve num caminho novo (ingestion_date=.../run_id=...), então uma extração nunca apaga
a anterior. Antes de ler cada tabela fazemos um dry run, que informa quantos bytes a consulta vai
escanear; esse número entra na tabela de custos. No fim gravamos um manifesto em _manifestos/ com
linhas, bytes, duração, job id e tamanho do Parquet de cada tabela.

Exemplos:
    python 02_ingestao/extrair_fonte.py --dry-run          # só mede os bytes
    python 02_ingestao/extrair_fonte.py                    # grava em DESTINO_BRONZE
    python 02_ingestao/extrair_fonte.py --destino s3://bucket/bronze
"""

import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone

import pyarrow.parquet as pq
from google.cloud import bigquery
from pyarrow import fs

DATASET = "basedosdados.br_inep_avaliacao_alfabetizacao"
TABELAS = [
    "alunos",
    "municipio",
    "uf",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "dicionario",
]


def abrir_destino(destino: str) -> tuple[fs.FileSystem, str]:
    """Devolve o filesystem (local ou S3) e o caminho base onde gravar."""
    if "://" in destino:
        return fs.FileSystem.from_uri(destino)
    return fs.LocalFileSystem(), os.path.abspath(destino)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--destino", default=os.environ.get("DESTINO_BRONZE", "data/bronze"))
    parser.add_argument("--projeto", default=os.environ.get("GCP_PROJECT"))
    parser.add_argument("--tabelas", nargs="*", default=TABELAS)
    parser.add_argument("--dry-run", action="store_true", help="só mede os bytes, não extrai")
    args = parser.parse_args()
    if not args.projeto:
        raise SystemExit("Defina GCP_PROJECT no ambiente ou passe --projeto.")

    cliente = bigquery.Client(project=args.projeto)
    filesystem, base = abrir_destino(args.destino)
    run_id = uuid.uuid4().hex[:12]
    agora = datetime.now(timezone.utc)
    manifesto = {"run_id": run_id, "ingested_at": agora.isoformat(), "destino": args.destino, "tabelas": []}

    for tabela in args.tabelas:
        sql = f"SELECT * FROM `{DATASET}.{tabela}`"
        dry = cliente.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
        item = {"tabela": tabela, "bytes_escaneados_dry_run": dry.total_bytes_processed}

        if not args.dry_run:
            inicio = time.monotonic()
            job = cliente.query(sql)
            dados = job.to_arrow(create_bqstorage_client=True)
            pasta = f"{base}/{tabela}/ingestion_date={agora:%Y-%m-%d}/run_id={run_id}"
            filesystem.create_dir(pasta, recursive=True)
            arquivo = f"{pasta}/{tabela}.parquet"
            pq.write_table(dados, arquivo, filesystem=filesystem, compression="snappy")
            item.update(
                job_id=job.job_id,
                bytes_faturados=job.total_bytes_billed,
                linhas=dados.num_rows,
                bytes_parquet=filesystem.get_file_info(arquivo).size,
                duracao_s=round(time.monotonic() - inicio, 2),
                caminho=arquivo,
            )

        manifesto["tabelas"].append(item)
        print(json.dumps(item, ensure_ascii=False))

    if not args.dry_run:
        filesystem.create_dir(f"{base}/_manifestos", recursive=True)
        with filesystem.open_output_stream(f"{base}/_manifestos/run_id={run_id}.json") as saida:
            saida.write(json.dumps(manifesto, ensure_ascii=False, indent=2).encode())


if __name__ == "__main__":
    main()
