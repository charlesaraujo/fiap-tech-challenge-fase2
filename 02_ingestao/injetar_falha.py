"""Cria uma extração adulterada na Bronze, para mostrar a quarentena e o portão de qualidade funcionando.

Copia uma extração real para um run_id novo (falha-<modo>-<id>) e estraga parte dos dados:
- leve: 15 linhas inválidas em `alunos` (proficiência fora da faixa e código inválido). Elas vão para a
  quarentena, ficam abaixo do limite de 0,1% e a execução segue publicando só as linhas boas.
- grave: as mesmas 15 linhas, mais 5.000 alunos duplicados e 3 com rede fora do dicionário. As regras
  bloqueantes reprovam e o portão interrompe a execução antes de publicar.
- gold: `alunos` fica intacto, mas a meta municipal de 2024 perde Pernambuco (184 municípios). A regra
  da Silver olha 2023 e 2024 juntos e ainda passa (~95,8%); o join de t+1 da Gold cai para ~92,6% e o
  portão da Gold barra o export.
O script não grava manifesto, então o run_id adulterado nunca é escolhido como "último" por engano.

Exemplo:
    python 02_ingestao/injetar_falha.py --base a3e3036c5911 --modo grave
    python apoio/athena.py sql/bronze/01_tabelas.sql sql/silver/0*.sql --var RUN_ID=<run_id impresso>
"""

import argparse
import os
import uuid

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyarrow import fs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", required=True, help="run_id real a copiar")
    parser.add_argument("--modo", choices=["leve", "grave", "gold"], required=True)
    args = parser.parse_args()
    bucket = os.environ["S3_BUCKET"]
    run_id = f"falha-{args.modo}-{uuid.uuid4().hex[:6]}"
    s3 = boto3.client("s3")

    for obj in s3.list_objects_v2(Bucket=bucket, Prefix="bronze/")["Contents"]:
        chave = obj["Key"]
        alterada = "bronze/meta_alfabetizacao_municipio/" if args.modo == "gold" else "bronze/alunos/"
        if f"/run_id={args.base}/" in chave and not chave.startswith(alterada):
            destino = chave.replace(f"run_id={args.base}", f"run_id={run_id}")
            s3.copy_object(Bucket=bucket, Key=destino, CopySource={"Bucket": bucket, "Key": chave})

    s3fs = fs.S3FileSystem(region=os.environ.get("AWS_REGION", "us-east-1"))
    if args.modo == "gold":
        origem = next(
            o["Key"] for o in s3.list_objects_v2(Bucket=bucket, Prefix=alterada)["Contents"]
            if f"/run_id={args.base}/" in o["Key"] and o["Key"].endswith(".parquet")
        )
        meta = pq.read_table(f"{bucket}/{origem}", filesystem=s3fs)
        sem_pe = pc.and_(pc.equal(meta["ano"], 2024), pc.starts_with(meta["id_municipio"], "26"))
        parcial = meta.filter(pc.invert(sem_pe))
        destino = origem.replace(f"run_id={args.base}", f"run_id={run_id}")
        pq.write_table(parcial, f"{bucket}/{destino}", filesystem=s3fs, compression="snappy")
        print(f"run_id={run_id} meta municipal sem PE em 2024: {meta.num_rows} -> {parcial.num_rows} linhas")
        return

    origem = next(
        o["Key"] for o in s3.list_objects_v2(Bucket=bucket, Prefix="bronze/alunos/")["Contents"]
        if f"/run_id={args.base}/" in o["Key"] and o["Key"].endswith(".parquet")
    )
    alunos = pq.read_table(f"{bucket}/{origem}", filesystem=s3fs)
    avaliados = alunos.filter(pc.equal(alunos["preenchimento_caderno"], "1"))

    def adulterar(linhas: pa.Table, prefixo: str, **colunas) -> pa.Table:
        ids = pa.array([f"{prefixo}{i:06d}" for i in range(linhas.num_rows)])
        linhas = linhas.set_column(linhas.schema.get_field_index("id_aluno"), "id_aluno", ids)
        for nome, valor in colunas.items():
            arr = pa.array([valor] * linhas.num_rows, type=linhas.schema.field(nome).type)
            linhas = linhas.set_column(linhas.schema.get_field_index(nome), nome, arr)
        return linhas

    partes = [
        alunos,
        adulterar(avaliados.slice(0, 10), "PROF", proficiencia=9999.0),
        adulterar(avaliados.slice(10, 5), "COD", alfabetizado="7"),
    ]
    if args.modo == "grave":
        partes += [alunos.slice(0, 5000), adulterar(avaliados.slice(20, 3), "REDE", rede="9")]
    adulterada = pa.concat_tables(partes)

    destino = origem.replace(f"run_id={args.base}", f"run_id={run_id}")
    pq.write_table(adulterada, f"{bucket}/{destino}", filesystem=s3fs, compression="snappy")
    print(f"run_id={run_id} linhas={adulterada.num_rows} (+{adulterada.num_rows - alunos.num_rows} adulteradas)")


if __name__ == "__main__":
    main()
