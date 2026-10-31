-- Bronze: tabelas externas sobre o Parquet bruto. Uma partição por execução (ingestion_date, run_id);
-- nenhuma execução sobrescreve outra. ${BUCKET} é trocado pelo bucket em apoio/athena.py.

CREATE DATABASE IF NOT EXISTS tc_fase2_bronze;

CREATE EXTERNAL TABLE IF NOT EXISTS tc_fase2_bronze.alunos (
  ano bigint,
  id_municipio string,
  id_escola string,
  id_aluno string,
  caderno string,
  serie string,
  rede string,
  presenca string,
  preenchimento_caderno string,
  alfabetizado string,
  proficiencia double,
  peso_aluno double
)
PARTITIONED BY (ingestion_date string, run_id string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/bronze/alunos/';

MSCK REPAIR TABLE tc_fase2_bronze.alunos;

CREATE EXTERNAL TABLE IF NOT EXISTS tc_fase2_bronze.municipio (
  ano bigint,
  id_municipio string,
  serie string,
  rede string,
  taxa_alfabetizacao double,
  media_portugues double,
  proporcao_aluno_nivel_0 double,
  proporcao_aluno_nivel_1 double,
  proporcao_aluno_nivel_2 double,
  proporcao_aluno_nivel_3 double,
  proporcao_aluno_nivel_4 double,
  proporcao_aluno_nivel_5 double,
  proporcao_aluno_nivel_6 double,
  proporcao_aluno_nivel_7 double,
  proporcao_aluno_nivel_8 double
)
PARTITIONED BY (ingestion_date string, run_id string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/bronze/municipio/';

MSCK REPAIR TABLE tc_fase2_bronze.municipio;

CREATE EXTERNAL TABLE IF NOT EXISTS tc_fase2_bronze.uf (
  ano bigint,
  sigla_uf string,
  serie string,
  rede string,
  taxa_alfabetizacao double,
  media_portugues double,
  proporcao_aluno_nivel_0 double,
  proporcao_aluno_nivel_1 double,
  proporcao_aluno_nivel_2 double,
  proporcao_aluno_nivel_3 double,
  proporcao_aluno_nivel_4 double,
  proporcao_aluno_nivel_5 double,
  proporcao_aluno_nivel_6 double,
  proporcao_aluno_nivel_7 double,
  proporcao_aluno_nivel_8 double
)
PARTITIONED BY (ingestion_date string, run_id string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/bronze/uf/';

MSCK REPAIR TABLE tc_fase2_bronze.uf;

CREATE EXTERNAL TABLE IF NOT EXISTS tc_fase2_bronze.meta_alfabetizacao_brasil (
  ano bigint,
  rede string,
  taxa_alfabetizacao double,
  meta_alfabetizacao_2024 double,
  meta_alfabetizacao_2025 double,
  meta_alfabetizacao_2026 double,
  meta_alfabetizacao_2027 double,
  meta_alfabetizacao_2028 double,
  meta_alfabetizacao_2029 double,
  meta_alfabetizacao_2030 double,
  percentual_participacao double
)
PARTITIONED BY (ingestion_date string, run_id string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/bronze/meta_alfabetizacao_brasil/';

MSCK REPAIR TABLE tc_fase2_bronze.meta_alfabetizacao_brasil;

CREATE EXTERNAL TABLE IF NOT EXISTS tc_fase2_bronze.meta_alfabetizacao_uf (
  ano bigint,
  sigla_uf string,
  rede string,
  taxa_alfabetizacao double,
  meta_alfabetizacao_2024 double,
  meta_alfabetizacao_2025 double,
  meta_alfabetizacao_2026 double,
  meta_alfabetizacao_2027 double,
  meta_alfabetizacao_2028 double,
  meta_alfabetizacao_2029 double,
  meta_alfabetizacao_2030 double,
  percentual_participacao double
)
PARTITIONED BY (ingestion_date string, run_id string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/bronze/meta_alfabetizacao_uf/';

MSCK REPAIR TABLE tc_fase2_bronze.meta_alfabetizacao_uf;

CREATE EXTERNAL TABLE IF NOT EXISTS tc_fase2_bronze.meta_alfabetizacao_municipio (
  ano bigint,
  id_municipio string,
  rede string,
  taxa_alfabetizacao double,
  meta_alfabetizacao_2024 double,
  meta_alfabetizacao_2025 double,
  meta_alfabetizacao_2026 double,
  meta_alfabetizacao_2027 double,
  meta_alfabetizacao_2028 double,
  meta_alfabetizacao_2029 double,
  meta_alfabetizacao_2030 double,
  nivel_alfabetizacao bigint,
  percentual_participacao double
)
PARTITIONED BY (ingestion_date string, run_id string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/bronze/meta_alfabetizacao_municipio/';

MSCK REPAIR TABLE tc_fase2_bronze.meta_alfabetizacao_municipio;

CREATE EXTERNAL TABLE IF NOT EXISTS tc_fase2_bronze.dicionario (
  id_tabela string,
  nome_coluna string,
  chave string,
  cobertura_temporal string,
  valor string
)
PARTITIONED BY (ingestion_date string, run_id string)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/bronze/dicionario/';

MSCK REPAIR TABLE tc_fase2_bronze.dicionario;

