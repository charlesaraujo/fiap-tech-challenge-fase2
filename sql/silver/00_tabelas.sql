-- Silver e ops: tabelas Iceberg (ACID, MERGE idempotente, time travel). Criadas uma vez;
-- as execuções só fazem MERGE/INSERT. ops.* é append-only: acumula o histórico das execuções.

CREATE DATABASE IF NOT EXISTS tc_fase2_silver;

CREATE DATABASE IF NOT EXISTS tc_fase2_ops;

-- Um aluno avaliado (ou não) em uma edição. Chave natural: (ano, id_aluno); id_aluno reinicia a cada edição.
CREATE TABLE IF NOT EXISTS tc_fase2_silver.alunos (
  ano int,
  id_aluno string,
  id_municipio string,
  sigla_uf string,
  regiao string,
  id_escola string,
  rede int,
  serie int,
  caderno string,
  presente boolean,
  prova_preenchida boolean,
  avaliado boolean,
  alfabetizado boolean,
  proficiencia double,
  peso_aluno double,
  origem string,
  run_id string,
  atualizado_em timestamp
)
PARTITIONED BY (ano)
LOCATION 's3://${BUCKET}/silver/alunos/'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet', 'write_compression' = 'snappy');

-- Resultado oficial agregado (tabelas municipio e uf da fonte, unificadas por nível).
CREATE TABLE IF NOT EXISTS tc_fase2_silver.resultado_oficial (
  nivel string,
  id_localidade string,
  ano int,
  rede int,
  serie int,
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
  proporcao_aluno_nivel_8 double,
  run_id string,
  atualizado_em timestamp
)
LOCATION 's3://${BUCKET}/silver/resultado_oficial/'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet', 'write_compression' = 'snappy');

-- Metas (Brasil, UF, município) com rede em código e série inferida (2º ano). ano_edicao = linha da fonte:
-- cada edição republica a trajetória de metas, que pode ser revisada.
CREATE TABLE IF NOT EXISTS tc_fase2_silver.meta (
  nivel string,
  id_localidade string,
  ano_edicao int,
  rede int,
  rede_texto_fonte string,
  serie int,
  taxa_alfabetizacao double,
  meta_2024 double,
  meta_2025 double,
  meta_2026 double,
  meta_2027 double,
  meta_2028 double,
  meta_2029 double,
  meta_2030 double,
  nivel_alfabetizacao int,
  percentual_participacao double,
  run_id string,
  atualizado_em timestamp
)
LOCATION 's3://${BUCKET}/silver/meta/'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet', 'write_compression' = 'snappy');

CREATE TABLE IF NOT EXISTS tc_fase2_silver.dicionario (
  id_tabela string,
  nome_coluna string,
  chave string,
  valor string,
  run_id string
)
LOCATION 's3://${BUCKET}/silver/dicionario/'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet', 'write_compression' = 'snappy');

-- Linhas que violaram uma regra de linha: ficam fora da Silver, com a regra e o registro original.
CREATE TABLE IF NOT EXISTS tc_fase2_silver.quarentena (
  run_id string,
  exec_id string,
  tabela string,
  regra string,
  severidade string,
  chave string,
  registro string,
  registrado_em timestamp
)
LOCATION 's3://${BUCKET}/silver/quarentena/'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet', 'write_compression' = 'snappy');

-- Resultado de cada regra de qualidade em cada execução (append-only).
CREATE TABLE IF NOT EXISTS tc_fase2_ops.quality_results (
  exec_id string,
  run_id string,
  executado_em timestamp,
  regra string,
  tabela string,
  severidade string,
  medido double,
  criterio string,
  aprovado boolean
)
LOCATION 's3://${BUCKET}/ops/quality_results/'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet', 'write_compression' = 'snappy');

-- Uma linha por execução da Step Function (sucesso ou falha): base dos alarmes e do FinOps.
CREATE TABLE IF NOT EXISTS tc_fase2_ops.run_metrics (
  exec_id string,
  run_id string,
  status string,
  erro string,
  iniciado_em timestamp,
  terminado_em timestamp,
  duracao_s double
)
LOCATION 's3://${BUCKET}/ops/run_metrics/'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet', 'write_compression' = 'snappy');

-- Uma linha por micro-lote do streaming (Kinesis -> Lambda -> MERGE na Silver).
CREATE TABLE IF NOT EXISTS tc_fase2_ops.streaming_metrics (
  lote_id string,
  recebidos int,
  duplicados_no_lote int,
  invalidos int,
  gravados int,
  latencia_p50_s double,
  latencia_p95_s double,
  latencia_max_s double,
  processado_em timestamp
)
LOCATION 's3://${BUCKET}/ops/streaming_metrics/'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet', 'write_compression' = 'snappy');
