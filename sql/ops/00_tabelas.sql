-- Métricas por etapa da Step Function (uma linha por etapa por execução, acumulando). Criada no passo
-- Preparar, antes de qualquer outra etapa gravar nela.
CREATE DATABASE IF NOT EXISTS tc_fase2_ops;

CREATE TABLE IF NOT EXISTS tc_fase2_ops.etapa_metrics (
  exec_id string,
  run_id string,
  etapa string,
  status string,
  consultas int,
  bytes_escaneados bigint,
  linhas_entrada bigint,
  linhas_escritas bigint,
  duracao_s double,
  erro string,
  registrado_em timestamp
)
LOCATION 's3://${BUCKET}/ops/etapa_metrics/'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet', 'write_compression' = 'snappy');
