-- Exporta a camada analítica para a Fase 3 (treino do modelo preditivo), em Parquet,
-- versionada pelo run_id da Bronze e pelo exec_id desta execução. Fica em export/, prefixo sem
-- regra de expiração; 06_analise/exportar_fase3.sh baixa uma cópia local.

UNLOAD (SELECT * FROM tc_fase2_gold.ml_features)
TO 's3://${BUCKET}/export/fase3/run_id=${RUN_ID}/exec_id=${EXEC_ID}/ml_features/'
WITH (format = 'PARQUET', compression = 'SNAPPY');

UNLOAD (SELECT * FROM tc_fase2_gold.ml_features_meta)
TO 's3://${BUCKET}/export/fase3/run_id=${RUN_ID}/exec_id=${EXEC_ID}/ml_features_meta/'
WITH (format = 'PARQUET', compression = 'SNAPPY');

UNLOAD (SELECT * FROM tc_fase2_gold.fato_resultado)
TO 's3://${BUCKET}/export/fase3/run_id=${RUN_ID}/exec_id=${EXEC_ID}/fato_resultado/'
WITH (format = 'PARQUET', compression = 'SNAPPY');

UNLOAD (SELECT * FROM tc_fase2_gold.fato_meta)
TO 's3://${BUCKET}/export/fase3/run_id=${RUN_ID}/exec_id=${EXEC_ID}/fato_meta/'
WITH (format = 'PARQUET', compression = 'SNAPPY');

UNLOAD (SELECT * FROM tc_fase2_gold.evolucao)
TO 's3://${BUCKET}/export/fase3/run_id=${RUN_ID}/exec_id=${EXEC_ID}/evolucao/'
WITH (format = 'PARQUET', compression = 'SNAPPY');

UNLOAD (SELECT * FROM tc_fase2_gold.dim_localidade)
TO 's3://${BUCKET}/export/fase3/run_id=${RUN_ID}/exec_id=${EXEC_ID}/dim_localidade/'
WITH (format = 'PARQUET', compression = 'SNAPPY');

UNLOAD (SELECT * FROM tc_fase2_gold.dim_rede)
TO 's3://${BUCKET}/export/fase3/run_id=${RUN_ID}/exec_id=${EXEC_ID}/dim_rede/'
WITH (format = 'PARQUET', compression = 'SNAPPY');
