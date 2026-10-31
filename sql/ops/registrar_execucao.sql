-- Fecha a execução em ops.run_metrics. Chamado pela Step Function no fim (sucesso) e no Catch (falha).
INSERT INTO tc_fase2_ops.run_metrics
SELECT '${EXEC_ID}', '${RUN_ID}', '${STATUS}', NULLIF('${ERRO}', ''),
  CAST(from_iso8601_timestamp('${INICIO}') AS timestamp(6)),
  CAST(current_timestamp AS timestamp(6)),
  date_diff('millisecond', from_iso8601_timestamp('${INICIO}'), current_timestamp) / 1000.0;
