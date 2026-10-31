-- Portão: se alguma regra bloqueante desta execução reprovou, a consulta falha de propósito
-- (cast de texto para inteiro) com o nome das regras na mensagem. O Athena devolve FAILED, o runner
-- e a Step Function param, e o passo de publicação (04) não roda.

SELECT CASE
  WHEN count(*) = 0 THEN CAST('BLOQUEADO: nenhuma regra registrada para ${EXEC_ID}' AS integer)
  WHEN count_if(NOT aprovado) > 0
    THEN CAST('BLOQUEADO pelas regras: ' || array_join(array_agg(CASE WHEN NOT aprovado THEN tabela || '.' || regra END), ', ')
      AS integer)
  ELSE count(*)
END AS regras_aprovadas
FROM tc_fase2_ops.quality_results
WHERE exec_id = '${EXEC_ID}' AND severidade = 'bloqueante';
