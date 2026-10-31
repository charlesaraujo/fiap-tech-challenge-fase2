-- Regras de qualidade sobre o staging, gravadas em ops.quality_results antes de publicar.
-- bloqueante: reprovada, o portão (03) para a execução e nada é publicado.
-- informativa: só registra; vira alerta/observação.
-- Match rate é medido depois de cada join (aluno→UF, aluno→resultado oficial, aluno→meta, meta UF→resultado UF).
-- Os joins da Gold têm regras próprias no fim de sql/gold/01_gold.sql.

-- Quarentena antes do portão: mesmo numa execução bloqueada fica registrado quais linhas e regras a derrubaram.
DELETE FROM tc_fase2_silver.quarentena WHERE run_id = '${RUN_ID}';

INSERT INTO tc_fase2_silver.quarentena
SELECT run_id, '${EXEC_ID}', 'alunos', motivo_quarentena, 'linha',
  CAST(ano AS varchar) || '|' || coalesce(id_aluno, '∅'),
  registro_fonte,
  CAST(current_timestamp AS timestamp(6))
FROM tc_fase2_silver.stg_alunos WHERE motivo_quarentena IS NOT NULL
UNION ALL
SELECT run_id, '${EXEC_ID}', 'resultado_oficial', motivo_quarentena, 'linha',
  nivel || '|' || coalesce(id_localidade, '∅') || '|' || CAST(ano AS varchar) || '|' || CAST(rede AS varchar),
  json_format(CAST(MAP(ARRAY['taxa_alfabetizacao'], ARRAY[CAST(taxa_alfabetizacao AS varchar)]) AS json)),
  CAST(current_timestamp AS timestamp(6))
FROM tc_fase2_silver.stg_resultado_oficial WHERE motivo_quarentena IS NOT NULL
UNION ALL
SELECT run_id, '${EXEC_ID}', 'meta', motivo_quarentena, 'linha',
  nivel || '|' || coalesce(id_localidade, '∅') || '|' || CAST(ano_edicao AS varchar),
  json_format(CAST(MAP(ARRAY['rede_texto_fonte', 'meta_2030'], ARRAY[rede_texto_fonte, CAST(meta_2030 AS varchar)]) AS json)),
  CAST(current_timestamp AS timestamp(6))
FROM tc_fase2_silver.stg_meta WHERE motivo_quarentena IS NOT NULL;

INSERT INTO tc_fase2_ops.quality_results
WITH
a AS (SELECT * FROM tc_fase2_silver.stg_alunos),
r AS (SELECT * FROM tc_fase2_silver.stg_resultado_oficial),
m AS (SELECT * FROM tc_fase2_silver.stg_meta),
mun_alunos AS (SELECT DISTINCT ano, id_municipio FROM a WHERE rede = 3 AND motivo_quarentena IS NULL),
match_mun AS (
  SELECT
    100.0 * count(ro.id_localidade) / count(*) AS pct_resultado,
    100.0 * count(me.id_localidade) / count(*) AS pct_meta
  FROM mun_alunos x
  LEFT JOIN r ro ON ro.nivel = 'municipio' AND ro.id_localidade = x.id_municipio AND ro.ano = x.ano AND ro.rede = 3
  LEFT JOIN m me ON me.nivel = 'municipio' AND me.id_localidade = x.id_municipio AND me.ano_edicao = x.ano
),
regras (regra, tabela, severidade, medido, criterio, aprovado) AS (
  SELECT 'volume_minimo', 'alunos', 'bloqueante', CAST(count(*) AS double), '>= 1000000', count(*) >= 1000000 FROM a
  UNION ALL
  SELECT 'pct_quarentena', 'alunos', 'bloqueante',
    100.0 * count_if(motivo_quarentena IS NOT NULL) / count(*), '<= 0.1 %',
    100.0 * count_if(motivo_quarentena IS NOT NULL) / count(*) <= 0.1 FROM a
  UNION ALL
  SELECT 'chave_duplicada', 'alunos', 'bloqueante', CAST(count_if(motivo_quarentena = 'chave_duplicada') AS double), '= 0',
    count_if(motivo_quarentena = 'chave_duplicada') = 0 FROM a
  UNION ALL
  -- Categoria "desconhecido": código de rede fora do dicionário tem de ter contagem zero.
  SELECT 'rede_desconhecida', 'alunos', 'bloqueante',
    CAST(count_if(motivo_quarentena = 'rede_fora_do_dicionario') AS double), '= 0',
    count_if(motivo_quarentena = 'rede_fora_do_dicionario') = 0 FROM a
  UNION ALL
  SELECT 'avaliado_sem_peso', 'alunos', 'bloqueante', CAST(count_if(motivo_quarentena = 'avaliado_sem_peso') AS double),
    '= 0', count_if(motivo_quarentena = 'avaliado_sem_peso') = 0 FROM a
  UNION ALL
  SELECT 'match_aluno_uf', 'alunos', 'bloqueante', 100.0 * count(sigla_uf) / count(*), '= 100 %',
    count(sigla_uf) = count(*) FROM a
  UNION ALL
  SELECT 'match_aluno_resultado_municipio', 'alunos', 'bloqueante', pct_resultado, '>= 99 %', pct_resultado >= 99
  FROM match_mun
  UNION ALL
  SELECT 'match_aluno_meta_municipio', 'alunos', 'bloqueante', pct_meta, '>= 95 %', pct_meta >= 95 FROM match_mun
  UNION ALL
  SELECT 'pct_quarentena', 'resultado_oficial', 'bloqueante', CAST(count_if(motivo_quarentena IS NOT NULL) AS double),
    '= 0', count_if(motivo_quarentena IS NOT NULL) = 0 FROM r
  UNION ALL
  SELECT 'pct_quarentena', 'meta', 'bloqueante', CAST(count_if(motivo_quarentena IS NOT NULL) AS double),
    '= 0', count_if(motivo_quarentena IS NOT NULL) = 0 FROM m
  UNION ALL
  -- Contrato de schema: a Bronze lê o Parquet por nome, então coluna renomeada ou removida na fonte vira nulo
  -- sem erro. Coluna inteira nula numa extração é quebra de contrato.
  SELECT 'colunas_vazias', 'alunos', 'bloqueante',
    CAST(cardinality(filter(ARRAY[count(id_escola), count(proficiencia), count(peso_aluno), count(alfabetizado)],
      x -> x = 0)) AS double), '= 0',
    cardinality(filter(ARRAY[count(id_escola), count(proficiencia), count(peso_aluno), count(alfabetizado)],
      x -> x = 0)) = 0 FROM a
  UNION ALL
  SELECT 'colunas_vazias', 'resultado_oficial', 'bloqueante',
    CAST(cardinality(filter(ARRAY[count(taxa_alfabetizacao), count(media_portugues)], x -> x = 0)) AS double), '= 0',
    cardinality(filter(ARRAY[count(taxa_alfabetizacao), count(media_portugues)], x -> x = 0)) = 0 FROM r
  UNION ALL
  SELECT 'colunas_vazias', 'meta', 'bloqueante',
    CAST(cardinality(filter(ARRAY[count(taxa_alfabetizacao), count(meta_2024), count(meta_2030),
      count(percentual_participacao)], x -> x = 0)) AS double), '= 0',
    cardinality(filter(ARRAY[count(taxa_alfabetizacao), count(meta_2024), count(meta_2030),
      count(percentual_participacao)], x -> x = 0)) = 0 FROM m
  UNION ALL
  -- A extração roda fora da orquestração (README, limitações): se ela parar, o pipeline diário reprocessaria
  -- um snapshot velho e ficaria verde. Extração mensal + folga: mais de 35 dias reprova e dispara o alerta.
  SELECT 'idade_extracao', 'bronze', 'bloqueante',
    CAST(date_diff('day', CAST(max(ingestion_date) AS date), current_date) AS double), '<= 35 dias',
    coalesce(date_diff('day', CAST(max(ingestion_date) AS date), current_date) <= 35, false)
  FROM tc_fase2_bronze.dicionario WHERE run_id = '${RUN_ID}'
  UNION ALL
  SELECT 'rede_privada_fora_do_recorte', 'alunos', 'informativa', CAST(count_if(rede = 4) AS double), 'registro', true
  FROM a
  UNION ALL
  SELECT 'pct_nao_avaliados', 'alunos', 'informativa', 100.0 * count_if(NOT avaliado) / count(*), 'registro', true FROM a
  UNION ALL
  SELECT 'meta_municipio_sem_resultado', 'meta', 'informativa',
    CAST(count_if(nivel = 'municipio' AND taxa_alfabetizacao IS NULL) AS double), 'registro', true FROM m
  UNION ALL
  SELECT 'match_meta_uf_resultado_uf', 'meta', 'informativa', 100.0 * count(ro.id_localidade) / count(*), 'registro', true
  FROM m me
  LEFT JOIN r ro ON ro.nivel = 'uf' AND ro.id_localidade = me.id_localidade AND ro.ano = me.ano_edicao AND ro.rede = 5
  WHERE me.nivel = 'uf' AND me.ano_edicao <= 2024
)
SELECT '${EXEC_ID}', '${RUN_ID}', CAST(current_timestamp AS timestamp(6)), regra, tabela, severidade, medido, criterio, aprovado
FROM regras;
