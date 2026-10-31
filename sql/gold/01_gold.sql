-- Gold: modelo estrela + tabelas analíticas, reconstruídas a cada execução a partir da Silver.
-- Iceberg; DROP + CTAS porque o volume é pequeno (dezenas de milhares de linhas) e o rebuild
-- garante que a Gold reflete exatamente a Silver publicada.
-- Recorte: escolas públicas (rede 2 estadual, 3 municipal; 5 = pública total). Rede privada (25 alunos) fica fora.

CREATE DATABASE IF NOT EXISTS tc_fase2_gold;

-- ---------- Dimensões ----------

DROP TABLE IF EXISTS tc_fase2_gold.dim_rede;

CREATE TABLE tc_fase2_gold.dim_rede
WITH (table_type = 'ICEBERG', location = 's3://${BUCKET}/gold/dim_rede/', is_external = false) AS
SELECT CAST(chave AS integer) AS rede, valor AS rede_nome,
  CASE CAST(chave AS integer) WHEN 3 THEN 'Municipal' WHEN 5 THEN 'Pública' END AS rede_texto_metas
FROM tc_fase2_silver.dicionario
WHERE id_tabela = 'municipio' AND nome_coluna = 'rede';

DROP TABLE IF EXISTS tc_fase2_gold.dim_localidade;

CREATE TABLE tc_fase2_gold.dim_localidade
WITH (table_type = 'ICEBERG', location = 's3://${BUCKET}/gold/dim_localidade/', is_external = false) AS
SELECT DISTINCT 'municipio' AS nivel, a.id_municipio AS id_localidade, a.sigla_uf, a.regiao
FROM tc_fase2_silver.alunos a
WHERE a.origem = 'oficial'
UNION ALL
SELECT 'uf', sigla_uf, sigla_uf, regiao FROM tc_fase2_silver.ref_uf
UNION ALL
SELECT 'brasil', 'BR', CAST(NULL AS varchar), CAST(NULL AS varchar);

-- ---------- Fato resultado: taxa calculada dos microdados x taxa oficial (validação cruzada) ----------
-- Grão: nível × localidade × ano × rede × série. Taxa ponderada = Σ peso·alfabetizado / Σ peso, só avaliados.
-- IC 95% de Wilson com n efetivo de Kish, (Σw)² / Σw², porque os pesos reduzem a informação de cada aluno.

DROP TABLE IF EXISTS tc_fase2_gold.fato_resultado;

CREATE TABLE tc_fase2_gold.fato_resultado
WITH (table_type = 'ICEBERG', location = 's3://${BUCKET}/gold/fato_resultado/', is_external = false) AS
WITH base AS (
  SELECT a.*, r.rede_agregada
  FROM tc_fase2_silver.alunos a
  CROSS JOIN UNNEST (ARRAY[a.rede, 5]) AS r (rede_agregada)
  WHERE a.rede IN (2, 3) AND a.origem = 'oficial'  -- eventos do streaming (simulado) não entram nos indicadores
),
niveis AS (
  SELECT b.*, n.nivel, n.id_localidade
  FROM base b
  CROSS JOIN UNNEST (ARRAY['municipio', 'uf', 'brasil'], ARRAY[b.id_municipio, b.sigla_uf, 'BR']) AS n (nivel, id_localidade)
),
agg AS (
  SELECT nivel, id_localidade, ano, rede_agregada AS rede, serie,
    count(*) AS alunos_previstos,
    count_if(avaliado) AS alunos_avaliados,
    sum(CASE WHEN avaliado THEN peso_aluno END) AS soma_pesos,
    sum(CASE WHEN avaliado AND alfabetizado THEN peso_aluno ELSE 0 END) AS soma_pesos_alfabetizados,
    power(sum(CASE WHEN avaliado THEN peso_aluno END), 2) / sum(CASE WHEN avaliado THEN peso_aluno * peso_aluno END)
      AS n_efetivo,
    sum(CASE WHEN avaliado THEN peso_aluno * proficiencia END) / sum(CASE WHEN avaliado THEN peso_aluno END)
      AS proficiencia_media,
    100.0 * avg(CASE WHEN avaliado AND NOT alfabetizado THEN 0 WHEN avaliado THEN 1 END) AS taxa_simples
  FROM niveis
  GROUP BY nivel, id_localidade, ano, rede_agregada, serie
),
calc AS (
  SELECT agg.*, soma_pesos_alfabetizados / soma_pesos AS p, 1.96 AS z FROM agg
),
oficial AS (
  SELECT nivel, id_localidade, ano, rede, taxa_alfabetizacao FROM tc_fase2_silver.resultado_oficial
  UNION ALL
  -- Brasil só existe na tabela de metas; a taxa da linha é o resultado oficial daquela edição.
  SELECT nivel, id_localidade, ano_edicao, rede, taxa_alfabetizacao FROM tc_fase2_silver.meta WHERE nivel = 'brasil'
)
SELECT
  c.nivel, c.id_localidade, c.ano, c.rede, c.serie,
  c.alunos_previstos, c.alunos_avaliados,
  round(100.0 * c.alunos_avaliados / c.alunos_previstos, 2) AS pct_participacao,
  round(c.n_efetivo, 1) AS n_efetivo,
  round(c.proficiencia_media, 2) AS proficiencia_media,
  round(100 * c.p, 2) AS taxa_calculada,
  round(c.taxa_simples, 2) AS taxa_media_simples,
  round(100 * (c.p + c.z * c.z / (2 * c.n_efetivo)
    - c.z * sqrt(c.p * (1 - c.p) / c.n_efetivo + c.z * c.z / (4 * c.n_efetivo * c.n_efetivo)))
    / (1 + c.z * c.z / c.n_efetivo), 2) AS ic95_inferior,
  round(100 * (c.p + c.z * c.z / (2 * c.n_efetivo)
    + c.z * sqrt(c.p * (1 - c.p) / c.n_efetivo + c.z * c.z / (4 * c.n_efetivo * c.n_efetivo)))
    / (1 + c.z * c.z / c.n_efetivo), 2) AS ic95_superior,
  o.taxa_alfabetizacao AS taxa_oficial,
  round(100 * c.p - o.taxa_alfabetizacao, 2) AS diferenca_pp,
  CASE
    WHEN c.p IS NULL THEN 'sem_calculo'  -- nenhum aluno avaliado: nulo não é divergência
    WHEN o.taxa_alfabetizacao IS NULL THEN 'sem_oficial'
    WHEN abs(100 * c.p - o.taxa_alfabetizacao) < 0.05 THEN 'bate'
    ELSE 'diverge'
  END AS convergencia
FROM calc c
LEFT JOIN oficial o
  ON o.nivel = c.nivel AND o.id_localidade = c.id_localidade AND o.ano = c.ano AND o.rede = c.rede;

-- ---------- Fato meta x resultado ----------
-- Grão: nível × localidade × edição. Resultado do ano t comparado com a meta do ano t publicada na mesma edição.
-- Estados: atingiu / nao_atingiu / sem_meta (não existe meta para o ano, ex.: 2023) / sem_resultado.
-- Meta pactuada 2030 = 80% (valor da fonte); meta aspiracional = 100% (alfabetização universal).

DROP TABLE IF EXISTS tc_fase2_gold.fato_meta;

CREATE TABLE tc_fase2_gold.fato_meta
WITH (table_type = 'ICEBERG', location = 's3://${BUCKET}/gold/fato_meta/', is_external = false) AS
WITH m AS (
  SELECT *,
    CASE ano_edicao WHEN 2024 THEN meta_2024 WHEN 2025 THEN meta_2025 WHEN 2026 THEN meta_2026
      WHEN 2027 THEN meta_2027 WHEN 2028 THEN meta_2028 WHEN 2029 THEN meta_2029 WHEN 2030 THEN meta_2030
    END AS meta_do_ano
  FROM tc_fase2_silver.meta
)
SELECT
  nivel, id_localidade, ano_edicao AS ano, rede, serie,
  taxa_alfabetizacao AS taxa_oficial,
  meta_do_ano,
  CASE
    WHEN meta_do_ano IS NULL THEN 'sem_meta'
    WHEN taxa_alfabetizacao IS NULL THEN 'sem_resultado'
    WHEN taxa_alfabetizacao >= meta_do_ano THEN 'atingiu'
    ELSE 'nao_atingiu'
  END AS status_meta,
  round(taxa_alfabetizacao - meta_do_ano, 2) AS distancia_meta_pp,
  meta_2030 AS meta_pactuada_2030,
  100.0 AS meta_aspiracional,
  round(meta_2030 - taxa_alfabetizacao, 2) AS falta_para_pactuada_2030_pp,
  round((meta_2030 - taxa_alfabetizacao) / (2030 - ano_edicao), 2) AS ritmo_necessario_pp_ano,
  nivel_alfabetizacao, percentual_participacao
FROM m;

-- ---------- Evolução ----------
-- Grão: nível × localidade × ano, rede pública agregada da meta (município = rede 3; UF/Brasil = rede 5).
-- O cenário 2030 é uma extrapolação linear do ritmo médio desde o primeiro ano, não uma previsão.
-- Limitado a [0, 100]: taxa é proporção, e uma queda forte extrapolada daria valor negativo.

DROP TABLE IF EXISTS tc_fase2_gold.evolucao;

CREATE TABLE tc_fase2_gold.evolucao
WITH (table_type = 'ICEBERG', location = 's3://${BUCKET}/gold/evolucao/', is_external = false) AS
WITH s AS (
  SELECT nivel, id_localidade, ano_edicao AS ano, rede, taxa_alfabetizacao AS taxa, meta_2030,
    lag(taxa_alfabetizacao) OVER (PARTITION BY nivel, id_localidade ORDER BY ano_edicao) AS taxa_ano_anterior,
    first_value(taxa_alfabetizacao) OVER (PARTITION BY nivel, id_localidade ORDER BY ano_edicao) AS taxa_primeiro_ano,
    first_value(ano_edicao) OVER (PARTITION BY nivel, id_localidade ORDER BY ano_edicao) AS primeiro_ano
  FROM tc_fase2_silver.meta
)
SELECT
  nivel, id_localidade, ano, rede, taxa, taxa_ano_anterior,
  round(taxa - taxa_ano_anterior, 2) AS variacao_pp,
  CASE
    WHEN taxa IS NULL OR taxa_ano_anterior IS NULL THEN 'sem_comparacao'
    WHEN taxa > taxa_ano_anterior THEN 'melhorou'
    WHEN taxa < taxa_ano_anterior THEN 'piorou'
    ELSE 'estavel'
  END AS tendencia,
  round(greatest(0, least(100, taxa + (taxa - taxa_primeiro_ano) / nullif(ano - primeiro_ano, 0) * (2030 - ano))), 1)
    AS cenario_linear_2030,
  meta_2030 AS meta_pactuada_2030
FROM s;

-- ---------- Features para a Fase 3 (modelo preditivo) ----------
-- Grão: município (rede municipal) × ano base t. Features só do ano t; alvo do ano t+1 (sem vazamento).
-- Hoje há um par (2023 → 2024); o SQL é genérico para quando a fonte trouxer novos anos.
-- Um alvo por tabela: ml_features (regressão, taxa de t+1) e ml_features_meta (classificação, atingiu a meta
-- de t+1). Um alvo deriva do outro, e entregá-los juntos seria vazamento de alvo.

DROP TABLE IF EXISTS tc_fase2_gold.ml_features;

CREATE TABLE tc_fase2_gold.ml_features
WITH (table_type = 'ICEBERG', location = 's3://${BUCKET}/gold/ml_features/', is_external = false) AS
SELECT
  f.id_localidade AS id_municipio, l.sigla_uf, l.regiao, f.ano AS ano_base,
  -- features do ano base
  f.alunos_previstos, f.alunos_avaliados, f.pct_participacao, f.n_efetivo, f.proficiencia_media,
  f.taxa_calculada, f.ic95_superior - f.ic95_inferior AS largura_ic95, f.taxa_oficial,
  ro.media_portugues,
  ro.proporcao_aluno_nivel_0, ro.proporcao_aluno_nivel_1, ro.proporcao_aluno_nivel_2, ro.proporcao_aluno_nivel_3,
  ro.proporcao_aluno_nivel_4, ro.proporcao_aluno_nivel_5, ro.proporcao_aluno_nivel_6, ro.proporcao_aluno_nivel_7,
  ro.proporcao_aluno_nivel_8,
  mb.nivel_alfabetizacao, mb.percentual_participacao AS percentual_participacao_meta,
  mb.meta_2030 AS meta_pactuada_2030,
  ma.meta_do_ano AS meta_ano_seguinte,  -- pactuada antes do resultado: pode ser feature
  -- alvo do ano seguinte
  fa.taxa_oficial AS alvo_taxa_ano_seguinte
FROM tc_fase2_gold.fato_resultado f
JOIN tc_fase2_gold.dim_localidade l ON l.nivel = 'municipio' AND l.id_localidade = f.id_localidade
LEFT JOIN tc_fase2_silver.resultado_oficial ro
  ON ro.nivel = 'municipio' AND ro.id_localidade = f.id_localidade AND ro.ano = f.ano AND ro.rede = 3
LEFT JOIN tc_fase2_silver.meta mb ON mb.nivel = 'municipio' AND mb.id_localidade = f.id_localidade AND mb.ano_edicao = f.ano
JOIN tc_fase2_gold.fato_resultado fa
  ON fa.nivel = 'municipio' AND fa.id_localidade = f.id_localidade AND fa.rede = 3 AND fa.ano = f.ano + 1
LEFT JOIN tc_fase2_gold.fato_meta ma
  ON ma.nivel = 'municipio' AND ma.id_localidade = f.id_localidade AND ma.ano = f.ano + 1
WHERE f.nivel = 'municipio' AND f.rede = 3;

DROP TABLE IF EXISTS tc_fase2_gold.ml_features_meta;

CREATE TABLE tc_fase2_gold.ml_features_meta
WITH (table_type = 'ICEBERG', location = 's3://${BUCKET}/gold/ml_features_meta/', is_external = false) AS
SELECT
  f.id_municipio, f.sigla_uf, f.regiao, f.ano_base,
  f.alunos_previstos, f.alunos_avaliados, f.pct_participacao, f.n_efetivo, f.proficiencia_media,
  f.taxa_calculada, f.largura_ic95, f.taxa_oficial, f.media_portugues,
  f.proporcao_aluno_nivel_0, f.proporcao_aluno_nivel_1, f.proporcao_aluno_nivel_2, f.proporcao_aluno_nivel_3,
  f.proporcao_aluno_nivel_4, f.proporcao_aluno_nivel_5, f.proporcao_aluno_nivel_6, f.proporcao_aluno_nivel_7,
  f.proporcao_aluno_nivel_8,
  f.nivel_alfabetizacao, f.percentual_participacao_meta, f.meta_pactuada_2030, f.meta_ano_seguinte,
  -- alvo do ano seguinte (nulo quando não há meta ou resultado: "sem meta" não é "não atingiu")
  CASE WHEN ma.status_meta IN ('atingiu', 'nao_atingiu') THEN ma.status_meta = 'atingiu' END
    AS alvo_atingiu_meta_ano_seguinte
FROM tc_fase2_gold.ml_features f
LEFT JOIN tc_fase2_gold.fato_meta ma
  ON ma.nivel = 'municipio' AND ma.id_localidade = f.id_municipio AND ma.ano = f.ano_base + 1;

-- ---------- Match rate dos joins da Gold ----------
-- Mesma lógica da Silver: join que não casa vira nulo (LEFT) ou some (INNER) sem erro. As regras entram em
-- ops.quality_results e o portão (sql/silver/03_portao.sql, rodado logo depois nesta etapa) impede o export.
-- Limiares medidos na execução de referência: 99,88%, 99,05% e 96,48%.

INSERT INTO tc_fase2_ops.quality_results
WITH
municipios AS (SELECT * FROM tc_fase2_gold.fato_resultado WHERE nivel = 'municipio'),
-- base esperada de ml_features: município × rede municipal em todo ano t que tem t+1 nos microdados
base_ml AS (
  SELECT count(*) AS n FROM municipios
  WHERE rede = 3 AND ano + 1 IN (SELECT DISTINCT ano FROM municipios)
),
ml AS (SELECT count(*) AS n, count(meta_ano_seguinte) AS com_meta FROM tc_fase2_gold.ml_features),
medidas AS (
  -- fato_resultado LEFT JOIN taxa oficial: o oficial cobre quase todo município com aluno.
  SELECT 'match_resultado_oficial_municipio' AS regra, 'gold.fato_resultado' AS tabela,
    CAST(100.0 * count(taxa_oficial) / count(*) AS double) AS medido, 99.0 AS limiar
  FROM municipios
  UNION ALL
  -- ml_features INNER JOIN resultado de t+1: município sem rede municipal avaliada em t+1 some da base.
  SELECT 'match_ml_alvo_ano_seguinte', 'gold.ml_features', CAST(100.0 * ml.n / base_ml.n AS double), 98.0
  FROM ml CROSS JOIN base_ml
  UNION ALL
  -- ml_features LEFT JOIN meta de t+1: nem todo município tem meta pactuada (mesmo limiar da Silver).
  SELECT 'match_ml_meta_ano_seguinte', 'gold.ml_features', CAST(100.0 * com_meta / n AS double), 95.0 FROM ml
)
SELECT '${EXEC_ID}', '${RUN_ID}', CAST(current_timestamp AS timestamp(6)), regra, tabela, 'bloqueante', medido,
  '>= ' || CAST(CAST(limiar AS integer) AS varchar) || ' %', medido >= limiar
FROM medidas;
