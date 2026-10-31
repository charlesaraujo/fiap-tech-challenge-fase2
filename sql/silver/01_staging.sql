-- Staging: views sobre a Bronze de um run_id, já limpas e tipadas, com o motivo de quarentena de cada linha.
-- Views não gravam nada: as regras de qualidade (02) avaliam o staging antes de qualquer publicação (04).

-- Referência IBGE: os 2 primeiros dígitos de id_municipio são o código da UF.
CREATE OR REPLACE VIEW tc_fase2_silver.ref_uf AS
SELECT * FROM (VALUES
  ('11', 'RO', 'Norte'), ('12', 'AC', 'Norte'), ('13', 'AM', 'Norte'), ('14', 'RR', 'Norte'),
  ('15', 'PA', 'Norte'), ('16', 'AP', 'Norte'), ('17', 'TO', 'Norte'),
  ('21', 'MA', 'Nordeste'), ('22', 'PI', 'Nordeste'), ('23', 'CE', 'Nordeste'), ('24', 'RN', 'Nordeste'),
  ('25', 'PB', 'Nordeste'), ('26', 'PE', 'Nordeste'), ('27', 'AL', 'Nordeste'), ('28', 'SE', 'Nordeste'),
  ('29', 'BA', 'Nordeste'),
  ('31', 'MG', 'Sudeste'), ('32', 'ES', 'Sudeste'), ('33', 'RJ', 'Sudeste'), ('35', 'SP', 'Sudeste'),
  ('41', 'PR', 'Sul'), ('42', 'SC', 'Sul'), ('43', 'RS', 'Sul'),
  ('50', 'MS', 'Centro-Oeste'), ('51', 'MT', 'Centro-Oeste'), ('52', 'GO', 'Centro-Oeste'), ('53', 'DF', 'Centro-Oeste')
) AS t (cod_uf, sigla_uf, regiao);

CREATE OR REPLACE VIEW tc_fase2_silver.stg_alunos AS
WITH b AS (
  SELECT a.*, row_number() OVER (PARTITION BY ano, id_aluno ORDER BY id_escola) AS ocorrencia
  FROM tc_fase2_bronze.alunos a
  WHERE run_id = '${RUN_ID}'
),
rede_dic AS (
  SELECT chave FROM tc_fase2_bronze.dicionario
  WHERE run_id = '${RUN_ID}' AND id_tabela = 'alunos' AND nome_coluna = 'rede'
),
t AS (
  SELECT
    b.*,
    b.presenca = '1' AND b.preenchimento_caderno = '1' AS eh_avaliado,
    u.sigla_uf,
    u.regiao,
    d.chave AS rede_no_dicionario
  FROM b
  LEFT JOIN tc_fase2_silver.ref_uf u ON u.cod_uf = substr(b.id_municipio, 1, 2)
  LEFT JOIN rede_dic d ON d.chave = b.rede
)
SELECT
  CAST(ano AS integer) AS ano,
  id_aluno,
  id_municipio,
  sigla_uf,
  regiao,
  id_escola,
  try_cast(rede AS integer) AS rede,
  try_cast(serie AS integer) AS serie,
  caderno,
  presenca = '1' AS presente,
  preenchimento_caderno = '1' AS prova_preenchida,
  eh_avaliado AS avaliado,
  -- Nulo ≠ zero: quem não foi avaliado não é "não alfabetizado".
  CASE WHEN eh_avaliado THEN alfabetizado = '1' END AS alfabetizado,
  proficiencia,
  peso_aluno,
  run_id,
  -- Valores originais da fonte, para a quarentena guardar o registro como veio.
  json_format(CAST(MAP(
    ARRAY['id_municipio', 'id_escola', 'rede', 'serie', 'presenca', 'preenchimento_caderno', 'alfabetizado',
      'proficiencia', 'peso_aluno'],
    ARRAY[id_municipio, id_escola, rede, serie, presenca, preenchimento_caderno, alfabetizado,
      CAST(proficiencia AS varchar), CAST(peso_aluno AS varchar)]
  ) AS json)) AS registro_fonte,
  CASE
    WHEN ano IS NULL OR id_aluno IS NULL OR id_municipio IS NULL THEN 'chave_nula'
    WHEN ocorrencia > 1 THEN 'chave_duplicada'
    WHEN NOT regexp_like(id_municipio, '^[0-9]{7}$') OR sigla_uf IS NULL THEN 'id_municipio_invalido'
    WHEN rede_no_dicionario IS NULL THEN 'rede_fora_do_dicionario'
    WHEN coalesce(presenca, '') NOT IN ('0', '1') OR coalesce(preenchimento_caderno, '') NOT IN ('0', '1')
      OR coalesce(alfabetizado, '') NOT IN ('0', '1') THEN 'codigo_invalido'
    WHEN eh_avaliado AND coalesce(peso_aluno, 0) <= 0 THEN 'avaliado_sem_peso'
    WHEN eh_avaliado AND (proficiencia IS NULL OR proficiencia NOT BETWEEN 400 AND 1100) THEN 'proficiencia_fora_da_faixa'
  END AS motivo_quarentena
FROM t;

CREATE OR REPLACE VIEW tc_fase2_silver.stg_resultado_oficial AS
WITH r AS (
  SELECT 'municipio' AS nivel, id_municipio AS id_localidade, ano, rede, serie, taxa_alfabetizacao, media_portugues,
    proporcao_aluno_nivel_0, proporcao_aluno_nivel_1, proporcao_aluno_nivel_2, proporcao_aluno_nivel_3,
    proporcao_aluno_nivel_4, proporcao_aluno_nivel_5, proporcao_aluno_nivel_6, proporcao_aluno_nivel_7,
    proporcao_aluno_nivel_8, run_id
  FROM tc_fase2_bronze.municipio WHERE run_id = '${RUN_ID}'
  UNION ALL
  SELECT 'uf', sigla_uf, ano, rede, serie, taxa_alfabetizacao, media_portugues,
    proporcao_aluno_nivel_0, proporcao_aluno_nivel_1, proporcao_aluno_nivel_2, proporcao_aluno_nivel_3,
    proporcao_aluno_nivel_4, proporcao_aluno_nivel_5, proporcao_aluno_nivel_6, proporcao_aluno_nivel_7,
    proporcao_aluno_nivel_8, run_id
  FROM tc_fase2_bronze.uf WHERE run_id = '${RUN_ID}'
)
SELECT
  nivel, id_localidade, CAST(ano AS integer) AS ano, try_cast(rede AS integer) AS rede,
  try_cast(serie AS integer) AS serie, taxa_alfabetizacao, media_portugues,
  proporcao_aluno_nivel_0, proporcao_aluno_nivel_1, proporcao_aluno_nivel_2, proporcao_aluno_nivel_3,
  proporcao_aluno_nivel_4, proporcao_aluno_nivel_5, proporcao_aluno_nivel_6, proporcao_aluno_nivel_7,
  proporcao_aluno_nivel_8, run_id,
  CASE
    WHEN id_localidade IS NULL OR ano IS NULL OR rede IS NULL THEN 'chave_nula'
    WHEN count(*) OVER (PARTITION BY nivel, id_localidade, ano, rede, serie) > 1 THEN 'chave_duplicada'
    WHEN taxa_alfabetizacao NOT BETWEEN 0 AND 100 THEN 'taxa_fora_da_faixa'
  END AS motivo_quarentena
FROM r;

CREATE OR REPLACE VIEW tc_fase2_silver.stg_meta AS
WITH m AS (
  SELECT 'brasil' AS nivel, 'BR' AS id_localidade, ano, rede, taxa_alfabetizacao,
    meta_alfabetizacao_2024, meta_alfabetizacao_2025, meta_alfabetizacao_2026, meta_alfabetizacao_2027,
    meta_alfabetizacao_2028, meta_alfabetizacao_2029, meta_alfabetizacao_2030,
    CAST(NULL AS bigint) AS nivel_alfabetizacao, percentual_participacao, run_id
  FROM tc_fase2_bronze.meta_alfabetizacao_brasil WHERE run_id = '${RUN_ID}'
  UNION ALL
  SELECT 'uf', sigla_uf, ano, rede, taxa_alfabetizacao,
    meta_alfabetizacao_2024, meta_alfabetizacao_2025, meta_alfabetizacao_2026, meta_alfabetizacao_2027,
    meta_alfabetizacao_2028, meta_alfabetizacao_2029, meta_alfabetizacao_2030,
    NULL, percentual_participacao, run_id
  FROM tc_fase2_bronze.meta_alfabetizacao_uf WHERE run_id = '${RUN_ID}'
  UNION ALL
  SELECT 'municipio', id_municipio, ano, rede, taxa_alfabetizacao,
    meta_alfabetizacao_2024, meta_alfabetizacao_2025, meta_alfabetizacao_2026, meta_alfabetizacao_2027,
    meta_alfabetizacao_2028, meta_alfabetizacao_2029, meta_alfabetizacao_2030,
    nivel_alfabetizacao, percentual_participacao, run_id
  FROM tc_fase2_bronze.meta_alfabetizacao_municipio WHERE run_id = '${RUN_ID}'
),
t AS (
  -- As metas trazem a rede em texto e sem dicionário; validado na exploração:
  -- 'Municipal' = rede 3 da tabela municipio, 'Pública' = rede 5 (Estadual + Municipal) da tabela uf.
  SELECT m.*, CASE rede WHEN 'Municipal' THEN 3 WHEN 'Pública' THEN 5 END AS rede_cod FROM m
)
SELECT
  nivel, id_localidade, CAST(ano AS integer) AS ano_edicao, rede_cod AS rede, rede AS rede_texto_fonte,
  2 AS serie,  -- metas não têm série; todo o resultado da fonte é do 2º ano (achado 2 da exploração)
  taxa_alfabetizacao,
  meta_alfabetizacao_2024 AS meta_2024, meta_alfabetizacao_2025 AS meta_2025, meta_alfabetizacao_2026 AS meta_2026,
  meta_alfabetizacao_2027 AS meta_2027, meta_alfabetizacao_2028 AS meta_2028, meta_alfabetizacao_2029 AS meta_2029,
  meta_alfabetizacao_2030 AS meta_2030,
  CAST(nivel_alfabetizacao AS integer) AS nivel_alfabetizacao, percentual_participacao, run_id,
  CASE
    WHEN id_localidade IS NULL OR ano IS NULL THEN 'chave_nula'
    WHEN rede_cod IS NULL THEN 'rede_meta_desconhecida'
    WHEN count(*) OVER (PARTITION BY nivel, id_localidade, ano, rede) > 1 THEN 'chave_duplicada'
    WHEN greatest(
      coalesce(meta_alfabetizacao_2024, 0), coalesce(meta_alfabetizacao_2030, 0), coalesce(taxa_alfabetizacao, 0)
    ) > 100 THEN 'valor_fora_da_faixa'
  END AS motivo_quarentena
FROM t;
