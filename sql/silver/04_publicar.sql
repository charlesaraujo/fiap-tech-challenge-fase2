-- Publicação: só roda depois do portão (03). Entram na Silver só as linhas sem motivo de quarentena.
-- alunos usa MERGE pela chave (ano, id_aluno): reprocessar o mesmo run_id não duplica nada,
-- e o mesmo MERGE recebe os eventos do streaming (origem = 'simulado').

MERGE INTO tc_fase2_silver.alunos t
USING (SELECT * FROM tc_fase2_silver.stg_alunos WHERE motivo_quarentena IS NULL) s
ON t.ano = s.ano AND t.id_aluno = s.id_aluno
WHEN MATCHED THEN UPDATE SET
  id_municipio = s.id_municipio, sigla_uf = s.sigla_uf, regiao = s.regiao, id_escola = s.id_escola, rede = s.rede,
  serie = s.serie, caderno = s.caderno, presente = s.presente, prova_preenchida = s.prova_preenchida,
  avaliado = s.avaliado, alfabetizado = s.alfabetizado, proficiencia = s.proficiencia, peso_aluno = s.peso_aluno,
  origem = 'oficial', run_id = s.run_id, atualizado_em = CAST(current_timestamp AS timestamp(6))
WHEN NOT MATCHED THEN INSERT (
  ano, id_aluno, id_municipio, sigla_uf, regiao, id_escola, rede, serie, caderno, presente, prova_preenchida, avaliado,
  alfabetizado, proficiencia, peso_aluno, origem, run_id, atualizado_em
) VALUES (
  s.ano, s.id_aluno, s.id_municipio, s.sigla_uf, s.regiao, s.id_escola, s.rede, s.serie, s.caderno, s.presente,
  s.prova_preenchida, s.avaliado, s.alfabetizado, s.proficiencia, s.peso_aluno, 'oficial', s.run_id,
  CAST(current_timestamp AS timestamp(6))
);

-- Tabelas pequenas (≤ 25 mil linhas): snapshot completo a cada execução.
DELETE FROM tc_fase2_silver.resultado_oficial WHERE true;

INSERT INTO tc_fase2_silver.resultado_oficial
SELECT nivel, id_localidade, ano, rede, serie, taxa_alfabetizacao, media_portugues,
  proporcao_aluno_nivel_0, proporcao_aluno_nivel_1, proporcao_aluno_nivel_2, proporcao_aluno_nivel_3,
  proporcao_aluno_nivel_4, proporcao_aluno_nivel_5, proporcao_aluno_nivel_6, proporcao_aluno_nivel_7,
  proporcao_aluno_nivel_8, run_id, CAST(current_timestamp AS timestamp(6))
FROM tc_fase2_silver.stg_resultado_oficial WHERE motivo_quarentena IS NULL;

DELETE FROM tc_fase2_silver.meta WHERE true;

INSERT INTO tc_fase2_silver.meta
SELECT nivel, id_localidade, ano_edicao, rede, rede_texto_fonte, serie, taxa_alfabetizacao,
  meta_2024, meta_2025, meta_2026, meta_2027, meta_2028, meta_2029, meta_2030,
  nivel_alfabetizacao, percentual_participacao, run_id, CAST(current_timestamp AS timestamp(6))
FROM tc_fase2_silver.stg_meta WHERE motivo_quarentena IS NULL;

DELETE FROM tc_fase2_silver.dicionario WHERE true;

INSERT INTO tc_fase2_silver.dicionario
SELECT id_tabela, nome_coluna, chave, valor, run_id
FROM tc_fase2_bronze.dicionario WHERE run_id = '${RUN_ID}';
