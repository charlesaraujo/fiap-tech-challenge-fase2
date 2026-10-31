-- Manutenção das tabelas Iceberg da Silver e de ops. Cada MERGE/DELETE+INSERT gera arquivos e snapshots
-- novos e mantém os antigos para time travel: sem manutenção, a Silver cresce a cada execução.
-- OPTIMIZE compacta arquivos pequenos (bin pack); VACUUM expira snapshots mais velhos que a retenção e
-- apaga os arquivos que só eles referenciavam. Retenção de 1 dia: suficiente para reprocessar/auditar o dia.

ALTER TABLE tc_fase2_silver.alunos SET TBLPROPERTIES ('vacuum_max_snapshot_age_seconds' = '86400');
OPTIMIZE tc_fase2_silver.alunos REWRITE DATA USING BIN_PACK;
VACUUM tc_fase2_silver.alunos;

ALTER TABLE tc_fase2_silver.resultado_oficial SET TBLPROPERTIES ('vacuum_max_snapshot_age_seconds' = '86400');
OPTIMIZE tc_fase2_silver.resultado_oficial REWRITE DATA USING BIN_PACK;
VACUUM tc_fase2_silver.resultado_oficial;

ALTER TABLE tc_fase2_silver.meta SET TBLPROPERTIES ('vacuum_max_snapshot_age_seconds' = '86400');
OPTIMIZE tc_fase2_silver.meta REWRITE DATA USING BIN_PACK;
VACUUM tc_fase2_silver.meta;

ALTER TABLE tc_fase2_silver.quarentena SET TBLPROPERTIES ('vacuum_max_snapshot_age_seconds' = '86400');
OPTIMIZE tc_fase2_silver.quarentena REWRITE DATA USING BIN_PACK;
VACUUM tc_fase2_silver.quarentena;

ALTER TABLE tc_fase2_ops.quality_results SET TBLPROPERTIES ('vacuum_max_snapshot_age_seconds' = '86400');
OPTIMIZE tc_fase2_ops.quality_results REWRITE DATA USING BIN_PACK;
VACUUM tc_fase2_ops.quality_results;
