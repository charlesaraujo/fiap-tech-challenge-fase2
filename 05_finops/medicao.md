## Medição (conta do Learner Lab, us-east-1)

Medido em 22/10/2026.

| Recurso | Quantidade medida | Custo no período |
| --- | --- | --- |
| S3 `athena-results/` | 0.1 MB | US$ 0.0000/mês |
| S3 `bronze/` | 273.1 MB | US$ 0.0061/mês |
| S3 `export/` | 6.3 MB | US$ 0.0001/mês |
| S3 `gold/` | 1.1 MB | US$ 0.0000/mês |
| S3 `ops/` | 1.0 MB | US$ 0.0000/mês |
| S3 `silver/` | 539.6 MB | US$ 0.0121/mês |
| Athena: 689 consultas | 8.02 GB escaneados; 14.21 GB cobrados (mínimo de 10 MB por consulta) | US$ 0.0694 |
| Lambda `tc-fase2-sql` | 50 invocações, 1,295 s a 256 MB | US$ 0.0054 |
| Lambda `tc-fase2-consumidor` | 2 invocações, 20 s a 256 MB | US$ 0.0001 |
| Step Functions | 7 execuções × ~16 transições | US$ 0.0028 |

### Projeção mensal (1 execução por dia, 30 dias)

| Item | Base | US$/mês |
| --- | --- | --- |
| S3 | 821 MB armazenados (cresce ~70 MB por extração nova na Bronze) | 0.018 |
| Athena | execução de referência `real-20261022-1500`: 90 consultas, 850.9 MB escaneados, 1,678 MB cobrados | 0.240 |
| Lambda + Step Functions | 317 s de Lambda e ~16 transições por execução | 0.052 |
| Alarme CloudWatch | 1 alarme | 0.10 |
| Scheduler, SNS (e-mail), Glue Catalog | dentro do nível gratuito | 0,00 |
| Kinesis (se ficasse ligado) | 1 shard × 730 h | 10.95 |
