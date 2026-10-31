#!/usr/bin/env bash
# Desliga o streaming depois da demonstração, porque o shard do Kinesis é cobrado por hora mesmo sem uso.
# O que já foi gravado (linhas com origem = 'simulado' na Silver e ops.streaming_metrics) continua lá.
set -euo pipefail
export AWS_DEFAULT_REGION=${AWS_REGION:-us-east-1}
for u in $(aws lambda list-event-source-mappings --function-name tc-fase2-consumidor --query 'EventSourceMappings[].UUID' --output text); do
  aws lambda delete-event-source-mapping --uuid "$u" >/dev/null
done
aws kinesis delete-stream --stream-name tc-fase2-eventos
aws cloudwatch delete-alarms --alarm-names tc-fase2-streaming-erros tc-fase2-streaming-atraso
echo "stream tc-fase2-eventos e alarmes do streaming removidos"
