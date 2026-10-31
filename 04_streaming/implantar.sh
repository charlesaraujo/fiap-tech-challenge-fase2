#!/usr/bin/env bash
# Sobe o streaming: stream Kinesis tc-fase2-eventos (1 shard) e Lambda tc-fase2-consumidor lendo em micro-lotes
# (até 500 eventos ou 20 s), com retentativa e alarmes de erro e de atraso no SNS tc-fase2-alertas.
# O shard é cobrado por hora, então rode 04_streaming/remover.sh quando terminar a demonstração.
# Uso: set -a; . ./.env; set +a; bash 04_streaming/implantar.sh
set -euo pipefail
export AWS_DEFAULT_REGION=${AWS_REGION:-us-east-1}
cd "$(dirname "$0")/.."
CONTA=$(aws sts get-caller-identity --query Account --output text)
PAPEL=arn:aws:iam::$CONTA:role/LabRole

aws kinesis describe-stream-summary --stream-name tc-fase2-eventos >/dev/null 2>&1 ||
  aws kinesis create-stream --stream-name tc-fase2-eventos --shard-count 1
aws kinesis wait stream-exists --stream-name tc-fase2-eventos
aws kinesis add-tags-to-stream --stream-name tc-fase2-eventos --tags projeto=tc-fase2
STREAM_ARN=$(aws kinesis describe-stream-summary --stream-name tc-fase2-eventos --query StreamDescriptionSummary.StreamARN --output text)

pacote=$(mktemp -d)/lambda.zip
zip -qj "$pacote" apoio/athena.py 04_streaming/lambda_consumidor.py
if aws lambda get-function --function-name tc-fase2-consumidor >/dev/null 2>&1; then
  aws lambda update-function-code --function-name tc-fase2-consumidor --zip-file "fileb://$pacote" >/dev/null
else
  aws lambda create-function --function-name tc-fase2-consumidor --runtime python3.12 \
    --handler lambda_consumidor.handler --role "$PAPEL" --timeout 300 --memory-size 256 \
    --zip-file "fileb://$pacote" --environment "Variables={S3_BUCKET=$S3_BUCKET}" --tags projeto=tc-fase2 >/dev/null
fi
aws lambda wait function-updated-v2 --function-name tc-fase2-consumidor

# Se um lote der erro, o gatilho tenta de novo até 2 vezes, partindo o lote ao meio para achar o registro ruim.
# Depois disso, os metadados do lote vão para o SNS de alertas e o shard segue em frente. Eventos com mais de
# 1 h seguem o mesmo caminho.
TOPICO_ARN=$(aws sns create-topic --name tc-fase2-alertas --tags Key=projeto,Value=tc-fase2 --query TopicArn --output text)
falhas=(--maximum-retry-attempts 2 --bisect-batch-on-function-error --maximum-record-age-in-seconds 3600
  --destination-config "{\"OnFailure\":{\"Destination\":\"$TOPICO_ARN\"}}")
uuid=$(aws lambda list-event-source-mappings --function-name tc-fase2-consumidor --query 'EventSourceMappings[0].UUID' --output text)
if [ "$uuid" = "None" ] || [ -z "$uuid" ]; then
  aws lambda create-event-source-mapping --function-name tc-fase2-consumidor --event-source-arn "$STREAM_ARN" \
    --starting-position TRIM_HORIZON --batch-size 500 --maximum-batching-window-in-seconds 20 "${falhas[@]}" >/dev/null
else
  aws lambda update-event-source-mapping --uuid "$uuid" "${falhas[@]}" >/dev/null
fi

# Alarmes do consumidor no mesmo tópico do batch: erro de invocação e atraso de leitura (IteratorAge, o "lag" do
# Kinesis). Escolhemos 5 min porque é umas 7 vezes o p95 que medimos (40 s).
alarme() {
  aws cloudwatch put-metric-alarm --alarm-name "$1" --namespace AWS/Lambda --metric-name "$2" --statistic "$3" \
    --dimensions Name=FunctionName,Value=tc-fase2-consumidor --period 300 --evaluation-periods 1 --threshold "$4" \
    --comparison-operator GreaterThanOrEqualToThreshold --treat-missing-data notBreaching \
    --alarm-actions "$TOPICO_ARN" --tags Key=projeto,Value=tc-fase2
}
alarme tc-fase2-streaming-erros Errors Sum 1
alarme tc-fase2-streaming-atraso IteratorAge Maximum 300000

echo "{\"stream\":\"$STREAM_ARN\"}"
