#!/usr/bin/env bash
# Sobe (ou atualiza) a orquestração na conta do Learner Lab. Pode rodar quantas vezes quiser.
#   Lambda tc-fase2-sql        executa os .sql do repositório no Athena, com o mesmo runner do uso local
#   Step Function tc-fase2     Bronze -> qualidade -> portão -> Silver -> Gold -> export; se falhar, grava em
#                              run_metrics e avisa no SNS
#   SNS tc-fase2-alertas       e-mail de alerta para ALERTA_EMAIL (é preciso confirmar a assinatura no e-mail)
#   Scheduler tc-fase2-diario  todo dia às 06:00 (America/Sao_Paulo), sobre o último manifesto da Bronze
#   Alarme do CloudWatch       ExecutionsFailed >= 1 -> SNS, para o caso de a própria máquina de estados quebrar
#   Ciclo de vida do S3        athena-results/ expira em 7 dias (resultado de consulta não precisa ficar)
# Tudo usa a LabRole, porque o Learner Lab não deixa criar papéis IAM.
# Uso: set -a; . ./.env; set +a; bash 03_orquestracao/implantar.sh
set -euo pipefail
cd "$(dirname "$0")/.."
REGIAO=${AWS_REGION:-us-east-1}
export AWS_DEFAULT_REGION=$REGIAO
CONTA=$(aws sts get-caller-identity --query Account --output text)
PAPEL=arn:aws:iam::$CONTA:role/LabRole
TAGS='[{"Key":"projeto","Value":"tc-fase2"}]'

pacote=$(mktemp -d)/lambda.zip
zip -qj "$pacote" apoio/athena.py 03_orquestracao/lambda_sql.py
zip -qr "$pacote" sql
if aws lambda get-function --function-name tc-fase2-sql >/dev/null 2>&1; then
  aws lambda update-function-code --function-name tc-fase2-sql --zip-file "fileb://$pacote" >/dev/null
else
  aws lambda create-function --function-name tc-fase2-sql --runtime python3.12 --handler lambda_sql.handler \
    --role "$PAPEL" --timeout 900 --memory-size 256 --zip-file "fileb://$pacote" \
    --environment "Variables={S3_BUCKET=$S3_BUCKET}" --tags projeto=tc-fase2 >/dev/null
fi
aws lambda wait function-updated-v2 --function-name tc-fase2-sql
LAMBDA_ARN=$(aws lambda get-function --function-name tc-fase2-sql --query Configuration.FunctionArn --output text)

TOPICO_ARN=$(aws sns create-topic --name tc-fase2-alertas --tags Key=projeto,Value=tc-fase2 --query TopicArn --output text)
if ! aws sns list-subscriptions-by-topic --topic-arn "$TOPICO_ARN" --query 'Subscriptions[].Endpoint' --output text | grep -q "$ALERTA_EMAIL"; then
  aws sns subscribe --topic-arn "$TOPICO_ARN" --protocol email --notification-endpoint "$ALERTA_EMAIL" >/dev/null
  echo "Assinatura criada: confirme o e-mail da AWS em $ALERTA_EMAIL"
fi

definicao=$(sed -e "s|\${LAMBDA_ARN}|$LAMBDA_ARN|g" -e "s|\${TOPICO_ARN}|$TOPICO_ARN|g" -e "s|\${REGIAO}|$REGIAO|g" \
  03_orquestracao/pipeline.asl.json)
SFN_ARN=arn:aws:states:$REGIAO:$CONTA:stateMachine:tc-fase2
if aws stepfunctions describe-state-machine --state-machine-arn "$SFN_ARN" >/dev/null 2>&1; then
  aws stepfunctions update-state-machine --state-machine-arn "$SFN_ARN" --definition "$definicao" >/dev/null
else
  aws stepfunctions create-state-machine --name tc-fase2 --role-arn "$PAPEL" --definition "$definicao" \
    --tags key=projeto,value=tc-fase2 >/dev/null
fi

alvo="{\"Arn\":\"$SFN_ARN\",\"RoleArn\":\"$PAPEL\",\"Input\":\"{}\"}"
acao=create-schedule
aws scheduler get-schedule --name tc-fase2-diario >/dev/null 2>&1 && acao=update-schedule
aws scheduler $acao --name tc-fase2-diario --schedule-expression "cron(0 6 * * ? *)" \
  --schedule-expression-timezone America/Sao_Paulo --flexible-time-window Mode=OFF --target "$alvo" >/dev/null

aws cloudwatch put-metric-alarm --alarm-name tc-fase2-execucao-falhou --namespace AWS/States \
  --metric-name ExecutionsFailed --dimensions Name=StateMachineArn,Value="$SFN_ARN" --statistic Sum \
  --period 300 --evaluation-periods 1 --threshold 1 --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching --alarm-actions "$TOPICO_ARN" --tags "$TAGS"

aws s3api put-bucket-lifecycle-configuration --bucket "$S3_BUCKET" --lifecycle-configuration \
  '{"Rules":[{"ID":"athena-results-7d","Status":"Enabled","Filter":{"Prefix":"athena-results/"},"Expiration":{"Days":7}}]}'

echo "{\"state_machine\":\"$SFN_ARN\",\"lambda\":\"$LAMBDA_ARN\",\"topico\":\"$TOPICO_ARN\"}"
