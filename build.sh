#!/bin/bash
set -e
AWS_REGION="ap-northeast-2"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

#Lambda배포
deploy_lambda () {
    FUNC_NAME=$1
    ROLE_ARN=$2
    SRC_DIR=$3
    RULE_NAME=$4
    EVENT_PATTERN=$5

    echo "소스코드 패키징: $FUNC_NAME"
    rm -f function.zip
    cd "$SRC_DIR"
    zip -r9 ../function.zip .
    cd ..

    if aws lambda get-function --function-name "$FUNC_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
        echo "♻️ Lambda 업데이트: $FUNC_NAME"
        aws lambda update-function-code \
            --function-name "$FUNC_NAME" \
            --zip-file "fileb://function.zip" \
            --region "$AWS_REGION"
    else
        echo "🚀 Lambda 생성: $FUNC_NAME"
        aws lambda create-function \
            --function-name "$FUNC_NAME" \
            --runtime python3.11 \
            --role "$ROLE_ARN" \
            --handler lambda_function.lambda_handler \
            --zip-file "fileb://function.zip" \
            --region "$AWS_REGION"
    fi

    echo "📡 EventBridge 규칙 생성: $RULE_NAME"
    aws events put-rule \
        --name "$RULE_NAME" \
        --event-pattern "$EVENT_PATTERN" \
        --region "$AWS_REGION"

    echo "🔑 Lambda Invoke 권한 부여"
    aws lambda add-permission \
        --function-name "$FUNC_NAME" \
        --statement-id "${RULE_NAME}-Invoke" \
        --action "lambda:InvokeFunction" \
        --principal events.amazonaws.com \
        --source-arn "arn:aws:events:${AWS_REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" \
        --region "$AWS_REGION" \
        || echo "⚠️ 권한 이미 있음"

    echo "🔗 EventBridge → Lambda 연결"
    aws events put-targets \
        --rule "$RULE_NAME" \
        --targets "Id"="1","Arn"="$(aws lambda get-function --function-name "$FUNC_NAME" --region "$AWS_REGION" --query 'Configuration.FunctionArn' --output text)" \
        --region "$AWS_REGION"
}

ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/your-lambda-role"
deploy_lambda "s3-import-lambda" "$ROLE_ARN "./s3_import"\
    
deploy_lambda "dataset-import-lambda" "$ROLE_ARN" "./dataset_import" \
    "dataset-import-active" \
    '{
        "source": ["aws.personalize"],
        "detail-type": ["Personalize Dataset Import Job State Change"],
        "detail": { "status": ["ACTIVE"] }
    }'

deploy_lambda "create-solution-lambda" "$ROLE_ARN" "./create_solution" \
    "solution-active" \
    '{
        "source": ["aws.personalize"],
        "detail-type": ["Personalize Solution State Change"],
        "detail": { "status": ["ACTIVE"] }
    }'

deploy_lambda "create-batch-lambda" "$ROLE_ARN" "./create_batch" \
    "solution-version-active" \
    '{
        "source": ["aws.personalize"],
        "detail-type": ["Personalize Solution Version State Change"],
        "detail": { "status": ["ACTIVE"] }
    }'

deploy_lambda "load-s3-lambda" "$ROLE_ARN" "./load_s3" \
    "batch-inference-active" \
    '{
        "source": ["aws.personalize"],
        "detail-type": ["Personalize Batch Inference Job State Change"],
        "detail": { "status": ["ACTIVE"] }
    }'

echo "✅ 전체 배포 완료"
