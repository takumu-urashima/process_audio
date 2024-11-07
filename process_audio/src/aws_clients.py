import boto3

# AWSクライアントの再利用用モジュール

# Transcribeクライアントをグローバルに初期化
transcribe_client = boto3.client('transcribe', region_name='ap-northeast-1')

# Bedrock Runtimeクライアントをグローバルに初期化
bedrock_runtime_client = boto3.client('bedrock-runtime', region_name='ap-northeast-1')

# SQSクライアントをグローバルに初期化
sqs_client = boto3.client('sqs', region_name='ap-northeast-1')