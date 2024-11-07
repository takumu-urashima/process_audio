import os
import json
import logging
import traceback
import uuid
import time
import requests
from .aws_clients import transcribe_client, bedrock_runtime_client
from dotenv import load_dotenv

# .envファイルをロード
load_dotenv()

# 環境変数から設定を取得
DATA_ACCESS_ROLE_ARN = os.getenv('DATA_ACCESS_ROLE_ARN')

logger = logging.getLogger(__name__)

def get_transcript(job_name, bucket_name, object_key):
    """
    音声ファイルを文字起こししての文字起こしを取得。

    Args:
        job_name (str): ジョブ名
        bucket_name (str): S3バケット名
        object_key (str): S3オブジェクトキー

    Returns:
        str: 文字起こしされたテキスト
    """
    try:
        response = transcribe_client.start_call_analytics_job(
            CallAnalyticsJobName=job_name,
            Media={
                'MediaFileUri': f's3://{bucket_name}/{object_key}'
            },
            Settings={
                'VocabularyName': 'ohaka-word',
                'LanguageOptions': ['ja-JP']
            },
            ChannelDefinitions=[
                {'ChannelId': 0, 'ParticipantRole': 'AGENT'},
                {'ChannelId': 1, 'ParticipantRole': 'CUSTOMER'}
            ],
            DataAccessRoleArn=DATA_ACCESS_ROLE_ARN
        )
        
        # ジョブの完了を待機
        while True:
            job_status = transcribe_client.get_call_analytics_job(CallAnalyticsJobName=job_name)['CallAnalyticsJob']['CallAnalyticsJobStatus']
            logger.info(f"ジョブ {job_name} の現在のステータス: {job_status}")
            
            if job_status in ('COMPLETED', 'FAILED'):
                break
            time.sleep(30)
        
        if job_status == 'FAILED':
            raise Exception(f"Transcribeジョブ {job_name} が失敗しました")
    
        response = transcribe_client.get_call_analytics_job(CallAnalyticsJobName=job_name)
        transcript_file_uri = response['CallAnalyticsJob']['Transcript']['TranscriptFileUri']
        
        # Transcriptファイルの内容を取得
        transcript_response = requests.get(transcript_file_uri)
        transcript_data = transcript_response.json()
    
        all_transcripts = ""
        previous_role = None
    
        for transcript in transcript_data.get("Transcript", []):
            role = transcript.get("ParticipantRole")
            content = transcript.get("Content", "")
            role_name = "事業者様" if role == "AGENT" else "お客様" if role == "CUSTOMER" else "未知の参加者"
    
            if previous_role and previous_role != role:
                all_transcripts += "\n"
    
            formatted_content = f"[{role_name}] {content}"
            all_transcripts += formatted_content
            previous_role = role
        
        return all_transcripts
    
    except Exception as e:
        logger.error(f"文字起こしの取得中にエラーが発生しました: {str(e)}")
        logger.error(traceback.format_exc())
        raise


def get_summary(transcript_text):
    """
    文字起こしテキストを要約。

    Args:
        transcript_text (str): 文字起こしされたテキスト

    Returns:
        Dict[str, Any]: 要約結果のJSON
    """
    model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"
    
    prompt = f"""
    Human: 次の文章は、霊園・墓地を管理している事業者とその事業者に要件があり電話した人（お客様）との録音音声を文字起こししたものです。
    以下の内容を文章から読み取り、指定されたキーと値のペアを持つJSON形式で応答してください。
    
    1. "category": お客様の区分を電話内容によって、お墓の相談、見学予約、資料送付、墓じまい相談、対象外、不通に分けてください。また、お墓参りについてや開園時間、最寄り駅などの質問は対象外に含まれます。
    2. "customer_info": お客様の区分を、以前に霊園・墓地の事業者とやり取りがなかったら新規、以前にお墓を施工していた場合や霊園・墓地の事業者がお客様を知得していた場合はその他としてください。
    3. "customer_name": お客様の名前を教えてください。漢字の説明がない場合はカタカナにしてください。
    5. "next_action": 今後の対応(資料送付、検討、折り返し電話、何月何日に見学など)を教えてください。
    6. "status": categoryがお墓の相談、見学予約、資料送付、墓じまい相談、不通だった場合、有効としてください。それ以外は無効としてください。
    7. "summary_content": 以下の項目ごとに通話内容を要約してください。
        - 希望の霊園・墓地
        - 希望のお墓の種類
        - 希望の地域
        - 見学希望日
        - その他の希望
        - 会話全体の簡単な要約
    
    JSON形式の応答では、指定されたキーを必ず使用し、対応する値を提供してください。情報が欠落している場合は、空の文字列を使用してください。
    
    {transcript_text}
    """

    try:
        response = bedrock_runtime_client.invoke_model(
            modelId=model_id,
            body=json.dumps({
                "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 2000,
                "temperature": 0.8,
                "top_p": 0.999,
                "stop_sequences": ["\n\nHuman:"]
            }),
            contentType='application/json'
        )
    
        response_body = json.loads(response['body'].read().decode('utf-8'))
        completion_text = response_body['content'][0]['text']
        logger.info(f"Bedrockからのレスポンス: {completion_text}")
        completion_json = json.loads(completion_text)
    
        return completion_json
    
    except Exception as e:
        logger.error(f"要約の取得中にエラーが発生しました: {str(e)}")
        logger.error(traceback.format_exc())
        raise


def process_audio(sqs_data):
    """
    音声データを処理し、文字起こしと要約を行います。

    Args:
        sqs_data (Dict[str, Any]): SQSから取得したデータ

    Returns:
        Dict[str, Any]: 処理結果
    """
    audio_path = sqs_data.get('audio_path', '')
    metadata = sqs_data.get('metadata', {})
    
    logger.info(f"音声パスの処理: {audio_path}")
    logger.info(f"メタデータ: {metadata}")
    
    if audio_path.endswith('.flac'):
        try:
            bucket_name = audio_path.split('/')[2]
            object_key = '/'.join(audio_path.split('/')[3:])
            
            job_name = f"transcribe-job-{uuid.uuid4()}"
            transcript_text = get_transcript(job_name, bucket_name, object_key)
            logger.info(f"文字起こし結果:\n{transcript_text}")
            
            summary = get_summary(transcript_text)
            logger.info(f"要約結果: {summary}")
    
            return {
                'transcript': transcript_text,
                'summary': summary,
                'metadata': metadata
            }
        except Exception as e:
            logger.error(f"音声処理中にエラーが発生しました: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    else:
        logger.error(f"無効な音声ファイルパス: {audio_path}")
        return None

def main(sqs_data):
    """
    メイン処理関数。

    Args:
        sqs_data (Dict[str, Any]): SQSから取得したデータ

    Returns:
        Dict[str, Any]: 処理結果
    """
    try:
        result = process_audio(sqs_data)
        if result:
            logger.info("音声処理が正常に完了しました")
            return result
        else:
            logger.error("処理する結果がありません")
            return None
    except Exception as e:
        error_message = f"メッセージ処理中にエラーが発生しました: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_message)
        return None