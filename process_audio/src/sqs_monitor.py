import os
import json
import logging
from botocore.exceptions import ClientError
from .aws_clients import sqs_client
from dotenv import load_dotenv

# .envファイルをロード
load_dotenv()

# 環境変数の取得
QUEUE_URL = os.getenv('QUEUE_URL')

logger = logging.getLogger(__name__)

def get_sqs_message():
    """
    SQSからメッセージを取得し、録音ファイルの情報を返す。

    Returns:
        Optional[Dict[str, Any]]: メッセージの内容またはNone
    """
    try:
        response = sqs_client.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,
            VisibilityTimeout=30
        )

        messages = response.get('Messages', [])

        if not messages:
            logger.info("キューにメッセージがありません。")
            return None

        message = messages[0]
        receipt_handle = message['ReceiptHandle']

        try:
            body = json.loads(message['Body'])
            audio_path = body.get('audio_path')
            metadata = body.get('metadata')

            if not audio_path or not metadata:
                raise ValueError("メッセージフォーマットが無効または必要なフィールドが欠落しています。")

            # メッセージを削除
            sqs_client.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
            logger.info("キューからメッセージを削除しました。")

            return {'audio_path': audio_path, 'metadata': metadata}

        except json.JSONDecodeError as e:
            logger.error(f"メッセージボディの解析に失敗しました: {str(e)}")
            sqs_client.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
        except ValueError as e:
            logger.error(str(e))
            sqs_client.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
        except Exception as e:
            logger.error(f"メッセージ処理中に予期せぬエラーが発生しました: {str(e)}")
            sqs_client.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)

    except ClientError as e:
        logger.error(f"Boto3エラー: {str(e)}")
    except Exception as e:
        logger.error(f"get_sqs_message関数内で予期せぬエラーが発生しました: {str(e)}", exc_info=True)

    return None


def main():
    """
    メイン関数。SQSからメッセージを取得。

    Returns:
        Optional[Dict[str, Any]]: メッセージの内容またはNone
    """
    return get_sqs_message()

if __name__ == "__main__":
    result = main()
    print(json.dumps(result))  # masterから呼び出された時にJSONとして結果を返す