import os
import json
import requests
import logging
import traceback
from dotenv import load_dotenv

# .envファイルをロード
load_dotenv()

# 環境変数から設定を取得
KINTONE_DOMAIN = os.getenv('KINTONE_DOMAIN')
KINTONE_API_TOKEN = os.getenv('KINTONE_API_TOKEN')
CUSTOMER_REFERRAL_APP_ID = os.getenv('CUSTOMER_REFERRAL_APP_ID')

# ロガーを設定
logger = logging.getLogger(__name__)

def update_kintone_transcript_record(uuid, formatted_transcript_text, summary_data):
    """
    Kintoneのレコードを更新。

    Args:
        uuid (str): 録音ファイルID
        data (str): 文字起こしデータ
        summary_data (Dict[str, Any]): 要約データ

    Returns:
        Optional[requests.Response]: APIのレスポンス、失敗時はNone
    """
    api_url = f"{KINTONE_DOMAIN}/k/v1/record.json"

    headers = {
        "X-Cybozu-API-Token": KINTONE_API_TOKEN,
        "Content-Type": "application/json",
    }

    summary_content = summary_data.get("summary_content", {})
    summary_content_str = "\n".join(
        f"{key}: {value}" for key, value in summary_content.items()
    )

    payload = {
        "app": CUSTOMER_REFERRAL_APP_ID,
        "updateKey": {
            "field": "録音ファイルID",
            "value": uuid
        },
        "record": {
            "AI要約": {"value": summary_content_str},
            "録音ファイル文字起こし": {"value": formatted_transcript_text},
            "送客内容": {"value": summary_data.get("category", "")},
            "問い合わせ者情報": {"value": summary_data.get("customer_info", "")},
            "問合せ者名": {"value": summary_data.get("customer_name", "")},
            "送客種別": {"value": summary_data.get("status", "")},
            "備考": {"value": summary_data.get("next_action", "")},
        }
    }

    try:
        resp = requests.put(api_url, headers=headers, data=json.dumps(payload))
        resp.raise_for_status()
        logger.info("Kintoneレコードが正常に更新されました。")
        return resp
    except requests.RequestException as e:
        logger.error(f"Kintone APIエラー: {str(e)}")
        logger.error(traceback.format_exc())
        return None


def main(summarize_data):
    """
    メイン処理関数。Kintoneのレコードを更新し、APIレスポンスを返す。

    Args:
        summarize_data (Dict[str, Any]): 要約データ

    Returns:
        Optional[requests.Response]: Kintone APIのレスポンス、失敗時はNone
    """
    # summarize_dataのチェックと出力
    if summarize_data:
        logger.info(f"summarize_data: {json.dumps(summarize_data, ensure_ascii=False, indent=2)}")
        logger.info("summarize_data が存在します")
    else:
        logger.info("summarize_data が空またはNoneです")
        return None  # summarize_dataが無効な場合はNoneを返す

    try:
        uuid = summarize_data['metadata']['uuid']
        transcript_data = summarize_data['transcript']
        summary_data = summarize_data['summary']

        logger.info(f"Processing UUID: {uuid}")
        logger.info(f"Transcript data: {transcript_data[:100]}...")
        logger.info(f"Summary data: {json.dumps(summary_data, ensure_ascii=False)}")

        if uuid:
            formatted_transcript_text = transcript_data.replace('[お客様]', '\n[お客様]').replace('[事業者様]', '\n[事業者様]')
            resp = update_kintone_transcript_record(uuid, formatted_transcript_text, summary_data)
            if resp is None:
                raise Exception("Kintoneレコードの更新に失敗しました。")
            return resp
        else:
            logger.warning("metadataにUUIDが見つかりません")
            return None

    except KeyError as e:
        logger.error(f"必要なキーが見つかりません: {str(e)}")
        logger.error(traceback.format_exc())
    except Exception as e:
        logger.error(f"main関数でエラーが発生しました: {str(e)}")
        logger.error(traceback.format_exc())

    return None