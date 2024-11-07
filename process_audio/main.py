#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import traceback
import time

# 他のファイルをインポート
import process_audio.src.sqs_monitor as sqs_monitor
import process_audio.src.summarize as summarize
import process_audio.src.kintone as kintone

# ロガーを設定
logger = logging.getLogger(__name__)

def run_sqs_monitor():
    """
    SQS モニターを実行し、メッセージを取得。

    Returns:
        Optional[Dict[str, Any]]: SQSメッセージの内容、取得できなかった場合はNone
    """
    logger.info("SQS モニター実行中")
    result = sqs_monitor.main()
    return result


def run_summarize(data):
    """
    音声ファイルデータから文字起こしし、それを要約。

    Args:
        data (Dict[str, Any]): SQSから取得したデータ

    Returns:
        Optional[Dict[str, Any]]: 要約結果、失敗時はNone
    """
    logger.info("要約実行中")
    result = summarize.main(data)
    return result


def run_kintone(data):
    """
    要約データを基にKintoneの送客管理レコードを更新。

    Args:
        data (Dict[str, Any]): 要約データ

    Returns:
        Optional[Any]: Kintoneの更新結果、失敗時はNone
    """
    logger.info("Kintoneレコード更新実行中")
    result = kintone.main(data)
    return result


def main():
    """
    メイン処理関数。SQSからメッセージを受信し、要約およびKintoneのレコード更新を行う。
    永続的に実行され、エラー発生時はログを記録して60秒待機後に再試行。
    """
    while True:
        try:
            sqs_data = run_sqs_monitor()
            if sqs_data is None:
                logger.info("SQSにメッセージがありません。60秒後に再試行します。")
                time.sleep(60)
                continue

            metadata = sqs_data.get('metadata', {})
            callnote_unique_id = metadata.get('uuid', '')

            if not callnote_unique_id:
                logger.warning("Callnoteの録音ファイルIDが取得できませんでした。メッセージをスキップします。")
                time.sleep(60)
                continue

            summarize_data = run_summarize(sqs_data)
            if summarize_data is None:
                raise Exception("SummarizeがNoneを返しました")

            kintone_data = run_kintone(summarize_data)
            if kintone_data is None:
                raise Exception("Kintoneのデータ更新が失敗しました")

            logger.info("全ての処理が正常に完了しました")

        except Exception as e:
            error_message = f"エラーが発生しました: {str(e)}"
            logger.error(error_message)
            logger.error(traceback.format_exc())
            # エラー発生時もループを継続
            logger.info("60秒後に再試行します。")
            time.sleep(60)  # エラー後も60秒待機

if __name__ == "__main__":
    main()