import json
import csv
import argparse
from datetime import datetime, timedelta, timezone
from collections import defaultdict

def extract_status_counts(json_file_path, output_csv_path):
    # JSONファイルを読み込む
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    issues = data.get('issues', [])
    ticket_histories = []

    for issue in issues:
        key = issue.get('key', '')
        fields = issue.get('fields', {})
        created = fields.get('created', '')
        created_dt = datetime.fromisoformat(created.replace('Z', '+00:00')) if created else None

        # チケットの初期ステータスを取得
        initial_status = fields.get('status', {}).get('name', '')
        status_changes = []
        if created_dt and initial_status:
            status_changes.append((created_dt, initial_status))

        # ステータス変更履歴を取得
        histories = fields.get('changelog', {}).get('histories', [])
        for history in histories:
            history_created = history.get('created', '')
            history_dt = datetime.fromisoformat(history_created.replace('Z', '+00:00')) if history_created else None
            for item in history.get('items', []):
                if item.get('field') == 'status':
                    to_status = item.get('toString', '')
                    if history_dt:
                        status_changes.append((history_dt, to_status))

        # 日付順にソート
        status_changes.sort(key=lambda x: x[0])
        ticket_histories.append({
            'key': key,
            'created': created_dt,
            'status_changes': status_changes
        })

    # 最も古い作成日を特定（UTC 0時開始）
    start_date = min(t['created'] for t in ticket_histories if t['created'])
    start_date = start_date.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    # 今日の日付（UTC 0時）
    end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    # 期間内の日ごとのステータス集計
    date = start_date
    date_status_counts = {}
    while date <= end_date:
        status_count = defaultdict(int)
        for ticket in ticket_histories:
            # チケットがまだ作成されていない日はスキップ
            if not ticket['created'] or ticket['created'] > date:
                continue

            # 最終ステータスを特定
            current_status = None
            for change_date, status in ticket['status_changes']:
                if change_date <= date:
                    current_status = status
                else:
                    break

            if current_status:
                status_count[current_status] += 1

        date_status_counts[date.strftime('%Y-%m-%d')] = dict(status_count)
        date += timedelta(days=1)

    # 全ステータス列を収集しソート
    all_statuses = sorted({s for counts in date_status_counts.values() for s in counts})

    # CSVに書き出し
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Date'] + all_statuses)
        for d, counts in date_status_counts.items():
            row = [d] + [counts.get(status, 0) for status in all_statuses]
            writer.writerow(row)

    print(f"ステータス統計を{output_csv_path}に出力しました。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract JIRA ticket status counts from JSON to CSV.')
    parser.add_argument('input_json', nargs='?', default='output.json', help='Input JSON file path (default: output.json)')
    parser.add_argument('output_csv', nargs='?', default='output.csv', help='Output CSV file path (default: output.csv)')
    args = parser.parse_args()

    extract_status_counts(args.input_json, args.output_csv)
