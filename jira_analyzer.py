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

    # チケットごとに作成日とステータス履歴を取得
    ticket_histories = []
    for issue in issues:
        key = issue.get('key', '')
        fields = issue.get('fields', {})
        created = fields.get('created', '')
        created_dt = datetime.fromisoformat(created.replace('Z', '+00:00')) if created else None
        histories = fields.get('changelog', {}).get('histories', [])

        status_changes = []
        for history in histories:
            history_created = history.get('created', '')
            history_dt = datetime.fromisoformat(history_created.replace('Z', '+00:00')) if history_created else None
            for item in history.get('items', []):
                if item.get('field') == 'status':
                    to_status = item.get('toString', '')
                    status_changes.append((history_dt, to_status))

        status_changes.sort()
        ticket_histories.append({
            'key': key,
            'created': created_dt,
            'status_changes': status_changes
        })

    # 最も古い作成日を特定
    start_date = min(ticket['created'] for ticket in ticket_histories if ticket['created'])
    start_date = start_date.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    # 今日の日付を取得
    end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    # 期間内の日付ごとにステータス集計
    date = start_date
    date_status_counts = {}
    while date <= end_date:
        status_count = defaultdict(int)
        for ticket in ticket_histories:
            current_status = None
            if ticket['created'] and ticket['created'] > date:
                continue  # チケットがまだ作成されていない

            for change_date, status in ticket['status_changes']:
                if change_date and change_date <= date:
                    current_status = status
                else:
                    break

            if current_status:
                status_count[current_status] += 1
        date_status_counts[date.strftime('%Y-%m-%d')] = dict(status_count)
        date += timedelta(days=1)

    # CSV出力のためのステータス一覧を収集
    all_statuses = set()
    for counts in date_status_counts.values():
        all_statuses.update(counts.keys())
    all_statuses = sorted(all_statuses)

    # CSVに書き出し
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Date'] + all_statuses)
        for date, counts in date_status_counts.items():
            row = [date] + [counts.get(status, 0) for status in all_statuses]
            writer.writerow(row)

    print(f"ステータス統計を{output_csv_path}に出力しました。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract JIRA ticket status counts from JSON to CSV.')
    parser.add_argument('input_json', nargs='?', default='output.json', help='Input JSON file path (default: output.json)')
    parser.add_argument('output_csv', nargs='?', default='output.csv', help='Output CSV file path (default: output.csv)')
    args = parser.parse_args()

    extract_status_counts(args.input_json, args.output_csv)
