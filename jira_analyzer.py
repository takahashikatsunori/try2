import json
import csv
import argparse
from datetime import datetime, timedelta, timezone
from collections import defaultdict

def parse_iso(dt_str):
    # ISOフォーマットをパースしてUTC対応のdatetimeを返す
    if not dt_str:
        return None
    if dt_str.endswith('Z'):
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    return datetime.fromisoformat(dt_str)

def extract_status_counts(json_file_path, output_csv_path):
    # JSONファイル読み込み
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    issues = data.get('issues', [])

    # チケットごとのステータス変更イベントを収集
    ticket_events = {}
    for issue in issues:
        key = issue.get('key', '')
        fields = issue.get('fields', {})
        created_dt = parse_iso(fields.get('created', ''))
        histories = fields.get('changelog', {}).get('histories', [])

        events = []
        # 初期ステータス: changelogにfromStringなしの最初のステータス変更があればそれを初期とする
        initial_found = False
        for hist in histories:
            for item in hist.get('items', []):
                if item.get('field') == 'status' and not item.get('fromString'):
                    dt = parse_iso(hist.get('created'))
                    events.append((dt, item.get('toString', '')))
                    initial_found = True
                    break
            if initial_found:
                break
        # changelogに初期がなければ、作成時点のステータスを使用
        if not initial_found and created_dt:
            init_status = fields.get('status', {}).get('name', '')
            events.append((created_dt, init_status))

        # 以降のステータス変更をすべて追加
        for hist in histories:
            dt = parse_iso(hist.get('created'))
            for item in hist.get('items', []):
                if item.get('field') == 'status':
                    events.append((dt, item.get('toString', '')))

        # イベントを時系列でソート
        events = [(dt.astimezone(timezone.utc), st) for dt, st in events if dt]
        events.sort(key=lambda x: x[0])
        ticket_events[key] = events

    # 集計期間の設定（最古のイベント日時を開始日、今日を終了日）
    all_dates = [events[0][0] for events in ticket_events.values() if events]
    if not all_dates:
        print("有効なステータスイベントが見つかりませんでした。")
        return
    start_date = min(all_dates).replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    # 日付ごとステータス数集計
    date = start_date
    date_status_counts = {}
    while date <= end_date:
        counts = defaultdict(int)
        for key, events in ticket_events.items():
            # 当日0時時点でのステータスを取得
            current = None
            for dt, status in events:
                if dt <= date:
                    current = status
                else:
                    break
            if current:
                counts[current] += 1
        date_str = date.strftime('%Y-%m-%d')
        date_status_counts[date_str] = counts
        date += timedelta(days=1)

    # CSVヘッダー用ステータスの総集合
    all_statuses = sorted({s for counts in date_status_counts.values() for s in counts})

    # CSV出力
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Date'] + all_statuses)
        for date_str, counts in date_status_counts.items():
            row = [date_str] + [counts.get(s, 0) for s in all_statuses]
            writer.writerow(row)

    print(f"ステータス統計を {output_csv_path} に出力しました。")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract JIRA ticket status counts from JSON to CSV.')
    parser.add_argument('input_json', nargs='?', default='output.json', help='Input JSON file path (default: output.json)')
    parser.add_argument('output_csv', nargs='?', default='output.csv', help='Output CSV file path (default: output.csv)')
    args = parser.parse_args()
    extract_status_counts(args.input_json, args.output_csv)
