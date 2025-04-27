import json
import csv
import argparse
from datetime import datetime, timedelta, timezone
from collections import defaultdict

def parse_iso(dt_str):
    if not dt_str:
        return None
    try:
        # Handle UTC 'Z'
        if dt_str.endswith('Z'):
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        # Fallback to fromisoformat for offsets
        return datetime.fromisoformat(dt_str)
    except ValueError:
        # Remove timezone part if parsing fails
        base = dt_str[:19]
        return datetime.fromisoformat(base)

def extract_status_counts(json_path, csv_path, debug=False):
    # JSON読み込み
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    issues = data.get('issues', [])

    # チケットごとの時系列ステータス
    ticket_events = {}
    for issue in issues:
        key = issue.get('key')
        fields = issue.get('fields', {})
        created_dt = parse_iso(fields.get('created', ''))
        # 初期ステータス
        init_status = fields.get('status', {}).get('name')
        events = []
        if created_dt and init_status:
            events.append((created_dt.astimezone(timezone.utc), init_status))
        # 履歴追加
        for hist in fields.get('changelog', {}).get('histories', []):
            dt = parse_iso(hist.get('created', ''))
            if not dt:
                continue
            dt = dt.astimezone(timezone.utc)
            for item in hist.get('items', []):
                if item.get('field') == 'status':
                    events.append((dt, item.get('toString')))
        # ソート
        events.sort(key=lambda x: x[0])
        ticket_events[key] = events
        if debug:
            print(f"[DEBUG] Ticket {key} events:")
            for dt, st in events:
                print(f"    {dt.isoformat()} -> {st}")

    # 集計期間
    all_dt = [ev[0] for evs in ticket_events.values() for ev in evs]
    if not all_dt:
        print("ステータスイベントがありません。")
        return
    start = min(all_dt).replace(hour=0, minute=0, second=0, microsecond=0)
    end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    # 日毎カウント
    date = start
    date_status = {}
    while date <= end:
        counts = defaultdict(int)
        if debug:
            print(f"[DEBUG] Snapshot for {date.date()}")
        for key, events in ticket_events.items():
            current = None
            for dt, st in events:
                if dt <= date:
                    current = st
                else:
                    break
            if debug:
                print(f"    Ticket {key} status at {date.isoformat()}: {current}")
            if current:
                counts[current] += 1
        date_status[date.strftime('%Y-%m-%d')] = counts
        date += timedelta(days=1)

    # ヘッダー取得
    statuses = sorted({s for cnts in date_status.values() for s in cnts})

    # CSV出力
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['Date'] + statuses)
        for d, cnts in date_status.items():
            w.writerow([d] + [cnts.get(s, 0) for s in statuses])

    print(f"ステータス統計を {csv_path} に出力しました。")

if __name__ == '__main__':
    p = argparse.ArgumentParser(description='JIRA JSONから日毎ステータス集計CSVを生成')
    p.add_argument('input_json', nargs='?', default='output.json', help='入力JSONファイル (default: output.json)')
    p.add_argument('output_csv', nargs='?', default='output.csv', help='出力CSVファイル (default: output.csv)')
    p.add_argument('--debug', action='store_true', help='デバッグ出力を有効にする')
    args = p.parse_args()
    extract_status_counts(args.input_json, args.output_csv, debug=args.debug)
