import json
import csv
import argparse
from datetime import datetime, timedelta, timezone
from collections import defaultdict

def parse_iso(dt_str):
    if not dt_str:
        return None
    try:
        if dt_str.endswith('Z'):
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        return datetime.fromisoformat(dt_str)
    except ValueError:
        base = dt_str.split('.')[0]
        return datetime.fromisoformat(base)

def extract_field_counts(json_path, field_id, debug=False):
    # 出力ファイル名
    csv_path = f"stat_{field_id}.csv"

    # JSON読み込み
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    issues = data.get('issues', [])

    # チケットごとにイベントリストを構築
    ticket_events = {}
    for issue in issues:
        key = issue.get('key')
        fields = issue.get('fields', {})
        created_dt = parse_iso(fields.get('created', ''))

        # 初期値
        if field_id == 'status':
            init_val = fields.get('status', {}).get('name')
        else:
            raw = fields.get(field_id)
            init_val = None
            if isinstance(raw, dict):
                init_val = raw.get('value') or raw.get('name')
            elif raw is not None:
                init_val = raw
        events = []
        if created_dt and init_val is not None:
            events.append((created_dt.astimezone(timezone.utc), init_val))

        # changelogからの変更イベント
        for hist in fields.get('changelog', {}).get('histories', []):
            dt = parse_iso(hist.get('created', ''))
            if not dt:
                continue
            dt = dt.astimezone(timezone.utc)
            for item in hist.get('items', []):
                if item.get('field') == field_id:
                    events.append((dt, item.get('toString')))

        # ソート
        events.sort(key=lambda x: x[0])
        ticket_events[key] = events

    # チケットごとに日付→ステータスマップを作成
    ticket_date_map = {}
    for key, events in ticket_events.items():
        date_map = {}
        for dt, val in events:
            date_map[dt.date()] = val
        ticket_date_map[key] = date_map

    if debug:
        print("[DEBUG] ticket_date_map:")
        for key, dm in ticket_date_map.items():
            print(f"  {key}: {dm}")

    # 集計期間
    all_dates = set()
    for dm in ticket_date_map.values():
        all_dates.update(dm.keys())
    if not all_dates:
        print("イベントが見つかりませんでした。JSONに changelog を含めて取得してください。")
        return
    start = min(all_dates)
    end = datetime.now(timezone.utc).date()

    # 各チケットの前日のステータス保持用
    last_status = {key: None for key in ticket_date_map}

    # 日毎集計
    date_counts = {}
    current = start
    while current <= end:
        counts = defaultdict(int)
        if debug:
            print(f"[DEBUG] Snapshot for {current}")
        for key, dm in ticket_date_map.items():
            # その日のイベントがあれば更新
            if current in dm:
                last_status[key] = dm[current]
            status = last_status[key]
            if debug:
                print(f"  {key} at {current}: {status}")
            if status is not None:
                counts[status] += 1
        date_counts[current.strftime('%Y-%m-%d')] = counts
        current += timedelta(days=1)

    # ヘッダー
    all_vals = sorted({v for cnts in date_counts.values() for v in cnts})

    # CSV出力
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Date'] + all_vals)
        for d, cnts in date_counts.items():
            writer.writerow([d] + [cnts.get(v, 0) for v in all_vals])

    print(f"統計結果を {csv_path} に出力しました。")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='JIRA JSONから日毎カスタムフィールド集計CSVを生成')
    parser.add_argument('input_json', nargs='?', default='output.json', help='入力JSONファイル (default: output.json)')
    parser.add_argument('field_id', nargs='?', default='status', help='統計を取るフィールドID (例: status or customfield_10010)')
    parser.add_argument('--debug', action='store_true', help='デバッグログ表示')
    args = parser.parse_args()
    extract_field_counts(args.input_json, args.field_id, debug=args.debug)
