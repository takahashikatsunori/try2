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
    # 出力ファイル名を固定
    csv_path = f"stat_{field_id}.csv"

    # JSON読み込み
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    issues = data.get('issues', [])
    ticket_events = {}

    for issue in issues:
        key = issue.get('key')
        fields = issue.get('fields', {})
        created_dt = parse_iso(fields.get('created', ''))

        # 初期値取得
        if field_id == 'status':
            init_val = fields.get('status', {}).get('name')
        else:
            raw = fields.get(field_id)
            if isinstance(raw, dict) and raw is not None:
                init_val = raw.get('value') or raw.get('name')
            else:
                init_val = raw
        events = []
        if created_dt and init_val is not None:
            events.append((created_dt.astimezone(timezone.utc), init_val))

        # 履歴からfield_idの変更を抽出
        for hist in fields.get('changelog', {}).get('histories', []):
            dt = parse_iso(hist.get('created', ''))
            if not dt:
                continue
            dt = dt.astimezone(timezone.utc)
            for item in hist.get('items', []):
                if item.get('field') == field_id:
                    val = item.get('toString')
                    events.append((dt, val))

        # ソートして保存
        events.sort(key=lambda x: x[0])
        ticket_events[key] = events

    # デバッグ: 各チケットのイベント数
    if debug:
        print(f"[DEBUG] Field '{field_id}' events per ticket:")
        for key, evs in ticket_events.items():
            print(f"  {key}: {len(evs)} events -> {[v for _,v in evs]}")

    # 集計期間算出
    all_dt = [ev[0] for evs in ticket_events.values() for ev in evs]
    if not all_dt:
        print("指定フィールドのイベントが見つかりませんでした。JSONに changelog が含まれているか確認してください。")
        return
    start = min(all_dt).replace(hour=0, minute=0, second=0, microsecond=0)
    end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    # 日毎集計
    date = start
    date_counts = {}
    while date <= end:
        counts = defaultdict(int)
        if debug:
            print(f"[DEBUG] Snapshot {date.date()}")
        for key, evs in ticket_events.items():
            current = None
            for dt, val in evs:
                if dt <= date:
                    current = val
                else:
                    break
            if debug:
                print(f"  {key} at {date.date()}: {current}")
            if current is not None:
                counts[current] += 1
        date_counts[date.strftime('%Y-%m-%d')] = counts
        date += timedelta(days=1)

    # ヘッダー収集
    all_vals = sorted({v for cnts in date_counts.values() for v in cnts})

    # CSV出力
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Date'] + all_vals)
        for date_str, cnts in date_counts.items():
            writer.writerow([date_str] + [cnts.get(v, 0) for v in all_vals])

    print(f"統計結果を {csv_path} に出力しました。")

if __name__ == '__main__':
    p = argparse.ArgumentParser(description='JIRA JSONから日毎カスタムフィールド集計CSVを生成')
    p.add_argument('input_json', nargs='?', default='output.json', help='入力JSONファイル (default: output.json)')
    p.add_argument('field_id', nargs='?', default='status', help='統計を取るフィールドID (例: status or customfield_10010)')
    p.add_argument('--debug', action='store_true', help='デバッグログ表示')
    args = p.parse_args()
    extract_field_counts(args.input_json, args.field_id, debug=args.debug)
