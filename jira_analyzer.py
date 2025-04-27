import json
import csv
import argparse

def extract_key_summary_from_jira(json_file_path, output_csv_path):
    # JSONファイルを読み込む
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # チケット情報が 'issues' というキーのリストに入っている前提
    issues = data.get('issues', [])

    # CSVに書き込むためのデータを作成
    extracted_data = []
    for issue in issues:
        key = issue.get('key', '')
        summary = issue.get('fields', {}).get('summary', '')
        extracted_data.append((key, summary))

    # CSVファイルに書き出し
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Key', 'Summary'])  # ヘッダー
        writer.writerows(extracted_data)

    print(f"{len(extracted_data)}件のチケット情報を{output_csv_path}に出力しました。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract JIRA ticket keys and summaries from JSON to CSV.')
    parser.add_argument('input_json', help='Input JSON file path')
    parser.add_argument('output_csv', help='Output CSV file path')
    args = parser.parse_args()

    extract_key_summary_from_jira(args.input_json, args.output_csv)
