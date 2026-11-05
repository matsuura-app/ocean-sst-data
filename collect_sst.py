import urllib.request
import urllib.parse
import urllib.error
import json
import csv
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

CSV_FILE = "data/japan_sst.csv"
BATCH_SIZE = 20
WAIT_SECONDS = 10
JST = ZoneInfo("Asia/Tokyo")

points = [
    (lat, lon)
    for lat in range(24, 47)
    for lon in range(122, 155)
]

FIELDS = [
    "data_time",
    "requested_lat",
    "requested_lon",
    "latitude",
    "longitude",
    "temperature",
    "fetched_at"
]

os.makedirs("data", exist_ok=True)

now = datetime.now(JST)
today = now.date()
yesterday = today - timedelta(days=1)
cutoff_date = today - timedelta(days=4)

fetched_at = now.isoformat(timespec="seconds")

# 既存CSVから直近5日分を引き継ぐ
records = {}

if os.path.exists(CSV_FILE):
    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                dt = datetime.fromisoformat(row["data_time"])

                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=JST)
                else:
                    dt = dt.astimezone(JST)

                if cutoff_date <= dt.date() <= today:
                    key = (
                        row["data_time"],
                        row["latitude"],
                        row["longitude"]
                    )
                    records[key] = row

            except (ValueError, KeyError):
                continue

# 既存データの日付を確認
existing_dates = set()

for row in records.values():
    try:
        dt = datetime.fromisoformat(row["data_time"])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=JST)
        else:
            dt = dt.astimezone(JST)
        existing_dates.add(dt.date())
    except ValueError:
        continue

# 5日分が揃っていない場合は初回補完
if not existing_dates or min(existing_dates) > cutoff_date:
    fetch_start_date = cutoff_date
    print("初回補完モード: 最新5日分を取得")
else:
    fetch_start_date = yesterday
    print("通常更新モード: 前日・当日を取得")

print("取得地点:", len(points))
print("既存データ:", len(records))
print("取得期間:", fetch_start_date, "〜", today)
print("保存対象:", cutoff_date, "〜", today)
print("取得開始:", fetched_at)

success_batches = 0
failed_batches = 0
new_records = 0
updated_records = 0

# 前日と当日の2日分だけ取得
for start in range(0, len(points), BATCH_SIZE):
    batch = points[start:start + BATCH_SIZE]

    params = {
        "latitude": ",".join(str(p[0]) for p in batch),
        "longitude": ",".join(str(p[1]) for p in batch),
        "hourly": "sea_surface_temperature",
        "start_date": fetch_start_date.isoformat(),
        "end_date": today.isoformat(),
        "forecast_days": 0,
        "timezone": "Asia/Tokyo",
        "cell_selection": "sea"
    }

    url = (
        "https://marine-api.open-meteo.com/v1/marine?"
        + urllib.parse.urlencode(params)
    )

    data = None

    for attempt in range(8):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "JapanSSTDataCollector/1.0"
                }
            )

            with urllib.request.urlopen(req, timeout=120) as response:
                data = json.load(response)

            break

        except urllib.error.HTTPError as e:
            if e.code == 429 or e.code >= 500:
                wait = min(60 * (2 ** attempt), 600)
                print("HTTPエラー:", e.code, "待機:", wait, "秒")
                time.sleep(wait)
            else:
                print("取得エラー:", e)
                break

        except Exception as e:
            wait = min(30 * (2 ** attempt), 600)
            print("通信エラー:", e, "待機:", wait, "秒")
            time.sleep(wait)

    if data is None:
        failed_batches += 1
        print("失敗:", start + 1, "〜", start + len(batch), "地点")
        continue

    if isinstance(data, dict):
        data = [data]

    for i, requested in enumerate(batch):
        if i >= len(data):
            continue

        item = data[i]
        hourly = item.get("hourly", {})

        times = hourly.get("time", [])
        temperatures = hourly.get("sea_surface_temperature", [])

        latitude = item.get("latitude")
        longitude = item.get("longitude")

        if latitude is None or longitude is None:
            continue

        for data_time, temperature in zip(times, temperatures):
            if temperature is None:
                continue

            try:
                dt = datetime.fromisoformat(data_time)

                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=JST)
                else:
                    dt = dt.astimezone(JST)

                if dt.date() < fetch_start_date or dt.date() > today:
                    continue

                if dt > now:
                    continue

            except ValueError:
                continue

            key = (
                data_time,
                str(latitude),
                str(longitude)
            )

            row = {
                "data_time": data_time,
                "requested_lat": requested[0],
                "requested_lon": requested[1],
                "latitude": latitude,
                "longitude": longitude,
                "temperature": temperature,
                "fetched_at": fetched_at
            }

            if key not in records:
                records[key] = row
                new_records += 1

            elif str(records[key]["temperature"]) != str(temperature):
                records[key] = row
                updated_records += 1

    success_batches += 1

    print(
        "進捗:",
        min(start + len(batch), len(points)),
        "/",
        len(points),
        "地点",
        "保存データ:",
        len(records)
    )

    if start + BATCH_SIZE < len(points):
        time.sleep(WAIT_SECONDS)

# 1バッチでも失敗した場合は既存CSVを変更しない
if failed_batches > 0:
    print("--------------------------------")
    print("❌ 取得失敗のためCSVは更新しません")
    print("失敗バッチ:", failed_batches)
    raise SystemExit(1)

# 日本時間で直近5日分に整理
final_records = {}

for key, row in records.items():
    try:
        dt = datetime.fromisoformat(row["data_time"])

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=JST)
        else:
            dt = dt.astimezone(JST)

        if cutoff_date <= dt.date() <= today and dt <= now:
            final_records[key] = row

    except (ValueError, KeyError):
        continue

# 一時ファイルに保存してから置き換える
temp_file = CSV_FILE + ".tmp"

with open(temp_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(
        sorted(
            final_records.values(),
            key=lambda r: (
                r["data_time"],
                float(r["latitude"]),
                float(r["longitude"])
            )
        )
    )

os.replace(temp_file, CSV_FILE)

print("--------------------------------")
print("✅ 処理終了")
print("成功バッチ:", success_batches)
print("新規データ:", new_records)
print("水温更新:", updated_records)
print("保存データ数:", len(final_records))
print("保存期間:", cutoff_date, "〜", today)
print("CSV保存先:", CSV_FILE)
print("取得日時:", fetched_at)
