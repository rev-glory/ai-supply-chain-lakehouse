import requests
import time
import os
import json
from datetime import datetime, timedelta


BASE_URL = "https://huggingface.co/api/models"
PARAMS = {"sort": "lastModified", "direction": -1, "limit": 20}
WATERMARK_PATH = "data/state/watermark.json"
OVERLAP_MINUTES = 15

DELAY = 1
MAX_PAGES = 3

def next_link(response):
    nxt = response.links.get("next")
    return nxt["url"] if nxt else None

def load_watermark():
    if not os.path.exists(WATERMARK_PATH):
        return None
    with open(WATERMARK_PATH, "r") as f:
        raw = json.load(f)["last_modified"]
    return datetime.fromisoformat(raw)

def save_watermark(value : datetime):
    os.makedirs(os.path.dirname(WATERMARK_PATH), exist_ok=True)
    tmp = WATERMARK_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump({'last_modified' : value.isoformat()}, f)
    os.replace(tmp, WATERMARK_PATH)

def parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


watermark = load_watermark()
cutoff = watermark - timedelta(minutes=OVERLAP_MINUTES) if watermark else None
kept = []
max_seen = None

session = requests.Session()
all_records = []

url = BASE_URL
params = PARAMS

for page in range(1,MAX_PAGES+1):

    response = session.get(url, params=params, timeout=30)

    if response.status_code == 200:
        data = response.json()
    else:
        print(f"Failed to fetch page number {page}: {response.status_code}")
        raise SystemExit

    stop_after_page = False

    for item in data:
        ts = parse_ts(item['lastModified'])
        if cutoff and ts < cutoff:
            stop_after_page = True
            continue
        kept.append(item)
        if max_seen is None or ts > max_seen:
            max_seen = ts

    all_records.extend(data)
    print(f"page {page}: {len(data)} records")

    url = next_link(response=response)
    params = None
    if url is None:
        break

    print()

print(f"total records = {len(all_records)}")
print(f"last id = {all_records[-1]['id']}" if all_records else "no records")


# print(next_link(response=response))