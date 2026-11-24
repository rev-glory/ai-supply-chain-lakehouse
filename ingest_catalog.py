import requests
import time
import os
import json
from datetime import datetime, timedelta
import random
from collections import Counter


BASE_URL = "https://huggingface.co/api/models"
HUB_ROOT = "https://huggingface.co"
PARAMS = {"sort": "lastModified", "direction": -1, "limit": 20}
FILES = ["README.md" , "config.json"]
MAX_REPOS = 10
WATERMARK_PATH = "data/state/watermark.json"
OVERLAP_MINUTES = 15
MAX_RETRIES = 5
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

def fetch_file(session, repo_id, filename):
    file_url = f'{HUB_ROOT}/{repo_id}/resolve/main/{filename}'
    result = {"repo_id": repo_id, "filename": filename,
              "status": None, "http_code": None, "content": None}

    for attempt in range(1, MAX_RETRIES+1):
        retry_after = None
        try : 
            response = session.get(file_url, timeout=30)
        except requests.RequestException:
            response = None

        if response is not None:
            code = response.status_code
            result["http_code"] = code 
    
            if code == 200:
                result['status'] = 'ok'
                result["content"] = response.content
                return result
            if code in (401 , 403):
                result['status'] = 'gated'
                return result
            if code == 404:
                result['status'] = 'not_found'
                return result
            if code !=429 and code < 500:
                result['status'] = 'unavailable'
                return result
            retry_after = response.headers.get("Retry-After")

        if retry_after and retry_after.isdigit():
            wait = int(retry_after)
        else:
            wait = min(2 ** attempt, 300)
        time.sleep(wait * random.uniform(0.5,1.0))
            
    result["status"] = "retry_exhausted"
    return result
    

watermark = load_watermark()
cutoff = watermark - timedelta(minutes=OVERLAP_MINUTES) if watermark else None
kept = []
max_seen = None

session = requests.Session()
all_records = []

file_results = []

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

    print(f"page {page}: {len(data)} records, {len(kept)} kept so far")
    if stop_after_page:
        break

    url = next_link(response=response)
    params = None
    if url is None:
        break

for item in kept[:MAX_REPOS]:
    for filename in FILES:
        file_results.append(fetch_file(session=session, repo_id=item["id"], filename=filename))
        time.sleep(0.2)

for r in file_results:
    print(f"{r['repo_id']:45} {r['filename']:12} {r['status']:16} {r['http_code']}")

print()
print("Summary: ", dict(Counter(r["status"] for r in file_results)))

if max_seen is not None:
    save_watermark(max_seen)