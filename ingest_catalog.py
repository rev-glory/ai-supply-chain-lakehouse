import requests
import time
import os
import json
import hashlib
import gzip
from datetime import datetime, timedelta, timezone
import random
from uuid import uuid4
from collections import Counter

MAX_REPOS = 10
OVERLAP_MINUTES = 15
MAX_RETRIES = 5
MAX_PAGES = 3

BASE_URL = "https://huggingface.co/api/models"
HUB_ROOT = "https://huggingface.co"

FIRST_PAGE_PARAMS = {"sort": "lastModified", "direction": -1, "limit": 20}

RUN_STARTED_AT = datetime.now(timezone.utc)
PARTITION_DATE = RUN_STARTED_AT.strftime("%Y-%m-%d")
RUN_ID = RUN_STARTED_AT.strftime("%Y%m%dT%H%M%SZ") + uuid4().hex[:8]

WATERMARK_PATH = "data/state/watermark.json"
REPO_FILENAMES = ["README.md" , "config.json"]
BRONZE_MODELS_PATH = (
    f"data/bronze/hf_models/ingestion_date={PARTITION_DATE}/run_id={RUN_ID}/part-0001.jsonl.gz"
)
BRONZE_REPO_FILES_PATH = (
    f"data/bronze/hf_repo_files/ingestion_date={PARTITION_DATE}/run_id={RUN_ID}/part-0001.jsonl.gz"
)

# Write models and files 
def write_jsonl_gz(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as file:
        for row in rows : 
            file.write(json.dumps(row) + "\n")

# Check whether next page exist
def next_link(response):
    nxt = response.links.get("next")
    return nxt["url"] if nxt else None

# Loads the last Run watermark
def load_watermark():
    if not os.path.exists(WATERMARK_PATH):
        return None
    with open(WATERMARK_PATH, "r") as f:
        raw = json.load(f)["last_modified"]
    return datetime.fromisoformat(raw)

# Saves the current run latest watermark for next run
def save_watermark(value : datetime):
    os.makedirs(os.path.dirname(WATERMARK_PATH), exist_ok=True)
    tmp = WATERMARK_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump({'last_modified' : value.isoformat()}, f)
    os.replace(tmp, WATERMARK_PATH)

# Helper function for time
def parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

# Fetches the README.md and config.json content for model
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
            http_status = response.status_code
            result["http_code"] = http_status 
    
            if http_status == 200:
                result['status'] = 'ok'
                result["content"] = response.content
                return result
            if http_status in (401 , 403):
                result['status'] = 'gated'
                return result
            if http_status == 404:
                result['status'] = 'not_found'
                return result
            if http_status !=429 and http_status < 500:
                result['status'] = 'unavailable'
                return result
            retry_after = response.headers.get("Retry-After")

        if retry_after and retry_after.isdigit():
            sleep_seconds = int(retry_after)
        else:
            sleep_seconds = min(2 ** attempt, 300)
        time.sleep(sleep_seconds * random.uniform(0.5,1.0))
            
    result["status"] = "retry_exhausted"
    return result
    

# Loading last run watermark
previous_watermark = load_watermark()

# Creating a session for fetching data
session = requests.Session()

# Fetching new models
new_models = []
new_watermark = None
overlap_cutoff = previous_watermark - timedelta(minutes=OVERLAP_MINUTES) if previous_watermark else None

page_url = BASE_URL
params = FIRST_PAGE_PARAMS

for page in range(1,MAX_PAGES+1):

    response = session.get(page_url, params=params, timeout=30)

    if response.status_code == 200:
        page_records = response.json()
    else:
        print(f"Failed to fetch page number {page}: {response.status_code}")
        raise SystemExit

    reached_cutoff = False

    for model in page_records:
        ts = parse_ts(model['lastModified'])
        if overlap_cutoff and ts < overlap_cutoff:
            reached_cutoff = True
            continue
        new_models.append(model)
        if new_watermark is None or ts > new_watermark:
            new_watermark = ts

    print(f"page {page}: {len(page_records)} records, {len(new_models)} new_models so far")
    if reached_cutoff:
        break

    page_url = next_link(response=response)
    params = None
    if page_url is None:
        break

# Fetching files
fetch_results = []
for model in new_models[:MAX_REPOS]:
    for filename in REPO_FILENAMES:
        fetch_results.append(fetch_file(session=session, repo_id=model["id"], filename=filename))
        time.sleep(0.2)

# Printing Fetch Results
for result in fetch_results:
    print(f"{result['repo_id']:45} {result['filename']:12} {result['status']:16} {result['http_code']}")

# Summarizing the status counts
print()
print("Summary: ", dict(Counter(result["status"] for result in fetch_results)))

# Creating model records for bronze storage
model_rows = []
for model in new_models:
    payload = json.dumps(model)
    model_rows.append({
        '_run_id' : RUN_ID,
        '_ingested_at_utc' : RUN_STARTED_AT.isoformat(),
        '_source_url' : f"{BASE_URL}/{model['id']}",
        '_source' : 'hf_api',
        '_payload' : payload,
        '_payload_sha256' : hashlib.sha256(payload.encode('utf-8')).hexdigest()
    })

# Creating file records for bronze storage
repo_file_rows = []
for fetch_result in fetch_results : 
    content = fetch_result['content']
    repo_file_rows.append({
            '_run_id' : RUN_ID,
            '_ingested_at_utc' : RUN_STARTED_AT.isoformat(),
            '_source_url' : f'{HUB_ROOT}/{fetch_result["repo_id"]}/resolve/main/{fetch_result["filename"]}',
            '_source' : 'hf_raw',
            'repo_id' : fetch_result['repo_id'],
            'filename' : fetch_result['filename'],
            'status' : fetch_result['status'],
            'http_code' : fetch_result['http_code'],
            '_payload' : content.decode('utf-8', errors='replace') if content is not None else None,
            '_payload_sha256' : hashlib.sha256(content).hexdigest() if content is not None else None
    })

# Writing files to Bronze
write_jsonl_gz(BRONZE_MODELS_PATH, model_rows)
write_jsonl_gz(BRONZE_REPO_FILES_PATH, repo_file_rows)

# Saving the latest watermark and exit
if new_watermark is not None:
    save_watermark(new_watermark)