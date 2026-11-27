"""
Checks the Bronze layer for duplicate ingestion across runs.

Overlap-window re-reads are expected, so duplicates are normal in Bronze.
Silver MERGE is what removes them. This script confirms that the duplicates
are exact copies (same payload hash), not silently different data.
"""

import glob
import gzip
import json
from collections import Counter


# Reading data from zip files and return them as rows
def read_rows(pattern):
    rows = []
    for path in glob.glob(pattern, recursive=True):
        with gzip.open(path, "rt", encoding='utf-8') as f:
            rows.extend(json.loads(line) for line in f)
    return rows

# Fetching rows from storage for models and files
models = read_rows("data/bronze/hf_models/**/*.jsonl.gz")
files = read_rows("data/bronze/hf_repo_files/**/*.jsonl.gz")

# Display total model and file rows stored in bronze in all runs
print("model rows:",len(models))
print("file rows:",len(files))

# Total models stored in each run
by_run = Counter(result["_run_id"] for result in models)
# print(by_run)
print("model rows per run:", dict(by_run))

# Total stale processing of models across all runs
model_dupes = Counter(result["_payload_sha256"] for result in models)
# print(model_dupes)
print("identical model payloads:", sum(c-1 for c in model_dupes.values() if c > 1))

# Total duplicate files of README.md or config.json generated across all runs
file_keys = Counter((result["repo_id"], result["filename"], result["_payload_sha256"]) for result in files)
# print(file_keys)
print("identical file payloads", sum(c-1 for c in file_keys.values() if c > 1))

# Total models processed more than once across all runs
model_ids = Counter(json.loads(result["_payload"])["id"] for result in models)
multi = {k: v for k, v in model_ids.items() if v > 1}
print("models appearing in more than one row:", len(multi))

# Checking if models get two or more hashes (may occur due to change in downloads, downloadsAllTime, likes, lastModified, ...)
hashes_per_model = {}
for row in models:
    model_id = json.loads(row["_payload"])["id"]
    hashes_per_model.setdefault(model_id, set()).add(row["_payload_sha256"])
print("Models with more than one distinct payload:", sum(1 for hs in hashes_per_model.values() if len(hs) > 1))
