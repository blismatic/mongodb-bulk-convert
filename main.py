import os
import threading
from concurrent.futures import ThreadPoolExecutor

import httpx2
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

REVERSE_API_URL = os.getenv("API_URL", "")
WORKERS = 10  # concurrent API calls; raise only if the API can handle it
BATCH = 1000

client = MongoClient(f"mongodb+srv://{os.getenv('USER', '')}:{os.getenv('PASSWORD', '')}@{os.getenv('CLUSTER', '')}/?retryWrites=true&w=majority")
db = client[os.getenv("DATABASE", "")]
source_collection = db[os.getenv("SOURCE_COLLECTION", "")]
destination_collection = db[os.getenv("DESTINATION_COLLECTION", "")]

_local = threading.local()


def http():
    if not hasattr(_local, "client"):
        _local.client = httpx2.Client(timeout=30)
    return _local.client


def transform(doc):
    last = doc["timeline"][0]
    for field in ("status", "apps", "responsible_app"):
        last.pop(field, None)

    response = http().post(REVERSE_API_URL, json=last["data"])
    if response.status_code != 200:
        raise RuntimeError(f"reverse failed for {doc['_id']}: {response.status_code} {response.text}")
    last["data"] = response.json()

    out = {k: doc[k] for k in ("_id", "catalog", "record_id") if k in doc}
    out["record"] = last
    return out


def process(doc):
    try:
        destination_collection.insert_one(transform(doc))
    except DuplicateKeyError:
        pass  # trigger already wrote this document, and its version is newer, so just skip


def main():
    # print("Hello from mongodb-bulk-convert!")
    done = {d["_id"] for d in destination_collection.find({}, {"_id": 1})}
    print(f"Skipping {len(done)} documents already in destination")

    cursor = source_collection.find({}, {"catalog": 1, "record_id": 1, "timeline": {"$slice": -1}}, batch_size=BATCH)

    processed = 0
    batch = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for doc in cursor:
            if doc["_id"] in done:
                continue
            batch.append(doc)
            if len(batch) == BATCH:
                list(pool.map(process, batch))  # surfaces the first failure after this batch completes
                processed += len(batch)
                print(f"Processed {processed} documents")
                batch = []
        if batch:
            list(pool.map(process, batch))
            processed += len(batch)

    print(f"Done! Proceesed {processed} documents in total.")


if __name__ == "__main__":
    main()
