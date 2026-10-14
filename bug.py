import os
from concurrent.futures import ThreadPoolExecutor

import httpx2

"""
The reverse translator seems to fail under concurrent requests.

Each request seems to write its input to `C:\\Input\\Input_<yyyyMMdd_HHmmss>.json`.
Requests within the same second resolve to the same file name and collide, which results in a 500 status code, file-lock error.
The following script reproduces this buggy behavior.

N number of sequential requests are made and succeed, while N number of concurrent requests are made and fail intermittently.

Suggested fix: 
    no temporary file at all, just pass the input directly to the translator so that it does the work in memory, not on disk...
    or have a unique file name per request (e.g., a GUID)
    
Also to check:
    if there is any gap between writing the input file and then reading it back, concurrent requests may silently
    read each other's input and return the wrong transformed data instead of erroring...
"""


URL = os.getenv("API_URL", "")
N = 25


def call(_):
    payload = {"id$": "6acfbc99999999cd999a9c99", "loanTerms": {"id$": "6acfbc99999999cd999a9c98", "loanTerm": 120, "loanAmount1": 249500}}
    response = httpx2.post(URL, json=payload, timeout=30)
    return response.status_code, response.text


def report(label, results):
    failures = [(status, body) for status, body in results if status != 200]
    print(f"{label}: {N - len(failures)}/{N} succeeded, {len(failures)} failed")
    for status, body in failures:
        print(f"  {status} {body}")


report("Sequential", [call(_) for _ in range(N)])

with ThreadPoolExecutor(max_workers=N) as pool:
    report("Concurrent", list(pool.map(call, range(N))))
