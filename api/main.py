# from fastapi import FastAPI, HTTPException
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.staticfiles import StaticFiles
# import json
# import subprocess
# import os
# import sys
# from dotenv import load_dotenv

# load_dotenv()

# app = FastAPI(title="SmartCart API")
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # FIX 1: All paths are now absolute, based on this file's location
# # Works regardless of where you launch uvicorn from
# API_DIR = os.path.dirname(os.path.abspath(__file__))
# ROOT_DIR = os.path.dirname(API_DIR)
# DATA_DIR = os.path.join(ROOT_DIR, "data")
# ANALYZED_DIR = os.path.join(DATA_DIR, "analyzed")
# FRONTEND_DIR = os.path.join(ROOT_DIR, "frontend")


# def run_fetch_and_analyze(query: str):
#     # Step 1: Fetch real prices from SerpAPI
#     subprocess.run(
#         [sys.executable, os.path.join(ROOT_DIR, "fetch_prices.py")] + query.split(),
#         cwd=ROOT_DIR,
#         # FIX 2: capture output so Spark logs don't flood your terminal
#         stdout=subprocess.PIPE,
#         stderr=subprocess.PIPE,
#     )
#     # Step 2: Run PySpark analysis
#     subprocess.run(
#         [sys.executable, os.path.join(ROOT_DIR, "pyspark_scripts", "analyze.py")] + query.split(),
#         cwd=ROOT_DIR,
#         stdout=subprocess.PIPE,
#         stderr=subprocess.PIPE,
#     )


# def load_analyzed(query: str):
#     safe = query.strip().replace(" ", "_").lower()
#     path = os.path.join(ANALYZED_DIR, f"{safe}.json")
#     if not os.path.exists(path):
#         return None
#     with open(path) as f:
#         return json.load(f)


# @app.get("/api/search")
# def search(q: str):
#     q = q.strip()
#     if not q or len(q) < 2:
#         raise HTTPException(status_code=400, detail="Query too short")
#     run_fetch_and_analyze(q)
#     data = load_analyzed(q)
#     if not data or not data.get("results"):
#         raise HTTPException(status_code=404, detail="No results found")
#     return data


# @app.get("/api/tracked")
# def get_tracked():
#     if not os.path.exists(ANALYZED_DIR):
#         return {"products": []}
#     products = []
#     for f in os.listdir(ANALYZED_DIR):
#         if f.endswith(".json") and f != "all.json":
#             query = f.replace(".json", "").replace("_", " ")
#             products.append(query)
#     return {"products": products}


# @app.get("/api/cached")
# def get_cached(q: str):
#     data = load_analyzed(q.strip())
#     if not data:
#         raise HTTPException(status_code=404, detail="Not cached")
#     return data


# # FIX 3: Mount static files LAST so /api/* routes are matched first
# app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")


from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import json
import subprocess
import os
import sys
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="SmartCart API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

API_DIR     = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR    = os.path.dirname(API_DIR)
DATA_DIR    = os.path.join(ROOT_DIR, "data")
ANALYZED_DIR = os.path.join(DATA_DIR, "analyzed")
FRONTEND_DIR = os.path.join(ROOT_DIR, "frontend")


def run_fetch_and_analyze(query: str):
    # FIX: redirect stdout+stderr to DEVNULL to prevent pipe-buffer deadlock.
    # Spark writes megabytes of logs; PIPE buffers fill up (~65KB) and the
    # process hangs forever waiting for the parent to drain the pipe.
    devnull = subprocess.DEVNULL

    subprocess.run(
        [sys.executable, os.path.join(ROOT_DIR, "fetch_prices.py")] + query.split(),
        cwd=ROOT_DIR,
        stdout=devnull,
        stderr=devnull,
    )
    subprocess.run(
        [sys.executable, os.path.join(ROOT_DIR, "pyspark_scripts", "analyze.py")] + query.split(),
        cwd=ROOT_DIR,
        stdout=devnull,
        stderr=devnull,
    )


def load_analyzed(query: str):
    safe = query.strip().replace(" ", "_").lower()
    path = os.path.join(ANALYZED_DIR, f"{safe}.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


@app.get("/api/search")
def search(q: str):
    q = q.strip()
    if not q or len(q) < 2:
        raise HTTPException(status_code=400, detail="Query too short")
    run_fetch_and_analyze(q)
    data = load_analyzed(q)
    if not data or not data.get("results"):
        raise HTTPException(status_code=404, detail="No results found")
    return data


@app.get("/api/tracked")
def get_tracked():
    if not os.path.exists(ANALYZED_DIR):
        return {"products": []}
    products = []
    for f in os.listdir(ANALYZED_DIR):
        if f.endswith(".json") and f != "all.json":
            query = f.replace(".json", "").replace("_", " ")
            products.append(query)
    return {"products": products}


@app.get("/api/cached")
def get_cached(q: str):
    data = load_analyzed(q.strip())
    if not data:
        raise HTTPException(status_code=404, detail="Not cached")
    return data


app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")