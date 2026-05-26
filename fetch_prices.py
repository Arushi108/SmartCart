# import requests
# import json
# import os
# import csv
# from datetime import datetime
# from dotenv import load_dotenv

# load_dotenv()
# SERPAPI_KEY = os.getenv("SERPAPI_KEY")

# def fetch_real_prices(query):
#     print(f"Fetching real prices for: {query}")
#     url = "https://serpapi.com/search"
#     params = {
#         "engine": "google_shopping",
#         "q": query,
#         "api_key": SERPAPI_KEY,
#         "gl": "in",
#         "hl": "en",
#         "num": 20
#     }
#     response = requests.get(url, params=params)
#     data = response.json()

#     results = []
#     if "shopping_results" not in data:
#         print("No results found")
#         return []

#     for item in data["shopping_results"][:15]:
#         price_str = item.get("price", "0")
#         price = float(price_str.replace("₹", "").replace(",", "").replace(" ", "").split(".")[0]) if price_str else 0
#         if price == 0:
#             continue
#         results.append({
#             "product_name": item.get("title", query)[:60],
#             "search_query": query,
#             "platform": item.get("source", "Unknown"),
#             "price": price,
#             "rating": item.get("rating", 0),
#             "reviews": item.get("reviews", 0),
#             "thumbnail": item.get("thumbnail", ""),
#             "link": item.get("link", "#"),
#             "date": datetime.now().strftime("%Y-%m-%d"),
#             "timestamp": datetime.now().isoformat()
#         })

#     print(f"Found {len(results)} results")
#     return results

# def save_prices(query, results):

#     os.makedirs("data/raw", exist_ok=True)

#     safe_query = query.replace(
#         " ",
#         "_"
#     ).lower()

#     filepath = f"data/raw/{safe_query}.json"

#     # Save raw JSON history

#     existing=[]

#     if os.path.exists(filepath):

#         with open(filepath,"r") as f:
#             existing=json.load(f)

#     existing.extend(results)

#     with open(filepath,"w") as f:

#         json.dump(
#             existing,
#             f,
#             indent=2
#         )

#     # Master CSV for PySpark

#     os.makedirs("data",exist_ok=True)

#     csv_path="data/all_prices.csv"

#     file_exists=os.path.exists(csv_path)

#     if results:

#         with open(
#             csv_path,
#             "a",
#             newline="",
#             encoding="utf-8"
#         ) as f:

#             writer=csv.DictWriter(
#                 f,
#                 fieldnames=results[0].keys()
#             )

#             if not file_exists:

#                 writer.writeheader()

#             writer.writerows(results)

#     print(f"Saved: {filepath}")

#     return filepath

# if __name__ == "__main__":
#     import sys
#     query = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "iPhone 15"
#     results = fetch_real_prices(query)
#     save_prices(query, results)
#     print(json.dumps(results[:2], indent=2))
import requests
import json
import os
import csv
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
SERPAPI_KEY = os.getenv("SERPAPI_KEY")

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, "data")


def fetch_real_prices(query):
    print(f"Fetching real prices for: {query}")

    if not SERPAPI_KEY:
        print("ERROR: SERPAPI_KEY not set in .env file")
        return []

    url = "https://serpapi.com/search"
    params = {
        "engine": "google_shopping",
        "q": query,
        "api_key": SERPAPI_KEY,
        "gl": "in",
        "hl": "en",
        "num": 20,
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"SerpAPI error: {e}")
        return []

    if "shopping_results" not in data:
        print("No shopping results:", data.get("error", "unknown"))
        return []

    results = []
    for item in data["shopping_results"][:15]:
        price_str = item.get("price", "0") or "0"
        try:
            price = float(
                price_str.replace("₹", "").replace(",", "")
                .replace(" ", "").split(".")[0]
            )
        except ValueError:
            price = 0
        if price == 0:
            continue

        # FIX: SerpAPI google_shopping uses 'product_link' for merchant URL
        # 'link' is google's own redirect. Use product_link first, then link,
        # then build a google search URL as last resort so button always works.
        merchant_link = (
            item.get("product_link")
            or item.get("link")
            or ""
        )
        if not merchant_link or merchant_link == "#":
            # Build a Google Shopping search URL as reliable fallback
            import urllib.parse
            platform = item.get("source", "")
            search_term = f"{item.get('title', query)} {platform} buy"
            merchant_link = "https://www.google.com/search?tbm=shop&q=" + urllib.parse.quote(search_term)

        results.append({
            "product_name": str(item.get("title", query))[:60],
            "search_query": query,
            "platform": str(item.get("source", "Unknown")),
            "price": price,
            "rating": float(item.get("rating") or 0),
            "reviews": int(item.get("reviews") or 0),
            "thumbnail": str(item.get("thumbnail", "")),
            "link": merchant_link,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "timestamp": datetime.now().isoformat(),
        })

    print(f"Found {len(results)} results")
    return results


def save_prices(query, results):
    if not results:
        print("No results to save")
        return None

    raw_dir = os.path.join(DATA_DIR, "raw")
    os.makedirs(raw_dir, exist_ok=True)

    safe_query = query.strip().replace(" ", "_").lower()
    filepath = os.path.join(raw_dir, f"{safe_query}.json")

    existing = []
    if os.path.exists(filepath):
        with open(filepath) as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                existing = []

    combined = (existing + results)[-50:]
    with open(filepath, "w") as f:
        json.dump(combined, f, indent=2)

    _rebuild_master_csv()
    print(f"Saved to {filepath}")
    return filepath


def _rebuild_master_csv():
    raw_dir = os.path.join(DATA_DIR, "raw")
    csv_path = os.path.join(DATA_DIR, "all_prices.csv")
    all_rows = []
    fields = None

    for fname in os.listdir(raw_dir):
        if not fname.endswith(".json"):
            continue
        with open(os.path.join(raw_dir, fname)) as f:
            try:
                rows = json.load(f)
                if rows and not fields:
                    fields = list(rows[0].keys())
                all_rows.extend(rows)
            except (json.JSONDecodeError, IndexError):
                continue

    if not all_rows or not fields:
        return

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Rebuilt CSV: {len(all_rows)} rows")


if __name__ == "__main__":
    import sys
    query = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "iPhone 15"
    results = fetch_real_prices(query)
    if results:
        save_prices(query, results)
        print(json.dumps(results[:2], indent=2))