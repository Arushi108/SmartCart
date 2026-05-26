# import json
# import sys
# import os
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     min, max, avg, round,
#     col, count, expr,
#     rank, lit
# )
# from pyspark.sql.window import Window

# # -------------------------
# # Spark Setup
# # -------------------------

# spark = SparkSession.builder \
#     .appName("SmartCart Analyzer") \
#     .config("spark.driver.memory", "2g") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("ERROR")

# query = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else None

# csv_path = "data/all_prices.csv"

# if not os.path.exists(csv_path):
#     print("No CSV found")
#     spark.stop()
#     sys.exit()

# print("Loading CSV...")

# df = spark.read.csv(
#     csv_path,
#     header=True,
#     inferSchema=True
# )

# if query:
#     df = df.filter(
#         col("search_query") == query
#     )

# if df.count() == 0:
#     print("No records found")
#     spark.stop()
#     sys.exit()

# print(f"Records loaded: {df.count()}")

# # -------------------------
# # Analytics
# # -------------------------

# window_platform = Window.partitionBy(
#     "search_query",
#     "platform"
# )

# window_query = Window.partitionBy(
#     "search_query"
# )

# df = df.withColumn(
#     "min_price",
#     min("price").over(window_platform)
# ).withColumn(
#     "max_price",
#     max("price").over(window_platform)
# ).withColumn(
#     "avg_price",
#     round(avg("price").over(window_platform),0)
# ).withColumn(
#     "global_min",
#     min("price").over(window_query)
# ).withColumn(
#     "global_max",
#     max("price").over(window_query)
# )

# df = df.withColumn(
#     "deal_score",
#     round(
#         (
#             1-
#             (
#                 (col("price")-col("global_min"))
#                 /
#                 (
#                     col("global_max")
#                     -col("global_min")
#                     +lit(1)
#                 )
#             )
#         )*100,
#         1
#     )
# )

# df = df.withColumn(
#     "recommendation",
#     expr("""
#     CASE
#     WHEN deal_score>=80 THEN 'Buy Now!'
#     WHEN deal_score>=60 THEN 'Good Deal'
#     WHEN deal_score>=40 THEN 'Average Price'
#     ELSE 'Overpriced'
#     END
#     """)
# )

# df = df.withColumn(
#     "savings",
#     round(
#         col("global_max")
#         -col("price"),
#         0
#     )
# )

# # -------------------------
# # Best item from each platform
# # -------------------------

# rank_window=Window.partitionBy(
#     "search_query",
#     "platform"
# ).orderBy(
#     col("price")
# )

# best=df.withColumn(
#     "rank",
#     rank().over(rank_window)
# ).filter(
#     col("rank")==1
# )

# rows=best.orderBy(
#     col("deal_score").desc()
# ).collect()

# results=[]

# for r in rows:

#     results.append({

#         "product_name":
#         str(r["product_name"]),

#         "search_query":
#         str(r["search_query"]),

#         "platform":
#         str(r["platform"]),

#         "price":
#         float(r["price"]),

#         "avg_price":
#         float(r["avg_price"]),

#         "min_price":
#         float(r["min_price"]),

#         "max_price":
#         float(r["max_price"]),

#         "deal_score":
#         float(r["deal_score"]),

#         "recommendation":
#         str(r["recommendation"]),

#         "savings":
#         float(r["savings"]),

#         "rating":
#         float(r["rating"] or 0),

#         "reviews":
#         int(r["reviews"] or 0),

#         "thumbnail":
#         str(r["thumbnail"] or ""),

#         "link":
#         str(r["link"] or "#"),

#         # FIXED DATE ISSUE
#         "date":
#         str(r["date"])
#     })

# # -------------------------
# # Final JSON
# # -------------------------

# final={

#     "query":
#     query,

#     "total_listings":
#     len(results),

#     "results":
#     results
# }

# os.makedirs(
#     "data/analyzed",
#     exist_ok=True
# )

# safe=(query or "all").replace(
#     " ",
#     "_"
# ).lower()

# out_path=f"data/analyzed/{safe}.json"

# with open(
#     out_path,
#     "w"
# ) as f:

#     json.dump(
#         final,
#         f,
#         indent=2,
#         default=str
#     )
# spark.stop()

import json
import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    min, max, avg, round, col, count,
    expr, rank, lit, to_date
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, IntegerType
)

# FIX: Absolute paths - works regardless of cwd
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")
CSV_PATH = os.path.join(DATA_DIR, "all_prices.csv")
ANALYZED_DIR = os.path.join(DATA_DIR, "analyzed")

# ── Spark Setup ──────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("SmartCart Analyzer")
    .config("spark.driver.memory", "2g")
    # FIX: suppress noisy warnings
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

query = " ".join(sys.argv[1:]).strip() if len(sys.argv) > 1 else None

if not os.path.exists(CSV_PATH):
    print(f"No CSV found at {CSV_PATH}")
    spark.stop()
    sys.exit(1)

print(f"Loading CSV from {CSV_PATH}...")

# FIX: Explicit schema prevents Spark from inferring 'date' as DateType
# which caused the date formatting inconsistency
schema = StructType([
    StructField("product_name", StringType(), True),
    StructField("search_query", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("rating", FloatType(), True),
    StructField("reviews", IntegerType(), True),
    StructField("thumbnail", StringType(), True),
    StructField("link", StringType(), True),
    StructField("date", StringType(), True),   # keep as string
    StructField("timestamp", StringType(), True),
])

df = spark.read.csv(CSV_PATH, header=True, schema=schema)

# FIX: Drop rows with null price (bad CSV rows)
df = df.filter(col("price").isNotNull() & (col("price") > 0))

if query:
    df = df.filter(col("search_query") == query)

total = df.count()
if total == 0:
    print(f"No records found for query: {query}")
    spark.stop()
    sys.exit(1)

print(f"Records loaded: {total}")

# ── Analytics ────────────────────────────────────────────────────────────────
window_platform = Window.partitionBy("search_query", "platform")
window_query    = Window.partitionBy("search_query")

df = (
    df
    .withColumn("min_price",  min("price").over(window_platform))
    .withColumn("max_price",  max("price").over(window_platform))
    .withColumn("avg_price",  round(avg("price").over(window_platform), 0))
    .withColumn("global_min", min("price").over(window_query))
    .withColumn("global_max", max("price").over(window_query))
)

df = df.withColumn(
    "deal_score",
    round(
        (1 - (col("price") - col("global_min"))
             / (col("global_max") - col("global_min") + lit(1.0))
        ) * 100,
        1,
    ),
)

df = df.withColumn(
    "recommendation",
    expr("""
        CASE
            WHEN deal_score >= 80 THEN 'Buy Now!'
            WHEN deal_score >= 60 THEN 'Good Deal'
            WHEN deal_score >= 40 THEN 'Average Price'
            ELSE 'Overpriced'
        END
    """),
)

df = df.withColumn("savings", round(col("global_max") - col("price"), 0))

# ── Best item from each platform ─────────────────────────────────────────────
rank_window = Window.partitionBy("search_query", "platform").orderBy(col("price"))

best = (
    df
    .withColumn("rank", rank().over(rank_window))
    .filter(col("rank") == 1)
)

rows = best.orderBy(col("deal_score").desc()).collect()

results = []
for r in rows:
    results.append({
        "product_name":   str(r["product_name"] or ""),
        "search_query":   str(r["search_query"] or ""),
        "platform":       str(r["platform"] or ""),
        "price":          float(r["price"] or 0),
        "avg_price":      float(r["avg_price"] or 0),
        "min_price":      float(r["min_price"] or 0),
        "max_price":      float(r["max_price"] or 0),
        "deal_score":     float(r["deal_score"] or 0),
        "recommendation": str(r["recommendation"] or ""),
        "savings":        float(r["savings"] or 0),
        "rating":         float(r["rating"] or 0),
        "reviews":        int(r["reviews"] or 0),
        "thumbnail":      str(r["thumbnail"] or ""),
        "link":           str(r["link"] or "#"),
        "date":           str(r["date"] or ""),   # always a plain string now
    })

# ── Save output ───────────────────────────────────────────────────────────────
final = {
    "query": query,
    "total_listings": len(results),
    "results": results,
}

os.makedirs(ANALYZED_DIR, exist_ok=True)
safe = (query or "all").strip().replace(" ", "_").lower()
out_path = os.path.join(ANALYZED_DIR, f"{safe}.json")

with open(out_path, "w") as f:
    json.dump(final, f, indent=2, default=str)

print(f"Saved {len(results)} platform results to {out_path}")
spark.stop()