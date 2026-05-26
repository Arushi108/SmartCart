import json
import random
import os
from datetime import datetime, timedelta

products = [
    {"name": "iPhone 15", "base_price": 79999, "category": "Smartphones"},
    {"name": "Samsung Galaxy S24", "base_price": 74999, "category": "Smartphones"},
    {"name": "MacBook Air M2", "base_price": 114900, "category": "Laptops"},
    {"name": "Dell XPS 15", "base_price": 145000, "category": "Laptops"},
    {"name": "Sony WH-1000XM5", "base_price": 29990, "category": "Headphones"},
    {"name": "iPad Pro 12.9", "base_price": 112900, "category": "Tablets"},
    {"name": "OnePlus 12", "base_price": 64999, "category": "Smartphones"},
    {"name": "LG OLED TV 55", "base_price": 139990, "category": "TVs"},
    {"name": "Canon EOS R50", "base_price": 67995, "category": "Cameras"},
    {"name": "PS5 Console", "base_price": 54990, "category": "Gaming"},
]

platforms = ["Amazon", "Flipkart"]

def generate_price_history(base_price, days=90):
    history = []
    current_price = base_price
    start_date = datetime.now() - timedelta(days=days)
    for i in range(days):
        date = start_date + timedelta(days=i)
        # Simulate realistic price changes
        change = random.choice([-1, -1, 0, 0, 0, 1])
        percent = random.uniform(0.01, 0.08)
        current_price = current_price * (1 + change * percent)
        current_price = max(base_price * 0.75, min(base_price * 1.15, current_price))
        # Sale events
        if random.random() < 0.05:
            current_price = base_price * random.uniform(0.80, 0.90)
        history.append({
            "date": date.strftime("%Y-%m-%d"),
            "price": round(current_price, 0),
            "platform": random.choice(platforms),
            "in_stock": random.random() > 0.05
        })
    return history

all_data = []
for product in products:
    for platform in platforms:
        history = generate_price_history(product["base_price"])
        for entry in history:
            all_data.append({
                "product_name": product["name"],
                "category": product["category"],
                "platform": platform,
                "date": entry["date"],
                "price": entry["price"],
                "base_price": product["base_price"],
                "in_stock": entry["in_stock"]
            })

os.makedirs("data", exist_ok=True)
with open("data/price_history.json", "w") as f:
    json.dump(all_data, f, indent=2)

# Also save as CSV for PySpark
import csv
with open("data/price_history.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=all_data[0].keys())
    writer.writeheader()
    writer.writerows(all_data)

print(f"Generated {len(all_data)} price records")
print(f"Products: {len(products)}")
print(f"Saved to data/price_history.csv and data/price_history.json")
