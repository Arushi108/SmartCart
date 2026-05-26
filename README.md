# SmartCart
# 🛒 SmartCart — Real-Time Price Intelligence Platform

SmartCart is a smart shopping assistance platform designed to help users make better purchasing decisions by comparing product prices across multiple sources and analyzing deals intelligently.

The platform collects pricing information, processes shopping data using Apache PySpark, computes deal insights, and presents users with recommendations through an interactive dashboard.

---

## 🚀 Problem Statement

Online shoppers often spend more than necessary because:

- Comparing prices across multiple websites is time-consuming
- Product prices change frequently
- Users struggle to identify the right time to purchase
- Manual tracking becomes inefficient

SmartCart addresses this by providing a centralized platform for price analysis and deal recommendations.

---

## ✨ Features

- 🔍 Search products and compare prices
- 📊 Deal scoring using PySpark processing
- 📈 Interactive dashboard and visualizations
- ⚡ Fast API-based backend responses
- ☁ Cloud deployment on AWS
- 💰 Savings-focused recommendations

---

## 🛠 Technology Stack

### Frontend
- HTML
- CSS
- JavaScript
- Chart.js

### Backend
- FastAPI
- Python

### Data Processing
- Apache PySpark

### Data Collection
- SerpAPI

### Cloud & Deployment
- AWS EC2
- AWS S3

### Version Control
- Git
- GitHub

---

## 🏗 System Architecture

```text
User Search Request

        ↓

Frontend Dashboard
(HTML/CSS/JavaScript)

        ↓

FastAPI Backend

        ↓

SerpAPI Data Fetching

        ↓

Apache PySpark Processing
(Deal Score Analysis)

        ↓

AWS EC2 + S3

        ↓

Result Dashboard


SmartCart/
│
├── frontend/
│   ├── index.html
│   ├── style.css
│   ├── script.js
│
├── backend/
│   ├── app.py
│   ├── api.py
│   ├── deal_scoring.py
│
├── pyspark/
│   ├── processing.py
│
├── assets/
│
├── requirements.txt
│
└── README.md
