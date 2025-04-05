# 🚀 Automated Ingestion and Stream-Based ETL for Amazon Review Data Using Snowflake

## 📘 Project Description

This project demonstrates an **automated, event-driven data pipeline** using **Snowflake**, **Amazon S3**, and **SQL Tasks & Streams** to ingest, transform, and structure semi-structured JSON data into analytics-ready tables. It replicates a near real-time data warehousing scenario by loading customer orders, product information, and transaction data using Snowflake-native orchestration and monitoring features.

### ✅ Key Features:
- JSON ingestion from an Amazon S3 bucket into a raw Snowflake table
- Snowflake Streams to capture incremental changes from the raw table
- Snowflake Tasks to automate data movement and transformation into dimension and fact tables
- Flattened JSON parsing to normalize deeply nested structures
- Sequenced task orchestration to load Customers, Products, and Orders in the correct dependency order
- Data cleansing and deduplication using `MERGE` operations and `ROW_NUMBER()` filtering
- Designed with scalability and low latency in mind using stream-based change tracking

---

## 🧱 Key Components

| Component                  | Description                                                                 |
|---------------------------|-----------------------------------------------------------------------------|
| **Amazon S3**             | Source of JSON data files                                                   |
| **Snowflake External Stage** | Secure reference to the S3 location                                        |
| **Raw Table (`json_data`)**   | Initial landing zone using `VARIANT` column                                 |
| **Stream (`stream_json_data`)** | Tracks incremental changes in the raw data                              |
| **Staging Table (`json_data_stg`)** | Temporary store to isolate batch load                             |
| **Tasks (`t_raw_load`, `t_customers`, etc.)** | Automate each transformation step on schedule         |
| **Dimension Tables (`CUSTOMERS`, `PRODUCTS`)** | Flattened, deduplicated JSON data using `MERGE`       |
| **Fact Table (`ORDERS`)**  | Fully joined business table with key relationships                        |

---

# 📦 Amazon Customer Reviews Dataset Setup

This project uses the **Amazon US Customer Reviews Dataset**, available publicly on Kaggle. The dataset contains millions of product reviews submitted by verified Amazon users across various product categories.

## 📥 How to Download the Dataset from Kaggle

````
import kagglehub

# Download latest version
path = kagglehub.dataset_download("cynthiarempel/amazon-us-customer-reviews-dataset")

print("Path to dataset files:", path)
````

---
You’ll need to upload sample JSON files (e.g., order transactions) to your Amazon S3 bucket. Files must follow this format:

```json
{
  "marketplace": "US",
  "customer_id": "123456",
  "review_id": "R1XYZ123ABC456",
  "product_id": "B00EXAMPLE",
  "product_title": "T-Shirt",
  "product_category": "Apparel",
  "star_rating": 5,
  "review_body": "This t-shirt is amazing! The fabric is soft and fits perfectly.",
  "review_date": "2023-08-01",
  "review_headline": "Great quality shirt!",
  "verified_purchase": "Y",
  "reviewer_name": "John Doe"
}
```


## ⚙️ Project Flow

1. ✅ Upload JSON files to **S3 bucket**
2. ✅ `COPY INTO` from external stage into `json_data`
3. ✅ Stream tracks new records and triggers `t_raw_load`
4. ✅ `t_raw_load` pushes data to `json_data_stg`
5. ✅ `t_customers` + `t_products` merge deduplicated data into dimensions
6. ✅ `t_orders` joins customer/product keys to populate fact table
7. ✅ Final result: Fully normalized tables for querying in `NAMASTEMART.DW`

---

## 🧩 Sample Query

```sql
SELECT 
  c.customer_name,
  r.review_date,
  p.product_title AS product_name,
  r.star_rating AS rating
FROM AMAZON_DB.DW.REVIEWS r
JOIN AMAZON_DB.DW.CUSTOMERS c ON r.customer_id = c.customer_id
JOIN AMAZON_DB.DW.PRODUCTS p ON r.product_id = p.product_id;

```
## 📌 Highlights

- ⚡ **Real-time stream processing** using `system$stream_has_data()`
- 🔁 **Multi-step task chaining** using `AFTER` dependencies for orchestration
- 🔄 **Dynamic and incremental updates** using `MERGE` and `ROW_NUMBER()`
- 🔧 **Extensible architecture**: Add Snowpipe, error handling, or audit logging


## 👨‍💻 Author

**Atharva Devne**  
