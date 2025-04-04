# 🚀 Snowflake JSON Data Pipeline Project

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

## 🗂️ Dataset

You’ll need to upload sample JSON files (e.g., order transactions) to your Amazon S3 bucket. Files must follow this format:

```json
{
  "order_id": 101,
  "order_date": "2023-08-01",
  "customer": {
    "customer_id": 1,
    "email": "user@example.com",
    "name": "John Doe",
    "address": "123 Main Street"
  },
  "products": [
    {
      "product_id": 501,
      "name": "T-Shirt",
      "category": "Apparel",
      "price": 25,
      "quantity": 2
    }
  ]
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
  c.name AS customer_name,
  o.order_date,
  p.name AS product_name,
  o.quantity
FROM NAMASTEMART.DW.ORDERS o
JOIN NAMASTEMART.DW.CUSTOMERS c ON o.customer_key = c.customer_key
JOIN NAMASTEMART.DW.PRODUCTS p ON o.product_key = p.product_key;

```
## 📌 Highlights

- ⚡ **Real-time stream processing** using `system$stream_has_data()`
- 🔁 **Multi-step task chaining** using `AFTER` dependencies for orchestration
- 🔄 **Dynamic and incremental updates** using `MERGE` and `ROW_NUMBER()`
- 🔧 **Extensible architecture**: Add Snowpipe, error handling, or audit logging


## 👨‍💻 Author

**Atharva Devne**  
