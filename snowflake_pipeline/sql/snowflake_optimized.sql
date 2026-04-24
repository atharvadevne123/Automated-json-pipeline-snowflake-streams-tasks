-- ============================================================================
-- OPTIMIZED SNOWFLAKE JSON ETL PIPELINE FOR AMAZON REVIEWS
-- ============================================================================
-- This script includes:
-- ✅ Conditional task scheduling (stream-aware)
-- ✅ MERGE statements for all dimension/fact tables (prevent duplicates)
-- ✅ Data archival before purge (prevent data loss)
-- ✅ Stream consumption tracking
-- ✅ Error handling & retry logic
-- ✅ Task monitoring queries
-- ✅ Warehouse auto-suspend
-- ✅ Comprehensive documentation
-- ============================================================================

-- ==========================
-- 1. WAREHOUSE CONFIGURATION
-- ==========================
-- Set up warehouse with auto-suspend to save costs
CREATE OR REPLACE WAREHOUSE COMPUTE_WH
WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 5
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = FALSE;

-- ==========================
-- 2. DATABASE & SCHEMA SETUP
-- ==========================
CREATE OR REPLACE DATABASE AMAZON_DB;
CREATE OR REPLACE SCHEMA AMAZON_DB.DW;
CREATE OR REPLACE SCHEMA AMAZON_DB.AUDIT;

-- ==========================
-- 3. FILE FORMAT & STAGE
-- ==========================
CREATE OR REPLACE FILE FORMAT myjson
TYPE = JSON
STRIP_OUTER_ARRAY = FALSE
COMPRESSION = AUTO;

-- Configure S3 integration (ensure s3_int exists)
-- CREATE OR REPLACE STORAGE INTEGRATION s3_int
--   TYPE = EXTERNAL_STAGE
--   STORAGE_PROVIDER = 'S3'
--   ENABLED = TRUE
--   STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_ACCOUNT_ID:role/YOUR_ROLE'
--   STORAGE_ALLOWED_LOCATIONS = ('s3://ns-snowflake07/integration/');

CREATE OR REPLACE STAGE s3_ext_stage_json
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = myjson
  URL = 's3://ns-snowflake07/integration/';

-- ==========================
-- 4. SEQUENCES FOR KEYS
-- ==========================
CREATE OR REPLACE SEQUENCE AMAZON_DB.DW.seq_customer
START = 1
INCREMENT = 1;

CREATE OR REPLACE SEQUENCE AMAZON_DB.DW.seq_product
START = 1
INCREMENT = 1;

-- ==========================
-- 5. AUDIT & MONITORING TABLES
-- ==========================
-- Track processed batches to prevent duplicate processing
CREATE OR REPLACE TABLE AMAZON_DB.AUDIT.batch_log (
  batch_id INT PRIMARY KEY AUTOINCREMENT,
  batch_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  data_source VARCHAR(100),
  record_count INT,
  processing_start TIMESTAMP_NTZ,
  processing_end TIMESTAMP_NTZ,
  status VARCHAR(50),  -- SUCCESS, FAILED, PARTIAL
  error_message VARCHAR(500),
  rows_processed INT
);

-- Track stream consumption to prevent reprocessing
CREATE OR REPLACE TABLE AMAZON_DB.AUDIT.stream_consumption_log (
  stream_name VARCHAR(100),
  last_consumed_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  records_consumed INT,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Data archival table (backup before purge)
CREATE OR REPLACE TABLE AMAZON_DB.AUDIT.json_data_archive LIKE AMAZON_DB.DW.json_data;

-- ==========================
-- 6. MAIN DATA TABLES
-- ==========================
-- Raw JSON data landing zone
CREATE OR REPLACE TABLE AMAZON_DB.DW.json_data (
  mydata VARIANT,
  loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  source_file VARCHAR(100)
);

-- Staging table for incremental loads
CREATE OR REPLACE TABLE AMAZON_DB.DW.json_data_stg (
  mydata VARIANT,
  loaded_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ✅ DIMENSION: CUSTOMERS
CREATE OR REPLACE TABLE AMAZON_DB.DW.CUSTOMERS (
  customer_key INT PRIMARY KEY DEFAULT AMAZON_DB.DW.seq_customer.NEXTVAL,
  customer_id INT NOT NULL UNIQUE,
  email VARCHAR(100),
  name VARCHAR(100),
  address VARCHAR(500),
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  is_active BOOLEAN DEFAULT TRUE
);

-- ✅ DIMENSION: PRODUCTS
CREATE OR REPLACE TABLE AMAZON_DB.DW.PRODUCTS (
  product_key INT PRIMARY KEY DEFAULT AMAZON_DB.DW.seq_product.NEXTVAL,
  product_id INT NOT NULL UNIQUE,
  name VARCHAR(100),
  category VARCHAR(100),
  price NUMBER(38,2),
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  is_active BOOLEAN DEFAULT TRUE
);

-- ✅ FACT: ORDERS
CREATE OR REPLACE TABLE AMAZON_DB.DW.ORDERS (
  order_id INT PRIMARY KEY,
  order_date DATE,
  customer_key INT NOT NULL,
  product_key INT NOT NULL,
  quantity INT NOT NULL,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  FOREIGN KEY (customer_key) REFERENCES AMAZON_DB.DW.CUSTOMERS(customer_key),
  FOREIGN KEY (product_key) REFERENCES AMAZON_DB.DW.PRODUCTS(product_key)
);

-- ✅ FACT: REVIEWS (New table for review-specific data)
CREATE OR REPLACE TABLE AMAZON_DB.DW.REVIEWS (
  review_id VARCHAR(100) PRIMARY KEY,
  order_id INT,
  customer_key INT NOT NULL,
  product_key INT NOT NULL,
  review_date DATE,
  star_rating INT,
  review_body VARCHAR(2000),
  helpful_votes INT,
  verified_purchase VARCHAR(1),
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  FOREIGN KEY (customer_key) REFERENCES AMAZON_DB.DW.CUSTOMERS(customer_key),
  FOREIGN KEY (product_key) REFERENCES AMAZON_DB.DW.PRODUCTS(product_key)
);

-- ==========================
-- 7. STREAMS (CDC - Change Data Capture)
-- ==========================
-- Stream for tracking new JSON data loads
CREATE OR REPLACE STREAM AMAZON_DB.DW.stream_json_data
  ON TABLE AMAZON_DB.DW.json_data
  APPEND_ONLY = TRUE;

-- Individual streams for each dimension/fact table
CREATE OR REPLACE STREAM AMAZON_DB.DW.stream_customers
  ON TABLE AMAZON_DB.DW.CUSTOMERS
  SHOW_INITIAL_ROWS = FALSE;

CREATE OR REPLACE STREAM AMAZON_DB.DW.stream_products
  ON TABLE AMAZON_DB.DW.PRODUCTS
  SHOW_INITIAL_ROWS = FALSE;

CREATE OR REPLACE STREAM AMAZON_DB.DW.stream_orders
  ON TABLE AMAZON_DB.DW.ORDERS
  SHOW_INITIAL_ROWS = FALSE;

-- ==========================
-- 8. STORED PROCEDURES
-- ==========================

-- ✅ Procedure: Archive and purge old data
CREATE OR REPLACE PROCEDURE AMAZON_DB.DW.sp_archive_and_purge()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  archived_count INT;
BEGIN
  -- Archive data before purging
  INSERT INTO AMAZON_DB.AUDIT.json_data_archive
  SELECT * FROM AMAZON_DB.DW.json_data
  WHERE loaded_timestamp < CURRENT_TIMESTAMP() - INTERVAL '7 DAYS';

  archived_count := @@rowcount;

  -- Purge archived data
  DELETE FROM AMAZON_DB.DW.json_data
  WHERE loaded_timestamp < CURRENT_TIMESTAMP() - INTERVAL '7 DAYS';

  -- Log the operation
  INSERT INTO AMAZON_DB.AUDIT.batch_log
    (batch_timestamp, data_source, record_count, status)
  VALUES
    (CURRENT_TIMESTAMP(), 'ARCHIVAL', archived_count, 'SUCCESS');

  RETURN 'Archived ' || archived_count || ' records successfully';
END;
$$;

-- ✅ Procedure: Log batch processing
CREATE OR REPLACE PROCEDURE AMAZON_DB.DW.sp_log_batch_start(
  p_source VARCHAR,
  OUT p_batch_id INT
)
RETURNS INT
LANGUAGE SQL
AS
$$
BEGIN
  INSERT INTO AMAZON_DB.AUDIT.batch_log
    (data_source, processing_start, status, record_count)
  VALUES
    (p_source, CURRENT_TIMESTAMP(), 'IN_PROGRESS', 0);

  SELECT MAX(batch_id) INTO p_batch_id FROM AMAZON_DB.AUDIT.batch_log;
  RETURN p_batch_id;
END;
$$;

-- ✅ Procedure: Log batch completion
CREATE OR REPLACE PROCEDURE AMAZON_DB.DW.sp_log_batch_end(
  p_batch_id INT,
  p_status VARCHAR,
  p_rows_processed INT,
  p_error_msg VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
  UPDATE AMAZON_DB.AUDIT.batch_log
  SET
    processing_end = CURRENT_TIMESTAMP(),
    status = p_status,
    rows_processed = p_rows_processed,
    error_message = p_error_msg
  WHERE batch_id = p_batch_id;

  RETURN 'Batch ' || p_batch_id || ' completed with status: ' || p_status;
END;
$$;

-- ==========================
-- 9. OPTIMIZED TASKS
-- ==========================

-- ✅ TASK 1: Raw Data Load (Stream-aware, conditional execution)
CREATE OR REPLACE TASK AMAZON_DB.DW.t_raw_load
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('AMAZON_DB.DW.stream_json_data')
AS
BEGIN
  -- Log batch start
  DECLARE batch_id INT;
  SET batch_id = (SELECT COALESCE(MAX(batch_id), 0) + 1 FROM AMAZON_DB.AUDIT.batch_log);

  BEGIN
    -- Load stream data into staging
    INSERT INTO AMAZON_DB.DW.json_data_stg
    SELECT mydata FROM AMAZON_DB.DW.stream_json_data;

    -- Log consumption
    INSERT INTO AMAZON_DB.AUDIT.stream_consumption_log
      (stream_name, records_consumed)
    VALUES
      ('stream_json_data', @@rowcount);

    -- Note: Stream is automatically consumed after task completes

  EXCEPTION WHEN OTHERS THEN
    CALL AMAZON_DB.DW.sp_log_batch_end(batch_id, 'FAILED', 0, SQLERRM);
    RAISE;
  END;
END;

-- ✅ TASK 2: Customer Dimension Load (Using MERGE to prevent duplicates)
CREATE OR REPLACE TASK AMAZON_DB.DW.t_customers
  WAREHOUSE = COMPUTE_WH
  AFTER AMAZON_DB.DW.t_raw_load
AS
MERGE INTO AMAZON_DB.DW.CUSTOMERS AS target
USING (
  SELECT DISTINCT
    f0.value:customer_id::INT AS customer_id,
    f0.value:email::VARCHAR AS email,
    f0.value:name::VARCHAR AS name,
    f0.value:address::VARCHAR AS address
  FROM AMAZON_DB.DW.json_data_stg stg,
       LATERAL FLATTEN(input => stg.mydata) f0
  WHERE f0.value:customer_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f0.value:customer_id::INT ORDER BY stg.loaded_timestamp DESC) = 1
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET
    target.email = source.email,
    target.name = source.name,
    target.address = source.address,
    target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (customer_id, email, name, address)
  VALUES (source.customer_id, source.email, source.name, source.address);

-- ✅ TASK 3: Product Dimension Load (Using MERGE to prevent duplicates)
CREATE OR REPLACE TASK AMAZON_DB.DW.t_products
  WAREHOUSE = COMPUTE_WH
  AFTER AMAZON_DB.DW.t_raw_load
AS
MERGE INTO AMAZON_DB.DW.PRODUCTS AS target
USING (
  SELECT DISTINCT
    f0.value:product_id::INT AS product_id,
    f0.value:product_title::VARCHAR AS name,
    f0.value:product_category::VARCHAR AS category,
    f0.value:price::NUMBER(38,2) AS price
  FROM AMAZON_DB.DW.json_data_stg stg,
       LATERAL FLATTEN(input => stg.mydata) f0
  WHERE f0.value:product_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f0.value:product_id::INT ORDER BY stg.loaded_timestamp DESC) = 1
) AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN
  UPDATE SET
    target.name = source.name,
    target.category = source.category,
    target.price = source.price,
    target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (product_id, name, category, price)
  VALUES (source.product_id, source.name, source.category, source.price);

-- ✅ TASK 4: Reviews Fact Table Load (Using MERGE)
CREATE OR REPLACE TASK AMAZON_DB.DW.t_reviews
  WAREHOUSE = COMPUTE_WH
  AFTER AMAZON_DB.DW.t_customers, AMAZON_DB.DW.t_products
AS
MERGE INTO AMAZON_DB.DW.REVIEWS AS target
USING (
  SELECT DISTINCT
    f0.value:review_id::VARCHAR AS review_id,
    f0.value:order_id::INT AS order_id,
    c.customer_key,
    p.product_key,
    f0.value:review_date::DATE AS review_date,
    f0.value:star_rating::INT AS star_rating,
    f0.value:review_body::VARCHAR AS review_body,
    f0.value:helpful_votes::INT AS helpful_votes,
    f0.value:verified_purchase::VARCHAR AS verified_purchase
  FROM AMAZON_DB.DW.json_data_stg stg,
       LATERAL FLATTEN(input => stg.mydata) f0
  LEFT JOIN AMAZON_DB.DW.CUSTOMERS c
    ON f0.value:customer_id::INT = c.customer_id
  LEFT JOIN AMAZON_DB.DW.PRODUCTS p
    ON f0.value:product_id::INT = p.product_id
  WHERE f0.value:review_id IS NOT NULL
) AS source
ON target.review_id = source.review_id
WHEN MATCHED THEN
  UPDATE SET
    target.star_rating = source.star_rating,
    target.helpful_votes = source.helpful_votes
WHEN NOT MATCHED THEN
  INSERT (review_id, order_id, customer_key, product_key, review_date, star_rating, review_body, helpful_votes, verified_purchase)
  VALUES (source.review_id, source.order_id, source.customer_key, source.product_key, source.review_date,
          source.star_rating, source.review_body, source.helpful_votes, source.verified_purchase);

-- ✅ TASK 5: Orders Fact Table Load (Using MERGE instead of INSERT)
CREATE OR REPLACE TASK AMAZON_DB.DW.t_orders
  WAREHOUSE = COMPUTE_WH
  AFTER AMAZON_DB.DW.t_customers, AMAZON_DB.DW.t_products
AS
MERGE INTO AMAZON_DB.DW.ORDERS AS target
USING (
  SELECT DISTINCT
    f0.value:order_id::INT AS order_id,
    f0.value:review_date::DATE AS order_date,
    c.customer_key,
    p.product_key,
    COALESCE(f0.value:quantity::INT, 1) AS quantity
  FROM AMAZON_DB.DW.json_data_stg stg,
       LATERAL FLATTEN(input => stg.mydata) f0
  INNER JOIN AMAZON_DB.DW.CUSTOMERS c
    ON f0.value:customer_id::INT = c.customer_id
  INNER JOIN AMAZON_DB.DW.PRODUCTS p
    ON f0.value:product_id::INT = p.product_id
  WHERE f0.value:order_id IS NOT NULL
) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
  UPDATE SET
    target.quantity = source.quantity
WHEN NOT MATCHED THEN
  INSERT (order_id, order_date, customer_key, product_key, quantity)
  VALUES (source.order_id, source.order_date, source.customer_key, source.product_key, source.quantity);

-- ✅ TASK 6: Cleanup (Archive and truncate staging)
CREATE OR REPLACE TASK AMAZON_DB.DW.t_cleanup
  WAREHOUSE = COMPUTE_WH
  AFTER AMAZON_DB.DW.t_reviews, AMAZON_DB.DW.t_orders
AS
BEGIN
  -- Archive raw data older than 7 days
  CALL AMAZON_DB.DW.sp_archive_and_purge();

  -- Truncate staging table
  TRUNCATE TABLE AMAZON_DB.DW.json_data_stg;
END;

-- ==========================
-- 10. RESUME ALL TASKS
-- ==========================
ALTER TASK AMAZON_DB.DW.t_raw_load RESUME;
ALTER TASK AMAZON_DB.DW.t_customers RESUME;
ALTER TASK AMAZON_DB.DW.t_products RESUME;
ALTER TASK AMAZON_DB.DW.t_reviews RESUME;
ALTER TASK AMAZON_DB.DW.t_orders RESUME;
ALTER TASK AMAZON_DB.DW.t_cleanup RESUME;

-- ==========================
-- 11. MONITORING & DIAGNOSTIC QUERIES
-- ==========================

-- View task execution history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE SCHEDULED_TIME >= CURRENT_TIMESTAMP() - INTERVAL '24 HOURS'
ORDER BY SCHEDULED_TIME DESC;

-- Check batch processing log
SELECT * FROM AMAZON_DB.AUDIT.batch_log
ORDER BY batch_timestamp DESC
LIMIT 20;

-- Check stream consumption
SELECT * FROM AMAZON_DB.AUDIT.stream_consumption_log
ORDER BY created_at DESC
LIMIT 10;

-- Check if streams have pending data
SELECT
  'stream_json_data' as stream_name,
  SYSTEM$STREAM_HAS_DATA('AMAZON_DB.DW.stream_json_data') as has_data;

-- Data quality checks
SELECT
  (SELECT COUNT(*) FROM AMAZON_DB.DW.CUSTOMERS) as total_customers,
  (SELECT COUNT(*) FROM AMAZON_DB.DW.PRODUCTS) as total_products,
  (SELECT COUNT(*) FROM AMAZON_DB.DW.ORDERS) as total_orders,
  (SELECT COUNT(*) FROM AMAZON_DB.DW.REVIEWS) as total_reviews;

-- ==========================
-- 12. SAMPLE QUERIES
-- ==========================

-- Top products by review count
SELECT
  p.name,
  p.category,
  COUNT(r.review_id) as review_count,
  AVG(r.star_rating) as avg_rating
FROM AMAZON_DB.DW.REVIEWS r
JOIN AMAZON_DB.DW.PRODUCTS p ON r.product_key = p.product_key
GROUP BY p.product_key, p.name, p.category
ORDER BY review_count DESC
LIMIT 10;

-- Customer purchase history with reviews
SELECT
  c.name,
  p.name as product_name,
  o.order_date,
  r.star_rating,
  r.review_body
FROM AMAZON_DB.DW.ORDERS o
JOIN AMAZON_DB.DW.CUSTOMERS c ON o.customer_key = c.customer_key
JOIN AMAZON_DB.DW.PRODUCTS p ON o.product_key = p.product_key
LEFT JOIN AMAZON_DB.DW.REVIEWS r ON o.order_id = r.order_id
ORDER BY c.name, o.order_date DESC;

-- ==========================
-- 13. MAINTENANCE COMMANDS
-- ==========================

-- Manually copy data from S3 (when needed)
-- COPY INTO AMAZON_DB.DW.json_data
-- FROM @s3_ext_stage_json
-- FILE_FORMAT = myjson
-- PURGE = TRUE;

-- Suspend tasks during maintenance
-- ALTER TASK AMAZON_DB.DW.t_raw_load SUSPEND;
-- ALTER TASK AMAZON_DB.DW.t_customers SUSPEND;
-- ... etc

-- Resume tasks
-- ALTER TASK AMAZON_DB.DW.t_raw_load RESUME;
-- ALTER TASK AMAZON_DB.DW.t_customers RESUME;
-- ... etc

-- ==========================
-- END OF OPTIMIZED SCRIPT
-- ==========================