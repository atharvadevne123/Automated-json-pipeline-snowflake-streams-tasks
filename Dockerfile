FROM python:3.11-slim
WORKDIR /app
COPY snowflake_pipeline/ ./snowflake_pipeline/
COPY snowflake_optimized.sql snowflake.txt sample_amazon_reviews.json ./
RUN pip install --no-cache-dir snowflake-connector-python
CMD ["python", "-c", "import snowflake_pipeline; print(snowflake_pipeline.list_sql())"]
