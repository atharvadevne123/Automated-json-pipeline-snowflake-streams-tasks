FROM python:3.14-slim
WORKDIR /app

COPY pyproject.toml ./
COPY snowflake_pipeline/ ./snowflake_pipeline/
COPY snowflake_optimized.sql sample_amazon_reviews.json ./

RUN pip install --no-cache-dir snowflake-connector-python && \
    useradd --no-create-home --shell /bin/false appuser && \
    chown -R appuser:appuser /app

USER appuser

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import snowflake_pipeline; snowflake_pipeline.list_sql()" || exit 1

CMD ["python", "-c", "import snowflake_pipeline; print(snowflake_pipeline.list_sql())"]
