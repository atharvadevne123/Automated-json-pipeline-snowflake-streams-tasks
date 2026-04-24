FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir build && python -m build && pip install dist/*.whl
ENTRYPOINT ["python", "-c", "import snowflake_pipeline; print(snowflake_pipeline.list_sql())"]
