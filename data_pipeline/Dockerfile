FROM prefecthq/prefect:3.4.24.dev5-python3.13
WORKDIR /app

# Copy and install dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

# Copy your code (ignores .venv due to .dockerignore)
COPY . .
ENV PYTHONPATH=/app

CMD ["prefect", "server", "start", "--host", "0.0.0.0"]
