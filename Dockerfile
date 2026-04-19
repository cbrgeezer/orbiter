FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml README.md /app/
COPY src /app/src
COPY examples /app/examples
COPY docs /app/docs

RUN pip install --no-cache-dir .

EXPOSE 8000

CMD ["orbiter", "serve-api", "examples/example_dag.py", "--host", "0.0.0.0", "--port", "8000", "--db", "postgresql://orbiter:orbiter@postgres:5432/orbiter", "--queue-backend", "store"]
