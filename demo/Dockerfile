# syntax=docker/dockerfile:1
FROM python:3.12-slim
WORKDIR /app
ENV PIP_DEFAULT_TIMEOUT=120 PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install '.[demo]'
COPY demo ./demo
EXPOSE 8000
CMD ["uvicorn", "demo.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
