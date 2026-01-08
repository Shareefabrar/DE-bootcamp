FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

COPY pyproject.toml .python-version uv.lock ./

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./

RUN uv sync --locked

COPY pipeline.py .

ENTRYPOINT ["python", "pipeline.py"]

# ENTRYPOINT [ "uv", "run", "python", "pipeline.py"]

# pipeline.py /code/pipeline.py

# ENTRYPOINT ["python", "pipeline.py"]