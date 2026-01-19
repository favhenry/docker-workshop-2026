# Base Docker image that we will build on
FROM python:3.13.11-slim

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

#set up the working directory
WORKDIR /code

# Add virtual environment to PATH so we can use installed packages
ENV PATH="/code/.venv/bin:$PATH"

#copy dependency files first (better layer caching)
COPY pyproject.toml .python-version uv.lock ./

#Install depedencies from lock file(ensures reproducible build)
RUN uv sync --locked

#copy the script to the container. 1st name is source field, 2nd is destination
COPY ingest_data.py .

ENTRYPOINT ["python", "ingest_data.py"]