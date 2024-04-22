FROM apache/airflow:slim-2.9.0b2-python3.12
COPY pyproject.toml .


USER 0

# Install Poetry (if not already installed on the base image)
RUN apt-get update && apt-get install -y --no-install-recommends \
       python3-pip  # Assuming Debian-based system

USER airflow

# Install Poetry
RUN pip install poetry

# Install dependencies with Poetry (excluding test dependencies)
RUN poetry config virtualenvs.create false  # Disable virtualenv creation within Docker
RUN poetry install --no-dev  # Install dependencies excluding development packages
