FROM apache/airflow:2.10.2

# Switch to airflow user BEFORE installing packages
USER airflow

# Copy requirements.txt from the build context (repo root) into the image
COPY requirements.txt /requirements.txt

# Install extra Python packages with Airflow constraints
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.12.txt" \
    -r /requirements.txt
