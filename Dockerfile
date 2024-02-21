FROM apache/airflow:latest-python3.11
COPY requirements.txt .

# Set the default Python version
RUN pip install -r requirements.txt