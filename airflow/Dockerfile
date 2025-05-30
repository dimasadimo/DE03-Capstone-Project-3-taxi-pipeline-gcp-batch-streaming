FROM apache/airflow:2.10.0-python3.10

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
    
USER root
RUN apt-get update && apt-get install -y git
USER airflow
# Copy only requirements first for caching
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt