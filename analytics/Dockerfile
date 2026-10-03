FROM apache/spark-py:v3.5.0

USER root
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENV PYTHONPATH=/app
USER spark
CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0", "/app/src/main.py"]
