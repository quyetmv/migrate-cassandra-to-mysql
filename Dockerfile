
# Base image: Python 3.7 (Slim for smaller size)
FROM python:3.7-slim

# Functionality: Spark 3.3.0, Java 11, Kerberos (kinit)

# 1. Install System Dependencies
# - openjdk-11-jre-headless: Required for Spark
# - krb5-user: Required for kinit
# - curl, procps: Utilities for downloading & running Spark
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless \
    krb5-user \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# 2. Setup Env Vars for Spark
ENV SPARK_VERSION=3.3.0
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# 3. Download and Install Spark (bin-without-hadoop)
RUN curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz" \
    | tar -xz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-without-hadoop $SPARK_HOME

# 4. Install Python Dependencies
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy Application Code
COPY . /app

# 6. Make script executable
RUN chmod +x /app/run_snapshot_v3.sh

# 7. Set Entrypoint to run the v3 snapshot script
CMD ["/app/run_snapshot_v3.sh"] 
