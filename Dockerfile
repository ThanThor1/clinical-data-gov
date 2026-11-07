# Ảnh dùng chung cho pipeline (driver Spark), Spark master / worker và dashboard.
# Dùng MỘT ảnh cho cả cụm để phiên bản Spark, Delta Lake, Python giống hệt nhau trên mọi node.
# Dữ liệu (data/) KHÔNG nằm trong ảnh mà được gắn vào lúc chạy (xem docker-compose.yml).
FROM python:3.12.13-slim-bookworm

ARG CODE_VERSION=dev

# Spark cần Java 17+; procps cho các script của Spark
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

# metadata.db và các khóa bí mật nằm trong /app/state (volume riêng, không nằm trong ảnh)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CODE_VERSION=${CODE_VERSION} \
    CLINICAL_GOV_DB=/app/state/metadata.db \
    PATIENT_HASH_KEY_FILE=/app/state/secrets/patient_hash.key \
    AUDIT_LOG_KEY_FILE=/app/state/secrets/audit_log.key \
    MPLCONFIGDIR=/tmp/matplotlib \
    SPARK_HOME=/usr/local/lib/python3.12/site-packages/pyspark \
    PATH=/usr/local/lib/python3.12/site-packages/pyspark/bin:$PATH

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Chạy bằng người dùng thường, không chạy bằng root
RUN useradd --create-home --uid 1000 app \
    && mkdir -p /app/state /app/data \
    && chown -R app:app /app
USER app

# Tải sẵn jar Delta Lake vào ảnh (~/.ivy2.5.2) để lúc chạy không cần mạng
RUN SPARK_UI=0 python -c "from pipeline.spark_session import get_spark; get_spark('tai-jar').stop()"

# 8501 dashboard | 4040 giao diện Spark của driver | 7077, 8080 Spark master | 8081 Spark worker
EXPOSE 8501 4040 7077 8080 8081
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')"

CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
