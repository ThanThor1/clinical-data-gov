# Ảnh chạy pipeline + dashboard của hệ thống quản trị dữ liệu lâm sàng.
# Dữ liệu (data/) KHÔNG nằm trong ảnh mà được gắn vào lúc chạy (xem docker-compose.yml).
FROM python:3.12.13-slim-bookworm

ARG CODE_VERSION=dev

# metadata.db và các khóa bí mật nằm trong /app/state (volume riêng, không nằm trong ảnh)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CODE_VERSION=${CODE_VERSION} \
    CLINICAL_GOV_DB=/app/state/metadata.db \
    PATIENT_HASH_KEY_FILE=/app/state/secrets/patient_hash.key \
    AUDIT_LOG_KEY_FILE=/app/state/secrets/audit_log.key \
    MPLCONFIGDIR=/tmp/matplotlib

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Chạy bằng người dùng thường, không chạy bằng root
RUN useradd --create-home --uid 1000 app \
    && mkdir -p /app/state /app/data \
    && chown -R app:app /app
USER app

EXPOSE 8501
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')"

CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
