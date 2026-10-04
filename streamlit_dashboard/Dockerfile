# Build from the REPO ROOT so the shared module can be copied in:
#   docker build -f streamlit_dashboard/Dockerfile -t hitl-dashboard .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=true \
    STREAMLIT_CLIENT_TOOLBAR_MODE=minimal \
    PORT=8080

WORKDIR /app

# Dependencies first for layer caching. No compilers needed: every dependency ships wheels.
COPY streamlit_dashboard/requirements.txt .
RUN pip install -r requirements.txt

COPY shared/tax_record.py ./tax_record.py
COPY streamlit_dashboard/app.py ./app.py

# Run as an unprivileged user.
RUN useradd --create-home --uid 10001 appuser && chown -R appuser /app
USER appuser

EXPOSE 8080

# Cloud Run injects $PORT (and ignores Docker HEALTHCHECK - use a startup probe instead).
CMD ["sh", "-c", "exec streamlit run app.py --server.port=${PORT} --server.address=0.0.0.0"]
