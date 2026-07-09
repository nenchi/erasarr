FROM python:3.14-slim

WORKDIR /app

# Install dependencies from app folder
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only the app code (not everything)
COPY app/ .

# Persistent data volume
VOLUME ["/data"]

ENV DATA_DIR=/data
ENV PORT=5000
ENV PYTHONUNBUFFERED=1

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import os, urllib.request; urllib.request.urlopen(f'http://localhost:{os.environ.get(\"PORT\", \"5000\")}/health')" || exit 1

# Run with gunicorn (1 worker — APScheduler runs in-process and must be single-worker).
# Shell form (not exec-form JSON array) so $PORT actually gets substituted; `exec` keeps
# gunicorn as PID 1 so it receives SIGTERM directly for a clean shutdown.
CMD exec gunicorn --bind 0.0.0.0:${PORT:-5000} --workers 1 --threads 4 --timeout 120 --access-logfile - --error-logfile - app:app
