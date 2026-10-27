# syntax=docker/dockerfile:1
# check=skip=JSONArgsRecommended
# ^ CMD below is shell-form on purpose, to expand $PORT — but it uses `exec`,
# which replaces the shell as PID 1, so signal handling is fine despite the lint.
# Pinned by digest: Renovate also picks up rebuilds of the same tag (Debian security fixes), each an automatic patch release
FROM python:3.15.2-slim@sha256:197f6622a70b727a4e61bdac13a73b4e2b914b896aa5a729ceb1f45a09bde5fa

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
