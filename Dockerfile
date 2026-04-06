FROM python:3.12-slim

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

# Run the app
CMD ["python", "app.py"]