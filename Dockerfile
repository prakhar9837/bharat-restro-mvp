FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    tesseract-ocr-hin \
    curl \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY app/ ./app/
COPY frontend/ ./frontend/
COPY data/ ./data/
COPY README.md ./

# Create data directories
RUN mkdir -p data exports logs

# Set default environment variables
ENV PYTHONPATH=/app \
    BHARAT_RESTO_DB_PATH=/app/data/restaurants.db \
    BHARAT_RESTO_DATA_DIR=/app/data \
    BHARAT_RESTO_EXPORT_DIR=/app/exports \
    BHARAT_RESTO_LOG_LEVEL=INFO \
    BHARAT_RESTO_LLM_ENABLED=true

# Expose port for FastAPI
EXPOSE 8000

# Health check for FastAPI
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command - start FastAPI server
CMD ["python", "-m", "app.server", "--host", "0.0.0.0", "--port", "8000"]
