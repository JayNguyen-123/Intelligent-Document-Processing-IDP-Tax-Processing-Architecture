# Use an official, lightweight Python runtime as the parent base image
FROM python:3.10-slim

# Prevent Python from writing .pyc files to disk and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working execution directory inside the container structure
WORKDIR /app

# Install system-level operational dependencies required for performance
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

# Copy the dependencies configuration file into the container workspace
COPY requirements.txt .

# Install pinned python dependencies via pip package manager wheels
RUN pip install --no-cache-dir -r requirements.txt

# Copy the remaining dashboard source code execution files into the container
COPY . .

# Expose port 8501, which is the default port utilized by Streamlit applications
EXPOSE 8501

# Configure container health checking criteria for Cloud Run orchestration
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
  CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Launch the Streamlit dashboard server with optimized cloud runtime arguments
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
