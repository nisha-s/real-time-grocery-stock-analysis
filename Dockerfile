# Real-Time Grocery Stock Analysis
# Python 3.11 + Java (for PySpark) + OpenCV runtime libraries.
FROM python:3.11-slim

# System dependencies:
#  - default-jre-headless : Java runtime required by PySpark
#  - libgl1, libglib2.0-0  : shared libraries required by OpenCV (cv2)
RUN apt-get update && apt-get install -y --no-install-recommends \
        default-jre-headless \
        libgl1 \
        libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install CPU-only PyTorch first (much smaller than the default CUDA build),
# then the remaining requirements (torch is already satisfied).
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir torch torchvision --index-url https://download.pytorch.org/whl/cpu

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project code and the fine-tuned model into the image.
# The dataset and runtime outputs are bind-mounted at run time (see docker-compose.yml).
COPY . .

# Default command: the detection pipeline. Override for the processor/visualizer.
CMD ["python", "-m", "src.realtime_grozi_pipeline"]
