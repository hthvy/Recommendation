FROM python:3.9-slim

WORKDIR /app

# Copy requirements và cài đặt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code vào thư mục src
COPY src/ ./src/

# Chỉnh PYTHONPATH để Python hiểu được folder filters khi import
ENV PYTHONPATH=/app/src

# Tự động chạy main.py khi bật container
CMD ["python", "src/main.py"]