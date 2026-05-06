FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1


COPY requirements.txt .
# Install all Python dependencies
# RUN pip install --no-cache-dir -r requirements.txt
RUN pip install -r requirements.txt   # cached until requirements.txt changes

# Install system dependencies for insightface
# RUN pip install insightface onnxruntime numpy

# COPY ./app ./app
COPY .env .

EXPOSE 8118

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8118"]