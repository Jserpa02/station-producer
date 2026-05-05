FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir pika==1.3.2

COPY . .

CMD ["python", "app.py"]
