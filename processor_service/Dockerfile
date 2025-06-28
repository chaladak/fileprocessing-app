FROM python:3.9-buster

WORKDIR /app

COPY wait-for-rabbitmq.py /app/wait-for-rabbitmq.py
RUN chmod +x /app/wait-for-rabbitmq.py

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Use an entrypoint script to wait for RabbitMQ then start your app
CMD ["sh", "-c", "python /app/wait-for-rabbitmq.py && python /app/processor.py"]
