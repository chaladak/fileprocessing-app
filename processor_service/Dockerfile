FROM python:3.9

WORKDIR /app

# Copy wait script first
COPY wait-for-rabbitmq.py /app/wait-for-rabbitmq.py
RUN chmod +x /app/wait-for-rabbitmq.py

# Copy the rest of your application
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Use an entrypoint script to wait for RabbitMQ then start your app
CMD ["sh", "-c", "python /app/wait-for-rabbitmq.py && python /app/processor.py"]