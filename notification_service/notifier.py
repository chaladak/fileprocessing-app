import os
import json
import time
import pika
import logging
import requests
from sqlalchemy import create_engine, Column, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Read RabbitMQ host and port from the environment variables
rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")

# Database setup (use DATABASE_URL from environment variable)
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
Base = declarative_base()
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class FileRecord(Base):
    __tablename__ = "file_records"
    
    id = Column(String, primary_key=True)
    filename = Column(String, nullable=False)
    s3_path = Column(String, nullable=False)
    nfs_path = Column(String, nullable=False)
    status = Column(String, nullable=False)
    uploaded_at = Column(DateTime, nullable=False)
    processed_at = Column(DateTime, nullable=True)
    processing_result = Column(Text, nullable=True)

class Notification(Base):
    __tablename__ = "notifications"
    
    id = Column(String, primary_key=True)
    job_id = Column(String, nullable=False)
    status = Column(String, nullable=False)
    sent_at = Column(DateTime, nullable=False)
    details = Column(Text, nullable=True)

# RabbitMQ connection and retry logic
def wait_for_rabbitmq():
    """Wait for RabbitMQ to be fully available before connecting."""
    rabbitmq_api_url = "http://rabbitmq:15672/api/healthchecks/node"
    rabbitmq_user = "guest"
    rabbitmq_pass = "guest"
    retry_count = 0
    max_retries = 30

    while retry_count < max_retries:
        try:
            response = requests.get(rabbitmq_api_url, auth=(rabbitmq_user, rabbitmq_pass), timeout=5)
            if response.status_code == 200 and response.json().get("status") == "ok":
                logger.info("RabbitMQ is ready.")
                return True
        except requests.RequestException:
            logger.info(f"Waiting for RabbitMQ... Attempt {retry_count + 1}/{max_retries}")

        retry_count += 1
        time.sleep(2)

    raise Exception("RabbitMQ did not become ready in time.")

def get_rabbitmq_connection():
    """Ensure RabbitMQ is ready before attempting to connect."""
    wait_for_rabbitmq()

    retry_count = 0
    max_retries = 30
    while retry_count < max_retries:
        try:
            connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
            return connection
        except pika.exceptions.AMQPConnectionError:
            retry_count += 1
            logger.info(f"Connection attempt {retry_count}/{max_retries} failed. Retrying...")
            time.sleep(2)

    raise Exception("Failed to connect to RabbitMQ after multiple attempts")

def send_notification(job_id, status, result):
    """
    Send notification about file processing results
    In a real application, this would send emails, push notifications, etc.
    """
    logger.info(f"Sending notification for job {job_id} with status {status}")
    
    # Simulate HTTP webhook call (for demonstration purposes)
    try:
        # Placeholder for actual HTTP webhook logic
        logger.info(f"NOTIFICATION: Job {job_id} - Status: {status} - Result: {result}")
        
        # Save notification to the database
        db = SessionLocal()
        notification = Notification(
            id=f"{job_id}_{datetime.now().timestamp()}",
            job_id=job_id,
            status=status,
            sent_at=datetime.now(),
            details=json.dumps(result)
        )
        db.add(notification)
        db.commit()
        db.close()
        
        return True
    except Exception as e:
        logger.error(f"Error sending notification for job {job_id}: {str(e)}")
        return False

def callback(ch, method, properties, body):
    message = json.loads(body)
    job_id = message["job_id"]
    status = message["status"]
    result = message.get("result", {})
    
    logger.info(f"Received notification job for {job_id} with status {status}")
    
    try:
        send_notification(job_id, status, result)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing notification for job {job_id}: {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    logger.info("Notification service starting...")
    
    connection = get_rabbitmq_connection()  # Establish the connection after ensuring RabbitMQ is ready
    
    channel = connection.channel()
    channel.queue_declare(queue='notifications')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='notifications', on_message_callback=callback)
    
    logger.info("Waiting for notification messages. To exit press CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Notification service stopping...")
