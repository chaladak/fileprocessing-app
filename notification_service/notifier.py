import os
import json
import time
import pika
import logging
import requests
from sqlalchemy import create_engine, Column, String, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Base = declarative_base()

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

# Get RabbitMQ URL from environment variables
def get_rabbitmq_url():
    rabbitmq_url = os.environ.get("RABBITMQ_URL")
    if not rabbitmq_url:
        rabbitmq_host = os.environ.get("RABBITMQ_HOST", "rabbitmq-service")
        rabbitmq_user = os.environ.get("RABBITMQ_USER", "guest")
        rabbitmq_pass = os.environ.get("RABBITMQ_PASSWORD", "guest")
        rabbitmq_url = f"amqp://{rabbitmq_user}:{rabbitmq_pass}@{rabbitmq_host}:5672/%2F"
        logger.info(f"Constructed RabbitMQ URL from environment variables: {rabbitmq_host}")
    return rabbitmq_url

# Get Database URL from environment variables
def get_database_url():
    # Check TESTING first to use SQLite for tests
    if os.getenv("TESTING", "false").lower() == "true":
        logger.info("Using SQLite in-memory database for testing")
        return "sqlite:///:memory:"
    
    # Production: Construct PostgreSQL URL
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        pg_user = os.environ.get("POSTGRES_USER")
        pg_pass = os.environ.get("POSTGRES_PASSWORD")
        pg_host = os.environ.get("POSTGRES_HOST")
        pg_db = os.environ.get("POSTGRES_DB")
        if all([pg_user, pg_pass, pg_host, pg_db]):
            database_url = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:5432/{pg_db}"
            logger.info(f"Constructed DATABASE_URL from environment variables")
        else:
            logger.error("Missing environment variables for PostgreSQL; cannot construct DATABASE_URL")
            raise ValueError("DATABASE_URL or PostgreSQL environment variables not set")
    return database_url

# Database setup
DATABASE_URL = get_database_url()
logger.info(f"Using database URL: {DATABASE_URL}")
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {},
    poolclass=StaticPool if "sqlite" in DATABASE_URL else None,
)
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_rabbitmq_connection():
    """Ensure RabbitMQ is ready before attempting to connect."""
    rabbitmq_url = get_rabbitmq_url()
    logger.info(f"Connecting to RabbitMQ using URL parameters (host: {rabbitmq_url.split('@')[1].split('/')[0]})")

    retry_count = 0
    max_retries = 30
    while retry_count < max_retries:
        try:
            connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
            logger.info(f"RabbitMQ connection established (Attempt {retry_count + 1})")
            return connection
        except pika.exceptions.AMQPConnectionError:
            retry_count += 1
            logger.info(f"Connection attempt {retry_count}/{max_retries} failed. Retrying...")
            time.sleep(2)

    logger.error("Failed to connect to RabbitMQ after multiple attempts")
    raise Exception("Failed to connect to RabbitMQ after multiple attempts")

def send_notification(job_id, status, result):
    """
    Send notification about file processing results
    In a real application, this would send emails, push notifications, etc.
    """
    logger.info(f"Sending notification for job {job_id} with status {status}")
    
    try:
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
        raise  # Raise exception to be handled by caller

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
    
    connection = get_rabbitmq_connection()
    
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