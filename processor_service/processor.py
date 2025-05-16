import os
import json
import time
import pika
import boto3
import logging
import hashlib
from sqlalchemy import create_engine, Column, String, DateTime, Text
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
from pika.exceptions import AMQPConnectionError, ChannelClosed, ConnectionClosed

# Set up logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define the models
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

# Database setup
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    # Construct DATABASE_URL from individual parts if not provided
    pg_user = os.environ.get("POSTGRES_USER")
    pg_pass = os.environ.get("POSTGRES_PASSWORD")
    pg_host = os.environ.get("POSTGRES_HOST")
    pg_db = os.environ.get("POSTGRES_DB")
    if all([pg_user, pg_pass, pg_host, pg_db]):
        DATABASE_URL = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:5432/{pg_db}"
        logger.info(f"Constructed DATABASE_URL from environment variables")

logger.info(f"Using database URL: {DATABASE_URL}")
engine = create_engine(DATABASE_URL)

# Create tables if they don't exist
Base.metadata.create_all(bind=engine)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# S3 setup
s3_client = boto3.client(
    's3',
    endpoint_url=os.environ.get("S3_ENDPOINT", f"http://{os.environ.get('MINIO_HOST')}"),
    aws_access_key_id=os.environ.get("S3_ACCESS_KEY", os.environ.get("MINIO_ACCESS_KEY")),
    aws_secret_access_key=os.environ.get("S3_SECRET_KEY", os.environ.get("MINIO_SECRET_KEY")),
    region_name='us-east-1'
)
BUCKET_NAME = "file-processing"

# NFS path
NFS_PATH = os.environ.get("NFS_PATH", "/mnt/nfs_clientshare")

# Construct RabbitMQ URL from environment variables if not directly provided
def get_rabbitmq_url():
    rabbitmq_url = os.environ.get("RABBITMQ_URL")
    if not rabbitmq_url:
        # Build it from individual environment variables
        rabbitmq_host = os.environ.get("RABBITMQ_HOST", "localhost")
        rabbitmq_user = os.environ.get("RABBITMQ_USER", "guest")
        rabbitmq_pass = os.environ.get("RABBITMQ_PASSWORD", "guest")
        rabbitmq_url = f"amqp://{rabbitmq_user}:{rabbitmq_pass}@{rabbitmq_host}:5672/%2F"
        logger.info(f"Constructed RabbitMQ URL from environment variables: {rabbitmq_host}")
    return rabbitmq_url

def get_rabbitmq_connection(max_retries=30, retry_delay=5):
    """
    Create and return a persistent RabbitMQ connection with retry mechanism.
    
    Args:
        max_retries (int): Number of connection retry attempts
        retry_delay (int): Delay between retry attempts in seconds
    """
    rabbitmq_url = get_rabbitmq_url()
    logger.info(f"Connecting to RabbitMQ using URL parameters (host: {rabbitmq_url.split('@')[1].split('/')[0]})")
    
    for attempt in range(max_retries):
        try:
            params = pika.URLParameters(rabbitmq_url + "?heartbeat=600")
            connection = pika.BlockingConnection(params)
            logger.info(f"RabbitMQ connection established (Attempt {attempt + 1})")
            return connection
        except (AMQPConnectionError, ConnectionClosed) as e:
            logger.warning(f"RabbitMQ connection failed (Attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error("Failed to establish RabbitMQ connection after maximum retries")
                raise

def compute_file_hash(nfs_path):
    """Compute SHA-256 hash of the file content."""
    hasher = hashlib.sha256()
    with open(nfs_path, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()

# Function to wait for NFS file availability
def wait_for_file(nfs_path, retries=5, delay=1):
    for attempt in range(retries):
        if os.path.exists(nfs_path):
            logger.info(f"File {nfs_path} is accessible after {attempt + 1} checks.")
            return True
        logger.warning(f"File {nfs_path} not found, retrying in {delay} seconds (Attempt {attempt + 1}/{retries})...")
        time.sleep(delay)
    logger.error(f"File {nfs_path} not found after {retries} retries.")
    return False

def process_file(message):
    job_id = message["job_id"]
    filename = message["filename"]
    nfs_path = message["nfs_path"]
    
    logger.info(f"Processing file for job {job_id}")
    try:
        # Simulate file processing (e.g., calculate file size and hash)
        import hashlib
        with open(nfs_path, "rb") as f:
            file_content = f.read()
            file_hash = hashlib.sha256(file_content).hexdigest()
        result = {
            "size_bytes": len(file_content),
            "processed_timestamp": datetime.now().isoformat(),
            "file_hash": file_hash,
            "success": True
        }

        # Update FileRecord in database
        db = SessionLocal()
        file_record = db.query(FileRecord).filter(FileRecord.id == job_id).first()
        if file_record:
            file_record.status = "processed"
            file_record.processed_at = datetime.now()
            file_record.processing_result = json.dumps(result)
            db.commit()
            logger.info(f"Updated FileRecord for job {job_id} to status: processed")
        else:
            logger.error(f"FileRecord not found for job {job_id}")
            raise Exception("FileRecord not found")
        db.close()

        # Send notification to RabbitMQ
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        channel.queue_declare(queue="notifications")
        notification_message = {
            "job_id": job_id,
            "status": "processed",
            "result": result
        }
        channel.basic_publish(
            exchange="",
            routing_key="notifications",
            body=json.dumps(notification_message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        logger.info(f"Sent notification for job {job_id}")
        connection.close()

    except Exception as e:
        logger.error(f"Error processing job {job_id}: {str(e)}")
        db = SessionLocal()
        file_record = db.query(FileRecord).filter(FileRecord.id == job_id).first()
        if file_record:
            file_record.status = "error"
            db.commit()
        db.close()
        raise

def callback(ch, method, properties, body):
    try:
        message = json.loads(body)
        process_file(message)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Processing error for job {message.get('job_id', 'unknown')}: {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    logger.info("File processor service starting...")

    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()

        channel.queue_declare(queue='file_processing')
        channel.basic_qos(prefetch_count=1)

        channel.basic_consume(
            queue='file_processing',
            on_message_callback=callback
        )

        logger.info("Waiting for file processing jobs. To exit press CTRL+C")

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
            channel.stop_consuming()
        finally:
            if not connection.is_closed:
                connection.close()

    except Exception as startup_error:
        logger.error(f"Service startup error: {startup_error}")
    finally:
        logger.info("Consumer stopped")

if __name__ == "__main__":
    main()