import os
import json
import uuid
import boto3
import pika
import shutil
import logging
from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from sqlalchemy import create_engine, Column, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from database import Base
from models import FileRecord

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="File Processing API")

# Database setup
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# Log environment variables for debugging
logger.info(f"POSTGRES_USER: {POSTGRES_USER}")
logger.info(f"POSTGRES_PASSWORD: {'*' * len(POSTGRES_PASSWORD) if POSTGRES_PASSWORD else None}")
logger.info(f"POSTGRES_HOST: {POSTGRES_HOST}")
logger.info(f"POSTGRES_DB: {POSTGRES_DB}")

# Validate environment variables
if not all([POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_DB]):
    missing = [k for k, v in {
        "POSTGRES_USER": POSTGRES_USER,
        "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
        "POSTGRES_HOST": POSTGRES_HOST,
        "POSTGRES_DB": POSTGRES_DB
    }.items() if not v]
    raise EnvironmentError(f"Missing environment variables: {missing}")

# Construct DATABASE_URL
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"

# Log DATABASE_URL for debugging (redacted password)
safe_db_url = DATABASE_URL.replace(POSTGRES_PASSWORD, "*****")
logger.info(f"DATABASE_URL: {safe_db_url}")

# Create engine and session
try:
    engine = create_engine(DATABASE_URL)
    logger.info("Database engine created successfully")
except Exception as e:
    logger.error(f"Failed to create database engine: {e}")
    raise

Base = declarative_base()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Function to create tables
def initialize_database():
    try:
        logger.info("Creating tables...")
        Base.metadata.create_all(engine)
        logger.info("Tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")

# Initialize database tables
initialize_database()

# S3 setup
s3_client = boto3.client(
    's3',
    endpoint_url=os.environ.get("S3_ENDPOINT"),
    aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
    aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
    region_name='us-east-1'
)
BUCKET_NAME = "file-processing"

# Ensure the bucket exists
try:
    s3_client.head_bucket(Bucket=BUCKET_NAME)
    logger.info(f"Bucket {BUCKET_NAME} exists")
except Exception as e:
    logger.info(f"Creating bucket {BUCKET_NAME}")
    try:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        logger.info(f"Bucket {BUCKET_NAME} created successfully")
    except Exception as e:
        logger.error(f"Failed to create bucket: {e}")

# RabbitMQ setup
def get_rabbitmq_connection():
    """
    Create and return a connection to RabbitMQ.
    """
    rabbitmq_url = os.environ.get("RABBITMQ_URL")
    if not rabbitmq_url:
        # Build it from individual environment variables
        rabbitmq_host = os.environ.get("RABBITMQ_HOST", "localhost")
        rabbitmq_user = os.environ.get("RABBITMQ_USER", "guest")
        rabbitmq_pass = os.environ.get("RABBITMQ_PASSWORD", "guest")
        rabbitmq_url = f"amqp://{rabbitmq_user}:{rabbitmq_pass}@{rabbitmq_host}:5672/%2F"
        logger.info(f"Constructed RabbitMQ URL from environment variables: {rabbitmq_host}")
    
    try:
        logger.info(f"Connecting to RabbitMQ using URL parameters (host: {rabbitmq_url.split('@')[1].split('/')[0]})")
        connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
        logger.info("RabbitMQ connection established")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise

# NFS path
NFS_PATH = os.environ.get("NFS_PATH", "/mnt/nfs_clientshare")

def process_uploaded_file(job_id: str, filename: str, temp_file_path: str, nfs_file_path: str):
    """
    Process an uploaded file by:
    1. Uploading to S3
    2. Publishing a message to RabbitMQ
    """
    try:
        # Upload to S3
        s3_key = f"{job_id}/{filename}"
        logger.info(f"Uploading file to S3 bucket {BUCKET_NAME}, key: {s3_key}")
        with open(temp_file_path, "rb") as file_data:
            s3_client.upload_fileobj(file_data, BUCKET_NAME, s3_key)
        
        # Publish message to RabbitMQ
        try:
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            
            # Ensure queue exists - passive=True to check if it exists without trying to change its properties
            try:
                channel.queue_declare(queue='file_processing', passive=True)
                logger.info("Queue 'file_processing' already exists")
            except pika.exceptions.ChannelClosedByBroker:
                # Reconnect if channel closed
                connection = get_rabbitmq_connection()
                channel = connection.channel()
                
                # Create queue if it doesn't exist
                logger.info("Creating queue 'file_processing' with durable=True")
                channel.queue_declare(queue='file_processing', durable=True)
            
            # Prepare message
            message = {
                "job_id": job_id,
                "filename": filename,
                "s3_key": s3_key,
                "nfs_path": nfs_file_path,
                "timestamp": datetime.now().isoformat()
            }
            
            # Publish message
            logger.info(f"Publishing message to RabbitMQ queue 'file_processing': {json.dumps(message)}")
            channel.basic_publish(
                exchange='',
                routing_key='file_processing',
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                )
            )
            
            logger.info(f"Message published successfully for job {job_id}")
            connection.close()
            
        except Exception as rabbitmq_error:
            logger.error(f"Failed to publish message to RabbitMQ: {rabbitmq_error}")
            raise
            
    except Exception as processing_error:
        logger.error(f"Error processing uploaded file: {processing_error}")
        # Update database status to "error"
        db = SessionLocal()
        try:
            file_record = db.query(FileRecord).filter(FileRecord.id == job_id).first()
            if file_record:
                file_record.status = "error"
                db.commit()
        except Exception as db_error:
            logger.error(f"Failed to update database: {db_error}")
        finally:
            db.close()
        raise

@app.post("/upload/")
async def upload_file(file: UploadFile, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    temp_file_path = f"/tmp/{job_id}_{file.filename}"
    
    logger.info(f"Processing file upload: {file.filename}, job_id: {job_id}")
    
    try:
        # Save uploaded file to temp location
        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        logger.info(f"File saved to temp location: {temp_file_path}")
        
        # Copy to NFS
        nfs_file_path = f"{NFS_PATH}/{job_id}_{file.filename}"
        shutil.copy(temp_file_path, nfs_file_path)
        logger.info(f"File copied to NFS: {nfs_file_path}")

        if os.path.exists(nfs_file_path):
            logger.info(f"File successfully written to NFS path: {nfs_file_path}")
        else:
            logger.warning(f"File not found in NFS path before RabbitMQ publish: {nfs_file_path}")

        # Create database record
        db = SessionLocal()
        file_record = FileRecord(
            id=job_id,
            filename=file.filename,
            s3_path=f"{BUCKET_NAME}/{job_id}/{file.filename}",
            nfs_path=nfs_file_path,
            status="uploaded",
            uploaded_at=datetime.now()
        )
        
        db.add(file_record)
        db.commit()
        db.close()
        logger.info(f"Database record created for job {job_id}")

        # Process file in background
        background_tasks.add_task(
            process_uploaded_file, 
            job_id, 
            file.filename, 
            temp_file_path, 
            nfs_file_path
        )
        
        return {"job_id": job_id, "status": "processing"}
    
    except Exception as e:
        logger.error(f"Error during file upload: {e}")
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/status/{job_id}")
async def get_status(job_id: str):
    logger.info(f"Getting status for job: {job_id}")
    db = SessionLocal()
    file_record = db.query(FileRecord).filter(FileRecord.id == job_id).first()
    if not file_record:
        logger.warning(f"Job not found: {job_id}")
        db.close()
        raise HTTPException(status_code=404, detail="Job not found")

    result = {
        "job_id": file_record.id,
        "filename": file_record.filename,
        "status": file_record.status,
        "uploaded_at": file_record.uploaded_at,
        "processed_at": file_record.processed_at
    }
    db.close()
    logger.info(f"Returning status for job {job_id}: {file_record.status}")
    return result

@app.get("/health")
async def health_check():
    logger.debug("Health check called")
    return {"status": "ok"}
