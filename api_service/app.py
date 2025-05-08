import os
import json
import uuid
import boto3
import pika
import shutil
from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from sqlalchemy import create_engine, Column, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from models import Base, FileRecord

app = FastAPI(title="File Processing API")

# Database setup
DATABASE_URL = os.environ.get("DATABASE_URL")
engine = create_engine(DATABASE_URL)
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

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
except:
    s3_client.create_bucket(Bucket=BUCKET_NAME)

# RabbitMQ setup
def get_rabbitmq_connection():
    connection = pika.BlockingConnection(
        pika.URLParameters(os.environ.get("RABBITMQ_URL"))
    )
    return connection

# NFS path
NFS_PATH = os.environ.get("NFS_MOUNT_PATH")

@app.post("/upload/")
async def upload_file(file: UploadFile, background_tasks: BackgroundTasks):
    # Create a unique ID for this file processing job
    job_id = str(uuid.uuid4())
    
    # Save the file temporarily
    temp_file_path = f"/tmp/{job_id}_{file.filename}"
    with open(temp_file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Upload to S3
    s3_key = f"{job_id}/{file.filename}"
    s3_client.upload_file(temp_file_path, BUCKET_NAME, s3_key)
    
    # Save to NFS
    nfs_file_path = f"{NFS_PATH}/{job_id}_{file.filename}"
    shutil.copy(temp_file_path, nfs_file_path)
    
    # Record in database
    db = SessionLocal()
    file_record = FileRecord(
        id=job_id,
        filename=file.filename,
        s3_path=s3_key,
        nfs_path=nfs_file_path,
        status="uploaded",
        uploaded_at=datetime.now()
    )
    db.add(file_record)
    db.commit()
    db.close()
    
    # Send message to RabbitMQ for processing
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    
    # Ensure queue exists
    channel.queue_declare(queue='file_processing')
    
    # Publish message
    message = {
        "job_id": job_id,
        "filename": file.filename,
        "s3_path": s3_key,
        "nfs_path": nfs_file_path
    }
    channel.basic_publish(
        exchange='',
        routing_key='file_processing',
        body=json.dumps(message)
    )
    connection.close()
    
    # Clean up temporary file
    os.remove(temp_file_path)
    
    return {"job_id": job_id, "status": "processing"}

@app.get("/status/{job_id}")
async def get_status(job_id: str):
    db = SessionLocal()
    file_record = db.query(FileRecord).filter(FileRecord.id == job_id).first()
    if not file_record:
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
    return result

@app.get("/health")
async def health_check():
    return {"status": "ok"}

# File: api_service/models.py
from sqlalchemy import Column, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base

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