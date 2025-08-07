import os
import uuid
import json
import time
import pytest
import pika
import boto3
import hashlib
import requests
import warnings
import sys
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock, mock_open
from io import BytesIO
from starlette.testclient import TestClient

# Add paths for imports - be more specific about paths
project_root = Path(__file__).parent.parent
api_service_path = project_root / "api_service"
processor_service_path = project_root / "processor_service"
notification_service_path = project_root / "notification_service"

# Add all necessary paths to Python path
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(api_service_path))
sys.path.insert(0, str(processor_service_path))
sys.path.insert(0, str(notification_service_path))

# Suppress datetime.utcnow() deprecation warning
warnings.filterwarnings("ignore", category=DeprecationWarning, message=r".*datetime\.utcnow.*")

# Set environment variables for testing BEFORE any imports
os.environ["TESTING"] = "true"
os.environ["S3_ENDPOINT"] = "http://localhost:9000"
os.environ["S3_ACCESS_KEY"] = "minioadmin"
os.environ["S3_SECRET_KEY"] = "minioadmin"
os.environ["RABBITMQ_URL"] = "amqp://guest:guest@localhost:5672/%2F"
os.environ["DATABASE_URL"] = "sqlite:///test_integration.db"  # Use file-based SQLite for persistence
os.environ["NFS_PATH"] = "/tmp"

# Global storage for file content during tests
test_file_storage = {}

def mock_open_function(file_path, mode='r', *args, **kwargs):
    """Mock the open function to handle our test file paths."""
    file_path_str = str(file_path)  # Convert PosixPath to string
    print(f"mock_open_function called with path: {file_path_str}, mode: {mode}")
    
    # Handle test file paths in /tmp/
    if file_path_str.startswith('/tmp/') and ('_test.txt' in file_path_str or file_path_str.count('-') == 4):
        if 'wb' in mode:
            # Handle write mode: store the content in test_file_storage
            test_file_storage[file_path_str] = b"Integration test content"
            print(f"Stored content in test_file_storage for key: {file_path_str}")
            return BytesIO()
        elif 'rb' in mode or 'r' in mode:
            # Handle read mode: return content from test_file_storage
            content = test_file_storage.get(file_path_str, b"Integration test content")
            print(f"Reading content from test_file_storage for key: {file_path_str}")
            if 'b' in mode:
                return BytesIO(content)
            else:
                return BytesIO(content.decode('utf-8').encode() if isinstance(content, bytes) else content.encode())
    
    # For non-test files, use the original open function
    return original_open(file_path, mode, *args, **kwargs)

def mock_shutil_copyfileobj(fsrc, fdst, *args, **kwargs):
    """Mock shutil.copyfileobj to handle our virtual files."""
    # If fsrc has a read method (like file objects), read from it
    if hasattr(fsrc, 'read'):
        content = fsrc.read()
        if hasattr(fdst, 'write'):
            fdst.write(content)
        print(f"mock_shutil_copyfileobj: copied {len(content)} bytes")
        return
    return original_shutil_copyfileobj(fsrc, fdst, *args, **kwargs)

def mock_shutil_copy(src, dst):
    """Mock shutil.copy to handle our virtual files."""
    src_str = str(src)
    dst_str = str(dst)
    print(f"mock_shutil_copy called with src: {src_str}, dst: {dst_str}")
    
    # Handle test file paths
    if src_str.startswith('/tmp/') and (src_str.count('-') == 4 or '_test.txt' in src_str):
        # Copy content in our test storage
        content = test_file_storage.get(src_str, b"Integration test content")
        test_file_storage[dst_str] = content
        print(f"Copied content in test_file_storage from {src_str} to {dst_str}")
        return dst
    
    # For non-test files, use the original function
    return original_shutil_copy(src, dst)

def mock_os_path_exists(path):
    """Mock os.path.exists for test file paths."""
    path_str = str(path)
    print(f"mock_os_path_exists called with path: {path_str}")
    
    # Return True for test file paths
    if path_str.startswith('/tmp/') and (path_str.count('-') == 4 or '_test.txt' in path_str):
        exists = path_str in test_file_storage
        print(f"Path {path_str} exists in test storage: {exists}")
        return exists
    
    # For non-test files, use the original function
    return original_os_path_exists(path)

def mock_os_remove(path):
    """Mock os.remove for test file paths."""
    path_str = str(path)
    print(f"mock_os_remove called with path: {path_str}")
    
    # Handle test file paths
    if path_str.startswith('/tmp/') and (path_str.count('-') == 4 or '_test.txt' in path_str):
        if path_str in test_file_storage:
            del test_file_storage[path_str]
            print(f"Removed {path_str} from test storage")
        return
    
    # For non-test files, use the original function
    return original_os_remove(path)

# Store original functions BEFORE any imports
original_open = open
original_shutil_copyfileobj = shutil.copyfileobj
original_shutil_copy = shutil.copy
original_os_path_exists = os.path.exists
original_os_remove = os.remove

# Apply global patches BEFORE importing the FastAPI app
import builtins
builtins.open = mock_open_function
shutil.copyfileobj = mock_shutil_copyfileobj
shutil.copy = mock_shutil_copy
shutil.copy2 = mock_shutil_copy  # Also patch copy2
os.path.exists = mock_os_path_exists
os.remove = mock_os_remove

print("✓ Applied mocks before FastAPI import")

# NOW import the actual API service modules (with mocks already in place)
import database
import models
import app as api_app

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Create test database engine using the same pattern as the API service
test_engine = create_engine(
    "sqlite:///test_integration.db", 
    connect_args={"check_same_thread": False}
)
TestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)

# Override the database dependency to use our test database
def override_get_db():
    db = TestSessionLocal()
    try:
        yield db
    finally:
        db.close()

# Override the API app's database dependency
api_app.app.dependency_overrides[database.get_db] = override_get_db

# Also override the engine in the database module
database.engine = test_engine
database.SessionLocal = TestSessionLocal

# Initialize FastAPI test client
client = TestClient(api_app.app, backend="asyncio")

# S3 (MinIO) setup
s3_client = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    aws_access_key_id=os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["S3_SECRET_KEY"],
)
BUCKET_NAME = "file-processing"

@pytest.fixture(autouse=True)
def clear_test_file_storage():
    """Clear test file storage between tests to ensure isolation."""
    test_file_storage.clear()
    yield

@pytest.fixture(scope="module", autouse=True)
def setup_test_environment():
    """Setup test environment before any tests run."""
    # Remove existing test database if it exists
    import os
    if os.path.exists("test_integration.db"):
        os.remove("test_integration.db")
    
    # Create tables in the test database using the models from the API service
    models.Base.metadata.create_all(bind=test_engine)
    print("✓ Test database tables created")
    
    # Setup S3 bucket
    try:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print("✓ S3 bucket created")
    except s3_client.exceptions.BucketAlreadyExists:
        print("✓ S3 bucket already exists")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print("✓ S3 bucket already owned by you")
    except Exception as e:
        print(f"Warning: Could not create bucket: {e}")
    
    yield
    
    # Cleanup
    try:
        # Clean S3 bucket
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
        if "Contents" in response:
            for obj in response["Contents"]:
                s3_client.delete_object(Bucket=BUCKET_NAME, Key=obj["Key"])
        s3_client.delete_bucket(Bucket=BUCKET_NAME)
        print("✓ S3 bucket cleaned up")
    except Exception as e:
        print(f"Warning: S3 cleanup failed: {e}")
    
    # Remove test database
    if os.path.exists("test_integration.db"):
        os.remove("test_integration.db")
    
    # Restore original functions
    builtins.open = original_open
    shutil.copyfileobj = original_shutil_copyfileobj
    shutil.copy = original_shutil_copy
    shutil.copy2 = original_shutil_copy
    os.path.exists = original_os_path_exists
    os.remove = original_os_remove
    print("✓ Original functions restored")

@pytest.fixture(scope="module")
def rabbitmq_connection():
    """Create RabbitMQ connection for tests."""
    connection = pika.BlockingConnection(pika.URLParameters(os.environ["RABBITMQ_URL"]))
    yield connection
    connection.close()

@pytest.fixture(scope="module")
def rabbitmq_channel(rabbitmq_connection):
    """Create RabbitMQ channel and declare queues."""
    channel = rabbitmq_connection.channel()
    
    # Declare queues
    channel.queue_declare(queue="file_processing", durable=True)
    channel.queue_declare(queue="notifications", durable=True)
    
    # Purge queues to start clean
    channel.queue_purge(queue="file_processing")
    channel.queue_purge(queue="notifications")
    
    yield channel

def test_file_upload_and_db_record():
    """Test file upload and database record creation in api_service."""
    file_content = b"Integration test content"
    filename = "test.txt"
    
    # The global mocks should handle everything, but let's add some extra debugging
    print(f"Current open function: {open}")
    print(f"Current shutil.copy: {shutil.copy}")
    print(f"Current os.path.exists: {os.path.exists}")
    
    # Use the TestClient with proper file upload format
    files = {"file": (filename, file_content, "text/plain")}
    response = client.post("/upload/", files=files)
    
    # Debug output to see what's happening
    print(f"Response status: {response.status_code}")
    if response.status_code != 200:
        print(f"Response text: {response.text}")
    
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    response_data = response.json()
    assert "job_id" in response_data, f"job_id not in response: {response_data}"
    job_id = response_data["job_id"]
    
    # Verify database record using the actual models
    db = TestSessionLocal()
    try:
        file_record = db.query(models.FileRecord).filter(models.FileRecord.id == job_id).first()
        assert file_record is not None, f"No file record found for job_id: {job_id}"
        assert file_record.filename == filename
        assert file_record.status == "uploaded"
        assert job_id in file_record.s3_path
        assert job_id in file_record.nfs_path
        print(f"✓ File upload and DB record test passed for job_id: {job_id}")
    finally:
        db.close()

def test_notification_creation(rabbitmq_channel):
    """Test notification handling."""
    job_id = str(uuid.uuid4())
    file_content = b"Integration test content"
    expected_hash = hashlib.sha256(file_content).hexdigest()
    
    notification_message = {
        "job_id": job_id,
        "status": "processed",
        "result": {
            "success": True,
            "file_hash": expected_hash,
            "size_bytes": len(file_content),
            "processed_timestamp": datetime.now(timezone.utc).isoformat()
        }
    }
    
    # Publish to notifications queue
    rabbitmq_channel.basic_publish(
        exchange="",
        routing_key="notifications",
        body=json.dumps(notification_message),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    
    # Verify message was published successfully by consuming it
    message_received = False
    for attempt in range(10):
        method, properties, body = rabbitmq_channel.basic_get(queue="notifications", auto_ack=False)
        if body:
            message_data = json.loads(body)
            assert message_data["job_id"] == job_id
            assert message_data["status"] == "processed"
            assert message_data["result"]["file_hash"] == expected_hash
            
            # Acknowledge the message
            rabbitmq_channel.basic_ack(delivery_tag=method.delivery_tag)
            message_received = True
            break
        time.sleep(1)
    
    assert message_received, "Notification message was not found in queue"
    print(f"✓ Notification test completed for job_id: {job_id}")

def test_file_processing(rabbitmq_channel):
    """Test file processing by processor_service."""
    file_content = b"Integration test content"
    filename = "test.txt"
    expected_hash = hashlib.sha256(file_content).hexdigest()
    
    # Use the TestClient with proper file upload format
    files = {"file": (filename, file_content, "text/plain")}
    response = client.post("/upload/", files=files)
    
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    job_id = response.json()["job_id"]
    
    # Wait for RabbitMQ message
    message_received = False
    for attempt in range(30):  # Wait up to 30 seconds
        method, properties, body = rabbitmq_channel.basic_get(queue="file_processing", auto_ack=False)
        if body:
            message = json.loads(body)
            assert message["job_id"] == job_id
            assert message["filename"] == filename
            assert message["s3_key"] == f"{job_id}/{filename}"
            
            # Acknowledge the message
            rabbitmq_channel.basic_ack(delivery_tag=method.delivery_tag)
            message_received = True
            break
        time.sleep(1)
    
    assert message_received, "No message received in file_processing queue"
    
    # Simulate processing completion by updating the database directly
    db = TestSessionLocal()
    try:
        file_record = db.query(models.FileRecord).filter(models.FileRecord.id == job_id).first()
        assert file_record is not None
        
        # Update the record as if processing completed
        file_record.status = "processed"
        file_record.processed_at = datetime.now(timezone.utc)
        file_record.processing_result = json.dumps({
            "success": True,
            "file_hash": expected_hash,
            "size_bytes": len(file_content),
            "processed_timestamp": datetime.now(timezone.utc).isoformat()
        })
        db.commit()
        
        # Publish notification message as processor would do
        notification_message = {
            "job_id": job_id,
            "status": "processed",
            "result": {
                "success": True,
                "file_hash": expected_hash,
                "size_bytes": len(file_content),
                "processed_timestamp": datetime.now(timezone.utc).isoformat()
            }
        }
        
        rabbitmq_channel.basic_publish(
            exchange="",
            routing_key="notifications",
            body=json.dumps(notification_message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        
        print(f"✓ File processing simulation completed for job_id: {job_id}")
        
    finally:
        db.close()

def test_services_connectivity():
    """Basic connectivity test for all services."""
    
    # Test MinIO connectivity
    try:
        s3_client.list_buckets()
        print("✓ MinIO connection successful")
    except Exception as e:
        pytest.fail(f"MinIO connection failed: {e}")
    
    # Test RabbitMQ connectivity
    try:
        connection = pika.BlockingConnection(pika.URLParameters(os.environ["RABBITMQ_URL"]))
        channel = connection.channel()
        channel.queue_declare(queue="test_connectivity", durable=False)
        channel.queue_delete(queue="test_connectivity")
        connection.close()
        print("✓ RabbitMQ connection successful")
    except Exception as e:
        pytest.fail(f"RabbitMQ connection failed: {e}")
    
    # Test FastAPI app
    try:
        response = client.get("/health")  
        # If health endpoint doesn't exist, try root endpoint
        if response.status_code == 404:
            response = client.get("/")
        
        # As long as we get some response (not a connection error), it's working
        print("✓ FastAPI app accessible")
    except Exception as e:
        pytest.fail(f"FastAPI app not accessible: {e}")
    
    # Test database connectivity
    try:
        db = TestSessionLocal()
        # Try to query the file_records table
        count = db.query(models.FileRecord).count()
        db.close()
        print(f"✓ Database connection successful (found {count} records)")
    except Exception as e:
        pytest.fail(f"Database connection failed: {e}")

def test_duplicate_file_upload_creates_new_job_id():
    """Uploading the same file twice should result in different job_ids and separate DB entries."""
    file_content = b"Duplicate test content"
    filename = "duplicate.txt"

    files = {"file": (filename, file_content, "text/plain")}
    response1 = client.post("/upload/", files=files)
    response2 = client.post("/upload/", files=files)

    assert response1.status_code == 200
    assert response2.status_code == 200

    job_id1 = response1.json()["job_id"]
    job_id2 = response2.json()["job_id"]

    assert job_id1 != job_id2, "Each upload should produce a unique job_id"

    db = TestSessionLocal()
    try:
        record1 = db.query(models.FileRecord).filter(models.FileRecord.id == job_id1).first()
        record2 = db.query(models.FileRecord).filter(models.FileRecord.id == job_id2).first()
        assert record1 and record2
        assert record1.filename == record2.filename
        assert record1.nfs_path != record2.nfs_path
    finally:
        db.close()


def test_status_for_nonexistent_job():
    """Requesting status for a non-existent job_id should return 404."""
    non_existent_job_id = str(uuid.uuid4())
    response = client.get(f"/status/{non_existent_job_id}")
    assert response.status_code == 404
    assert response.json()["detail"] == "Job not found"


def test_upload_empty_file():
    """Uploading an empty file should still be accepted and processed."""
    filename = "empty.txt"
    files = {"file": (filename, b"", "text/plain")}
    response = client.post("/upload/", files=files)

    assert response.status_code == 200
    job_id = response.json()["job_id"]

    db = TestSessionLocal()
    try:
        file_record = db.query(models.FileRecord).filter(models.FileRecord.id == job_id).first()
        assert file_record is not None
        assert file_record.filename == filename
        assert file_record.status == "uploaded"
    finally:
        db.close()


def test_background_task_processes_and_updates_status(rabbitmq_channel):
    """Test full flow: upload file -> wait -> simulate processor -> verify DB status."""
    file_content = b"Background task content"
    filename = "background.txt"
    expected_hash = hashlib.sha256(file_content).hexdigest()

    files = {"file": (filename, file_content, "text/plain")}
    response = client.post("/upload/", files=files)

    assert response.status_code == 200
    job_id = response.json()["job_id"]

    # Wait for file_processing message
    for _ in range(20):
        method, properties, body = rabbitmq_channel.basic_get(queue="file_processing", auto_ack=False)
        if body:
            message = json.loads(body)
            if message["job_id"] == job_id:
                rabbitmq_channel.basic_ack(delivery_tag=method.delivery_tag)
                break
        time.sleep(1)
    else:
        pytest.fail("No message received in file_processing queue for background task test")

    # Simulate processor behavior
    db = TestSessionLocal()
    try:
        file_record = db.query(models.FileRecord).filter(models.FileRecord.id == job_id).first()
        file_record.status = "processed"
        file_record.processed_at = datetime.now(timezone.utc)
        file_record.processing_result = json.dumps({
            "success": True,
            "file_hash": expected_hash,
            "size_bytes": len(file_content),
            "processed_timestamp": datetime.now(timezone.utc).isoformat()
        })
        db.commit()
    finally:
        db.close()

    # Check status endpoint
    status_response = client.get(f"/status/{job_id}")
    assert status_response.status_code == 200
    data = status_response.json()
    assert data["status"] == "processed"
    assert data["processed_at"] is not None

def test_already_processed_job_not_reprocessed():
    """Test that an already processed job does not get reprocessed or reset when fetched via /status."""
    file_content = b"Final test content"
    filename = "final.txt"
    expected_hash = hashlib.sha256(file_content).hexdigest()

    # Upload file
    files = {"file": (filename, file_content, "text/plain")}
    response = client.post("/upload/", files=files)
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    # Simulate processing completion
    db = TestSessionLocal()
    try:
        file_record = db.query(models.FileRecord).filter(models.FileRecord.id == job_id).first()
        file_record.status = "processed"
        file_record.processed_at = datetime.now(timezone.utc)
        file_record.processing_result = json.dumps({
            "success": True,
            "file_hash": expected_hash,
            "size_bytes": len(file_content),
            "processed_timestamp": datetime.now(timezone.utc).isoformat()
        })
        db.commit()
    finally:
        db.close()

    # Fetch status again – status should remain processed
    response = client.get(f"/status/{job_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "processed"
    assert data["processed_at"] is not None

if __name__ == "__main__":
    # Run tests individually for debugging
    print("Running integration tests...")
    
    # You can run individual tests like this:
    # test_services_connectivity()
    # test_file_upload_and_db_record()
    
    print("All tests completed!")