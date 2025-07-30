import pytest
import os
import sys
import json
import tempfile
import shutil
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime
import uuid
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

# Clear module cache to prevent stale imports
for module in list(sys.modules.keys()):
    if module.startswith("api_service") or module.startswith("processor_service"):
        del sys.modules[module]

# Add the project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Set up an in-memory SQLite database for testing
DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Import after setting up environment variables
from api_service.database import Base
from api_service.models import FileRecord

# Fixture to set up environment variables
@pytest.fixture(autouse=True)
def setup_env():
    os.environ["POSTGRES_USER"] = "test"
    os.environ["POSTGRES_PASSWORD"] = "test"
    os.environ["POSTGRES_HOST"] = "localhost"
    os.environ["POSTGRES_DB"] = "test"
    os.environ["S3_ENDPOINT"] = "http://localhost:9000"
    os.environ["S3_ACCESS_KEY"] = "test_key"
    os.environ["S3_SECRET_KEY"] = "test_secret"
    os.environ["RABBITMQ_URL"] = "amqp://guest:guest@localhost:5672/%2F"
    os.environ["NFS_PATH"] = "/tmp/nfs_test"
    os.environ["TESTING"] = "true"  # Ensure TESTING is set
    yield
    # Clean up environment variables
    for key in [
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_HOST",
        "POSTGRES_DB",
        "S3_ENDPOINT",
        "S3_ACCESS_KEY",
        "S3_SECRET_KEY",
        "RABBITMQ_URL",
        "NFS_PATH",
        "TESTING",
    ]:
        os.environ.pop(key, None)

# Create tables before tests
@pytest.fixture(scope="session", autouse=True)
def setup_database():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)

# Fixture to create temporary NFS directory and test files
@pytest.fixture
def temp_files():
    temp_dir = tempfile.mkdtemp()
    test_file_path = os.path.join(temp_dir, "test_file.txt")
    test_content = b"This is test file content for processing"
    
    with open(test_file_path, "wb") as f:
        f.write(test_content)
    
    yield {
        "temp_dir": temp_dir,
        "test_file_path": test_file_path,
        "test_content": test_content
    }
    
    shutil.rmtree(temp_dir, ignore_errors=True)

# Test 1: Test successful file processing
def test_process_file_success(temp_files):
    # Mock all the external dependencies
    with patch('pika.BlockingConnection') as mock_pika, \
         patch('boto3.client') as mock_boto3:
        
        # Setup mocks
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_pika.return_value = mock_connection
        
        # Mock the SessionLocal in the processor module
        with patch('processor_service.processor.SessionLocal', TestingSessionLocal):
            # Import processor after mocking
            from processor_service import processor
            
            # Set up test data
            job_id = str(uuid.uuid4())
            filename = "test_file.txt"
            nfs_path = temp_files["test_file_path"]
            
            # Create initial file record in database
            db = TestingSessionLocal()
            file_record = FileRecord(
                id=job_id,
                filename=filename,
                s3_path=f"file-processing/{job_id}/{filename}",
                nfs_path=nfs_path,
                status="uploaded",
                uploaded_at=datetime.now()
            )
            db.add(file_record)
            db.commit()
            db.close()
            
            # Create message
            message = {
                "job_id": job_id,
                "filename": filename,
                "nfs_path": nfs_path
            }
            
            # Process the file
            processor.process_file(message)
            
            # Verify database was updated
            db = TestingSessionLocal()
            updated_record = db.query(FileRecord).filter(FileRecord.id == job_id).first()
            assert updated_record is not None
            assert updated_record.status == "processed"
            assert updated_record.processed_at is not None
            assert updated_record.processing_result is not None
            
            # Verify processing result
            result = json.loads(updated_record.processing_result)
            assert result["success"] is True
            assert result["size_bytes"] == len(temp_files["test_content"])
            assert "file_hash" in result
            assert "processed_timestamp" in result
            
            db.close()

# Test 2: Test file processing with missing file
def test_process_file_missing_file():
    with patch('pika.BlockingConnection') as mock_pika, \
         patch('boto3.client') as mock_boto3:
        
        # Setup mocks
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_pika.return_value = mock_connection
        
        with patch('processor_service.processor.SessionLocal', TestingSessionLocal):
            from processor_service import processor
            
            # Set up test data
            job_id = str(uuid.uuid4())
            filename = "missing_file.txt"
            nfs_path = "/nonexistent/path/missing_file.txt"
            
            # Create initial file record in database
            db = TestingSessionLocal()
            file_record = FileRecord(
                id=job_id,
                filename=filename,
                s3_path=f"file-processing/{job_id}/{filename}",
                nfs_path=nfs_path,
                status="uploaded",
                uploaded_at=datetime.now()
            )
            db.add(file_record)
            db.commit()
            db.close()
            
            # Create message
            message = {
                "job_id": job_id,
                "filename": filename,
                "nfs_path": nfs_path
            }
            
            # Process the file (should raise exception)
            with pytest.raises(Exception):
                processor.process_file(message)
            
            # Verify database was updated with error status
            db = TestingSessionLocal()
            updated_record = db.query(FileRecord).filter(FileRecord.id == job_id).first()
            assert updated_record is not None
            assert updated_record.status == "error"
            db.close()

# Test 3: Test compute_file_hash function
def test_compute_file_hash(temp_files):
    with patch('pika.BlockingConnection'), \
         patch('boto3.client'):
        
        with patch('processor_service.processor.SessionLocal', TestingSessionLocal):
            from processor_service.processor import compute_file_hash
            import hashlib
            
            # Compute hash using the function
            computed_hash = compute_file_hash(temp_files["test_file_path"])
            
            # Compute expected hash manually
            expected_hash = hashlib.sha256(temp_files["test_content"]).hexdigest()
            
            assert computed_hash == expected_hash