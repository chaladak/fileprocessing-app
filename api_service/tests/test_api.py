import pytest
import os
import sys
import json
import tempfile
import shutil
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

# Add the api_service directory to Python path so imports work
api_service_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if api_service_path not in sys.path:
    sys.path.insert(0, api_service_path)

# Set up test environment variables BEFORE importing the app
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
    os.environ["TESTING"] = "true"
    
    # Create NFS test directory
    os.makedirs("/tmp/nfs_test", exist_ok=True)
    
    yield
    
    # Clean up
    for key in [
        "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_DB",
        "S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "RABBITMQ_URL", "NFS_PATH", "TESTING"
    ]:
        os.environ.pop(key, None)
    
    # Clean up NFS test directory
    shutil.rmtree("/tmp/nfs_test", ignore_errors=True)

# Set up test database
DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()

@pytest.fixture
def client():
    # Mock external dependencies before importing the app
    with patch('boto3.client') as mock_boto3, \
         patch('pika.BlockingConnection') as mock_pika:
        
        # Setup S3 mock
        mock_s3 = MagicMock()
        mock_boto3.return_value = mock_s3
        
        # Setup RabbitMQ mock
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_pika.return_value = mock_connection
        
        # Now import the app after mocking
        from app import app
        from database import Base, get_db
        
        # Override the database dependency
        app.dependency_overrides[get_db] = override_get_db
        
        # Create tables
        Base.metadata.create_all(bind=engine)
        
        yield TestClient(app)
        
        # Clean up
        Base.metadata.drop_all(bind=engine)
        app.dependency_overrides.clear()

def test_upload_file(client):
    # Create a test file
    test_content = b"This is test file content"
    
    with patch('shutil.copy') as mock_copy, \
         patch('os.path.exists', return_value=True):
        
        response = client.post(
            "/upload/",
            files={"file": ("test.txt", test_content, "text/plain")}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "job_id" in data
        assert data["status"] == "processing"

def test_get_status(client):
    # First upload a file to create a record
    test_content = b"This is test file content"
    
    with patch('shutil.copy') as mock_copy, \
         patch('os.path.exists', return_value=True):
        
        upload_response = client.post(
            "/upload/",
            files={"file": ("test.txt", test_content, "text/plain")}
        )
        
        job_id = upload_response.json()["job_id"]
        
        # Now check the status
        status_response = client.get(f"/status/{job_id}")
        assert status_response.status_code == 200
        
        data = status_response.json()
        assert data["job_id"] == job_id
        assert data["filename"] == "test.txt"
        assert data["status"] == "uploaded"
        assert "uploaded_at" in data

def test_get_status_not_found(client):
    response = client.get("/status/nonexistent-job-id")
    assert response.status_code == 404
    assert response.json()["detail"] == "Job not found"

def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}