import pytest
import os
import sys
import shutil
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from unittest.mock import patch, MagicMock
from datetime import datetime
import uuid

# Clear module cache to prevent stale imports
for module in list(sys.modules.keys()):
    if module.startswith("api_service"):
        del sys.modules[module]

# Add the project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import after setting up environment variables
from api_service.database import Base, get_db
from api_service.models import FileRecord

# Set up an in-memory SQLite database for testing
DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Override the database session in the app for testing
def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()

# Fixture for FastAPI test client
@pytest.fixture
def client():
    from api_service.app import app
    app.dependency_overrides[get_db] = override_get_db
    return TestClient(app)

# Create tables before tests
@pytest.fixture(scope="session", autouse=True)
def setup_database():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)

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

# Fixture to create temporary NFS directory
@pytest.fixture
def nfs_dir():
    nfs_path = "/tmp/nfs_test"
    os.makedirs(nfs_path, exist_ok=True)
    yield nfs_path
    shutil.rmtree(nfs_path, ignore_errors=True)

# Test 1: Test file upload endpoint
def test_upload_file(client, nfs_dir):
    # Mock S3 client and RabbitMQ connection
    with patch("api_service.app.s3_client") as mock_s3_client, patch(
        "api_service.app.get_rabbitmq_connection"
    ) as mock_rabbit:
        mock_s3_client.head_bucket.return_value = None
        mock_s3_client.upload_fileobj.return_value = None
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_rabbit.return_value = mock_connection

        # Create a test file
        test_file_content = b"test content"
        test_file = ("test.txt", test_file_content, "text/plain")

        # Send upload request
        response = client.post("/upload/", files={"file": test_file})

        # Assert response
        assert response.status_code == 200
        assert "job_id" in response.json()
        assert response.json()["status"] == "processing"

        # Verify database record
        db = TestingSessionLocal()
        job_id = response.json()["job_id"]
        file_record = db.query(FileRecord).filter(FileRecord.id == job_id).first()
        assert file_record is not None
        assert file_record.filename == "test.txt"
        assert file_record.status == "uploaded"
        assert file_record.s3_path == f"file-processing/{job_id}/test.txt"
        assert file_record.nfs_path.startswith(nfs_dir)
        assert file_record.uploaded_at is not None
        db.close()

# Test 2: Test status endpoint
def test_get_status(client):
    # Create a test record in the database
    db = TestingSessionLocal()
    job_id = str(uuid.uuid4())
    file_record = FileRecord(
        id=job_id,
        filename="test.txt",
        s3_path=f"file-processing/{job_id}/test.txt",
        nfs_path="/tmp/nfs_test/test.txt",
        status="processed",
        uploaded_at=datetime.now(),
        processed_at=datetime.now(),
    )
    db.add(file_record)
    db.commit()

    # Get status
    response = client.get(f"/status/{job_id}")

    # Assert response
    assert response.status_code == 200
    assert response.json()["job_id"] == job_id
    assert response.json()["filename"] == "test.txt"
    assert response.json()["status"] == "processed"
    assert response.json()["uploaded_at"] is not None
    assert response.json()["processed_at"] is not None
    db.close()

# Test 3: Test status endpoint with non-existent job
def test_get_status_not_found(client):
    response = client.get(f"/status/{str(uuid.uuid4())}")
    assert response.status_code == 404
    assert response.json()["detail"] == "Job not found"