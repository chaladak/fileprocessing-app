import pytest
import os
import sys
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from unittest.mock import patch, MagicMock
from datetime import datetime
import uuid

# Clear module cache to prevent stale imports
for module in list(sys.modules.keys()):
    if module.startswith("processor_service") or module.startswith("api_service"):
        del sys.modules[module]

# Add the project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import after setting up environment variables
from api_service.database import Base, get_db
from api_service.models import FileRecord
from processor_service.processor import process_message

# Set up an in-memory SQLite database for testing
DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

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

# Fixture to set up database
@pytest.fixture(scope="session", autouse=True)
def setup_database():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)

# Fixture to provide database session
@pytest.fixture
def db_session():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()

# Test 1: Test process_message function
def test_process_message(db_session, nfs_dir):
    # Mock S3 client and file operations
    with patch("processor_service.processor.s3_client") as mock_s3_client, patch(
        "processor_service.processor.os.path.exists"
    ) as mock_exists:
        mock_s3_client.download_fileobj.return_value = None
        mock_exists.return_value = True

        # Create a test database record
        job_id = str(uuid.uuid4())
        file_record = FileRecord(
            id=job_id,
            filename="test.txt",
            s3_path=f"file-processing/{job_id}/test.txt",
            nfs_path=f"{nfs_dir}/{job_id}_test.txt",
            status="uploaded",
            uploaded_at=datetime.now(),
        )
        db_session.add(file_record)
        db_session.commit()

        # Create a test message
        message = {
            "job_id": job_id,
            "filename": "test.txt",
            "s3_key": f"{job_id}/test.txt",
            "nfs_path": f"{nfs_dir}/{job_id}_test.txt",
            "timestamp": datetime.now().isoformat()
        }

        # Mock RabbitMQ channel
        mock_channel = MagicMock()

        # Call process_message
        process_message(mock_channel, None, None, json.dumps(message))

        # Verify database update
        updated_record = db_session.query(FileRecord).filter(FileRecord.id == job_id).first()
        assert updated_record is not None
        assert updated_record.status == "processed"
        assert updated_record.processed_at is not None
        assert updated_record.processing_result == "Processed successfully"
        assert mock_s3_client.download_fileobj.called

