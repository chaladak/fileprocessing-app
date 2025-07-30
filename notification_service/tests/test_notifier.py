import pytest
import os
import sys
import json
import importlib.util
import importlib
from unittest.mock import patch, MagicMock
from datetime import datetime
import uuid
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

# Clear module cache to prevent premature imports
for module in list(sys.modules.keys()):
    if module.startswith("notification_service") or module == "notifier":
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

# Function to dynamically import the notifier module
def import_notifier():
    """Try to import the notifier module from various possible locations"""
    possible_imports = [
        'notification_service.notifier',
        'notifier'
    ]
    
    # Set environment variables before any import
    env_vars = {
        'POSTGRES_USER': 'test',
        'POSTGRES_PASSWORD': 'test',
        'POSTGRES_HOST': 'localhost',
        'POSTGRES_DB': 'test',
        'RABBITMQ_URL': 'amqp://guest:guest@localhost:5672/%2F',
        'TESTING': 'true'  # Enable testing mode
    }
    
    with patch.dict('os.environ', env_vars):
        # Clear module cache again to ensure clean import
        for module in list(sys.modules.keys()):
            if module.startswith("notification_service") or module == "notifier":
                del sys.modules[module]
        
        # Try standard imports first
        for import_path in possible_imports:
            try:
                module = importlib.import_module(import_path)
                # Ensure module is reloaded with test environment
                importlib.reload(module)
                return module
            except ImportError:
                continue
        
        # Try direct file imports
        possible_paths = [
            os.path.join(os.path.dirname(__file__), '..', 'notifier.py'),
            os.path.join(project_root, 'notifier.py'),
            os.path.join(project_root, 'notification_service', 'notifier.py'),
        ]
        
        for notifier_path in possible_paths:
            if os.path.exists(notifier_path):
                spec = importlib.util.spec_from_file_location("notifier", notifier_path)
                notifier_module = importlib.util.module_from_spec(spec)
                sys.modules["notifier"] = notifier_module
                spec.loader.exec_module(notifier_module)
                return notifier_module
    
    raise ImportError("Could not find notifier module in any expected location")

# Fixture to set up environment variables
@pytest.fixture(autouse=True)
def setup_env():
    env_vars = {
        "POSTGRES_USER": "test",
        "POSTGRES_PASSWORD": "test",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_DB": "test",
        "RABBITMQ_URL": "amqp://guest:guest@localhost:5672/%2F",
        "TESTING": "true"
    }
    with patch.dict('os.environ', env_vars):
        # Clear module cache in fixture to ensure clean state
        for module in list(sys.modules.keys()):
            if module.startswith("notification_service") or module == "notifier":
                del sys.modules[module]
        yield

# Import notifier module once for the test session
notifier_module = None

@pytest.fixture(scope="session", autouse=True)
def setup_notifier():
    global notifier_module
    notifier_module = import_notifier()
    
    # Create tables using the imported Base
    notifier_module.Base.metadata.create_all(bind=engine)
    yield
    notifier_module.Base.metadata.drop_all(bind=engine)

# Test 1: Test successful notification sending
def test_send_notification_success():
    # Mock the SessionLocal in the notifier module
    original_session_local = notifier_module.SessionLocal
    notifier_module.SessionLocal = TestingSessionLocal
    
    try:
        # Set up test data
        job_id = str(uuid.uuid4())
        status = "processed"
        result = {
            "size_bytes": 1024,
            "processed_timestamp": datetime.now().isoformat(),
            "file_hash": "abc123def456",
            "success": True
        }
        
        # Send notification
        success = notifier_module.send_notification(job_id, status, result)
        
        # Assert notification was sent successfully
        assert success is True
        
        # Verify notification was saved to database
        db = TestingSessionLocal()
        notifications = db.query(notifier_module.Notification).filter(
            notifier_module.Notification.job_id == job_id
        ).all()
        assert len(notifications) == 1
        
        notification = notifications[0]
        assert notification.job_id == job_id
        assert notification.status == status
        assert notification.sent_at is not None
        assert notification.details is not None
        
        # Verify the result details were stored correctly
        stored_result = json.loads(notification.details)
        assert stored_result == result
        
        db.close()
        
    finally:
        # Restore original SessionLocal
        notifier_module.SessionLocal = original_session_local

# Test 2: Test notification callback processing
def test_callback_success():
    # Mock the SessionLocal in the notifier module
    original_session_local = notifier_module.SessionLocal
    notifier_module.SessionLocal = TestingSessionLocal
    
    try:
        # Mock channel and method
        mock_channel = MagicMock()
        mock_method = MagicMock()
        mock_method.delivery_tag = "test_delivery_tag"
        
        # Set up test message
        job_id = str(uuid.uuid4())
        message = {
            "job_id": job_id,
            "status": "processed",
            "result": {
                "size_bytes": 2048,
                "success": True,
                "file_hash": "xyz789abc123"
            }
        }
        body = json.dumps(message).encode()
        
        # Process the callback
        notifier_module.callback(mock_channel, mock_method, None, body)
        
        # Verify message was acknowledged
        mock_channel.basic_ack.assert_called_once_with(delivery_tag="test_delivery_tag")
        
        # Verify notification was created in database
        db = TestingSessionLocal()
        notifications = db.query(notifier_module.Notification).filter(
            notifier_module.Notification.job_id == job_id
        ).all()
        assert len(notifications) == 1
        
        notification = notifications[0]
        assert notification.job_id == job_id
        assert notification.status == "processed"
        
        stored_result = json.loads(notification.details)
        assert stored_result == message["result"]
        
        db.close()
        
    finally:
        # Restore original SessionLocal
        notifier_module.SessionLocal = original_session_local
