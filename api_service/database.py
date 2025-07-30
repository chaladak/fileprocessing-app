import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import StaticPool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Base = declarative_base()

# Database setup
if os.getenv("TESTING", "false").lower() == "true":
    DATABASE_URL = "sqlite:///:memory:"
    logger.info("Using SQLite in-memory database for testing")
else:
    DATABASE_URL = os.environ.get("DATABASE_URL")
    if not DATABASE_URL:
        pg_user = os.environ.get("POSTGRES_USER")
        pg_pass = os.environ.get("POSTGRES_PASSWORD")
        pg_host = os.environ.get("POSTGRES_HOST")
        pg_db = os.environ.get("POSTGRES_DB")
        if all([pg_user, pg_pass, pg_host, pg_db]):
            DATABASE_URL = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:5432/{pg_db}"
            logger.info(f"Constructed DATABASE_URL from environment variables")
        else:
            logger.error("Missing environment variables for PostgreSQL")
            raise ValueError("DATABASE_URL or PostgreSQL environment variables not set")

logger.info(f"Using database URL: {DATABASE_URL}")
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {},
    poolclass=StaticPool if "sqlite" in DATABASE_URL else None,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()