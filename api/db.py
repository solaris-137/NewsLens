import os

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

raw_url = os.environ["DATABASE_URL"].replace(
    "postgresql://",
    "postgresql+asyncpg://",
)
engine = create_async_engine(raw_url, pool_size=10, max_overflow=20)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
