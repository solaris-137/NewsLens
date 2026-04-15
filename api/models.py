import datetime

from sqlalchemy import Column, DateTime, Float, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Article(Base):
    __tablename__ = "articles"

    id = Column(String, primary_key=True)
    url = Column(Text, nullable=False)
    source = Column(String(50))
    title = Column(Text)
    summary = Column(Text)
    published_at = Column(DateTime)
    fetched_at = Column(DateTime)
    sentiment_label = Column(String(10))
    sentiment_score = Column(Float)
    sentiment_raw = Column(JSONB)
    category = Column(String(50))
    content_chars = Column(Integer)
    extractor_used = Column(String(20))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
