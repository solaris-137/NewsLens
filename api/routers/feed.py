from datetime import timezone
from typing import Literal

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_db
from models import Article
from monitoring import articles_processed_measure, record_metric
from rate_limit import limiter

router = APIRouter()

CategoryParam = Literal[
    "earnings",
    "products",
    "legal",
    "regulatory",
    "macroeconomic",
    "competition",
    "executive",
    "supply_chain",
    "general",
]
SentimentParam = Literal["positive", "negative", "neutral"]


def _to_iso(value):
    if value is None:
        return None
    if getattr(value, "tzinfo", None) is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def _serialize_article(article: Article) -> dict:
    return {
        "id": article.id,
        "url": article.url,
        "source": article.source,
        "title": article.title,
        "summary": article.summary,
        "published_at": _to_iso(article.published_at),
        "sentiment_label": article.sentiment_label,
        "sentiment_score": article.sentiment_score,
        "sentiment_raw": article.sentiment_raw,
        "category": article.category,
    }


@router.get("/api/feed")
@limiter.limit("100/minute")
async def get_feed(
    request: Request,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    category: CategoryParam | None = None,
    sentiment: SentimentParam | None = None,
    db: AsyncSession = Depends(get_db),
):
    filters = []
    if category is not None:
        filters.append(Article.category == category)
    if sentiment is not None:
        filters.append(Article.sentiment_label == sentiment)

    total_query = select(func.count()).select_from(Article).where(*filters)
    total_result = await db.execute(total_query)
    total = int(total_result.scalar() or 0)

    articles_query = (
        select(Article)
        .where(*filters)
        .order_by(Article.published_at.desc())
        .offset(offset)
        .limit(limit)
    )
    article_result = await db.execute(articles_query)
    articles = article_result.scalars().all()
    payload = [_serialize_article(article) for article in articles]
    record_metric(articles_processed_measure, len(payload))

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "articles": payload,
    }
