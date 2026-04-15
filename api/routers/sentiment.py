from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cache import cache_get, cache_set
from db import get_db
from models import Article
from rate_limit import limiter

router = APIRouter()
CACHE_KEY = "sentiment:summary:24hr"


def _bucket_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


@router.get("/api/sentiment/summary")
@limiter.limit("100/minute")
async def get_sentiment_summary(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    cached = await cache_get(CACHE_KEY)
    if cached is not None:
        return cached

    since = datetime.utcnow() - timedelta(hours=24)
    result = await db.execute(
        select(
            Article.published_at,
            Article.sentiment_label,
            Article.sentiment_raw,
        )
        .where(Article.published_at.is_not(None), Article.published_at >= since)
        .order_by(Article.published_at.asc())
    )

    buckets: dict[datetime, dict] = {}
    for published_at, label, sentiment_raw in result.all():
        if published_at is None:
            continue

        hour = published_at.replace(minute=0, second=0, microsecond=0)
        bucket = buckets.setdefault(
            hour,
            {
                "hour": _bucket_iso(hour),
                "count": 0,
                "avg_composite": 0.0,
                "positive_count": 0,
                "negative_count": 0,
                "neutral_count": 0,
                "_composite_total": 0.0,
            },
        )

        payload = sentiment_raw or {}
        positive = float(payload.get("positive", 0.0))
        negative = float(payload.get("negative", 0.0))
        composite = positive - negative

        bucket["count"] += 1
        bucket["_composite_total"] += composite
        if label == "positive":
            bucket["positive_count"] += 1
        elif label == "negative":
            bucket["negative_count"] += 1
        else:
            bucket["neutral_count"] += 1

    response = []
    for hour in sorted(buckets):
        bucket = buckets[hour]
        count = bucket["count"] or 1
        bucket["avg_composite"] = round(bucket["_composite_total"] / count, 4)
        del bucket["_composite_total"]
        response.append(bucket)

    await cache_set(CACHE_KEY, response, 300)
    return response
