from datetime import datetime

from fastapi import APIRouter, Depends, Request
from fastapi.responses import PlainTextResponse
from prometheus_client import Gauge, REGISTRY, generate_latest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from cache import redis_client
from db import get_db
from rate_limit import limiter

router = APIRouter()


def _get_or_create_gauge(name: str, description: str) -> Gauge:
    existing = REGISTRY._names_to_collectors.get(name)
    if existing is not None:
        return existing
    return Gauge(name, description)


articles_processed = _get_or_create_gauge(
    "articles_processed_total",
    "Articles processed today",
)
articles_filtered = _get_or_create_gauge(
    "articles_filtered_total",
    "Articles filtered today",
)
avg_sentiment = _get_or_create_gauge(
    "avg_sentiment_24hr",
    "Avg composite sentiment last 24hr",
)
pipeline_latency = _get_or_create_gauge(
    "pipeline_latency_ms",
    "Latest pipeline latency in ms",
)


@router.get("/api/metrics", response_class=PlainTextResponse)
@limiter.limit("100/minute")
async def metrics_endpoint(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    articles_processed.set(int(await redis_client.get(f"stats:nlp_processed:{today}") or 0))
    articles_filtered.set(int(await redis_client.get(f"stats:nlp_filtered:{today}") or 0))

    result = await db.execute(
        text(
            """
            SELECT COALESCE(
                AVG((sentiment_raw->>'positive')::float - (sentiment_raw->>'negative')::float),
                0
            )
            FROM articles
            WHERE published_at > NOW() - INTERVAL '24 hours'
            """
        )
    )
    avg_sentiment.set(float(result.scalar() or 0.0))
    pipeline_latency.set(float(await redis_client.get("stats:pipeline_latency_ms") or 0.0))

    return PlainTextResponse(generate_latest(REGISTRY).decode())
