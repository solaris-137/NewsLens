import asyncio
import os
import time
from datetime import timezone

from fastapi import APIRouter, Depends, Request
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from cache import redis_client
from db import get_db
from models import Article
from rate_limit import limiter

router = APIRouter()


def _to_iso(value):
    if value is None:
        return None
    if getattr(value, "tzinfo", None) is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


async def check_db(db: AsyncSession) -> dict:
    start = time.time()
    try:
        await db.execute(text("SELECT 1"))
        return {"status": "ok", "latency_ms": round((time.time() - start) * 1000)}
    except Exception as exc:
        return {"status": "down", "error": str(exc)}


async def check_redis() -> dict:
    start = time.time()
    try:
        await redis_client.ping()
        return {"status": "ok", "latency_ms": round((time.time() - start) * 1000)}
    except Exception as exc:
        return {"status": "down", "error": str(exc)}


def _service_bus_runtime_check() -> dict:
    from azure.servicebus.management import ServiceBusAdministrationClient

    client = ServiceBusAdministrationClient.from_connection_string(
        os.environ["AZURE_SERVICE_BUS_CONN_STR"]
    )
    try:
        client.get_queue_runtime_properties("articles-raw")
        return {"status": "ok"}
    finally:
        client.close()


async def check_service_bus() -> dict:
    if os.environ.get("LOCAL_DEV") == "true":
        return {"status": "ok", "note": "local dev - using Redis"}

    try:
        return await asyncio.to_thread(_service_bus_runtime_check)
    except Exception as exc:
        return {"status": "down", "error": str(exc)}


@router.get("/api/health")
@limiter.limit("100/minute")
async def get_health(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    database, redis_check, service_bus = await asyncio.gather(
        check_db(db),
        check_redis(),
        check_service_bus(),
    )

    last_ingestion = None
    articles_24hr = None
    if database["status"] == "ok":
        try:
            last_ingestion_result = await db.execute(select(func.max(Article.fetched_at)))
            count_result = await db.execute(
                text(
                    "SELECT COUNT(*) FROM articles "
                    "WHERE published_at > NOW() - INTERVAL '24 hours'"
                )
            )
            last_ingestion = _to_iso(last_ingestion_result.scalar())
            articles_24hr = int(count_result.scalar() or 0)
        except Exception as exc:
            database = {**database, "meta_error": str(exc)}

    if database["status"] == "down":
        overall_status = "down"
    elif redis_check["status"] == "down" or service_bus["status"] == "down":
        overall_status = "degraded"
    else:
        overall_status = "ok"

    return {
        "status": overall_status,
        "checks": {
            "database": database,
            "redis": redis_check,
            "service_bus": service_bus,
            "last_ingestion": last_ingestion,
            "articles_24hr": articles_24hr,
        },
    }
