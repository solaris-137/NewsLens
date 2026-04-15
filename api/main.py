import asyncio
import contextlib
import json
import logging
import os

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from cache import redis_client
from rate_limit import limiter

if dsn := os.environ.get("SENTRY_DSN"):
    import sentry_sdk
    from sentry_sdk.integrations.fastapi import FastApiIntegration

    sentry_sdk.init(
        dsn=dsn,
        integrations=[FastApiIntegration()],
        traces_sample_rate=0.2,
        profiles_sample_rate=0.1,
    )

LOCAL_DEV = os.environ.get("LOCAL_DEV", "true").lower() == "true"
REDIS_URL = os.environ["REDIS_URL"]
AZURE_SERVICE_BUS_CONN_STR = os.environ.get("AZURE_SERVICE_BUS_CONN_STR", "")
AZURE_APPINSIGHTS_CONN_STR = os.environ.get("AZURE_APPINSIGHTS_CONN_STR", "")
PRODUCTION_DOMAIN = os.environ.get("PRODUCTION_DOMAIN", "")
ARTICLES_PROCESSED_SUBSCRIPTION = os.environ.get(
    "ARTICLES_PROCESSED_SUBSCRIPTION",
    "api-subscription",
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

connected_clients: set[WebSocket] = set()

app = FastAPI(title="Apple Sentiment API")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

origins = ["http://localhost:3000"]
if PRODUCTION_DOMAIN:
    origins.append(PRODUCTION_DOMAIN)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

from routers.feed import router as feed_router
from routers.health import router as health_router
from routers.metrics import router as metrics_router
from routers.sentiment import router as sentiment_router
from routers.stock import router as stock_router

app.include_router(feed_router)
app.include_router(sentiment_router)
app.include_router(stock_router)
app.include_router(health_router)
app.include_router(metrics_router)


def _message_body_to_text(message) -> str:
    body = message.body
    if isinstance(body, bytes):
        return body.decode("utf-8")
    if isinstance(body, str):
        return body

    chunks = []
    for chunk in body:
        if isinstance(chunk, bytes):
            chunks.append(chunk)
        elif isinstance(chunk, bytearray):
            chunks.append(bytes(chunk))
        else:
            chunks.append(str(chunk).encode("utf-8"))
    return b"".join(chunks).decode("utf-8")


async def broadcast_article(article: dict):
    dead = set()
    message = json.dumps({"type": "new_article", "data": article})
    for websocket in connected_clients:
        try:
            await websocket.send_text(message)
        except Exception:
            dead.add(websocket)
    connected_clients.difference_update(dead)


async def _redis_listener():
    while True:
        raw = await redis_client.blpop("articles-processed", timeout=5)
        if not raw:
            continue

        _, payload = raw
        try:
            await broadcast_article(json.loads(payload))
        except json.JSONDecodeError as exc:
            logger.warning(f"Invalid Redis websocket payload | {exc}")


async def _service_bus_listener():
    from azure.servicebus.aio import ServiceBusClient

    while True:
        try:
            async with ServiceBusClient.from_connection_string(
                AZURE_SERVICE_BUS_CONN_STR
            ) as client:
                receiver = client.get_subscription_receiver(
                    topic_name="articles-processed",
                    subscription_name=ARTICLES_PROCESSED_SUBSCRIPTION,
                    max_wait_time=5,
                )
                async with receiver:
                    while True:
                        messages = await receiver.receive_messages(
                            max_message_count=10,
                            max_wait_time=5,
                        )
                        for message in messages:
                            try:
                                article = json.loads(_message_body_to_text(message))
                                await broadcast_article(article)
                                await receiver.complete_message(message)
                            except Exception as exc:
                                logger.warning(f"WebSocket listener message failed | {exc}")
                                await receiver.abandon_message(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception(f"Service Bus listener error | {exc}")
            await asyncio.sleep(5)


async def service_bus_listener():
    if LOCAL_DEV:
        await _redis_listener()
    else:
        await _service_bus_listener()


@app.on_event("startup")
async def startup():
    app.state.listener_task = asyncio.create_task(service_bus_listener())


@app.on_event("shutdown")
async def shutdown():
    listener_task = getattr(app.state, "listener_task", None)
    if listener_task is not None:
        listener_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await listener_task
    await redis_client.aclose()


@app.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.discard(websocket)
    except Exception:
        connected_clients.discard(websocket)
        raise
