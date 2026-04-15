import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import psycopg2
import redis as redis_sync
import sentry_sdk
import spacy
from psycopg2 import InterfaceError, OperationalError
from psycopg2.extras import Json
from sumy.nlp.tokenizers import Tokenizer
from sumy.parsers.plaintext import PlaintextParser
from sumy.summarizers.lsa import LsaSummarizer
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    pipeline as hf_pipeline,
)

MODEL_PATH = "/models/finbert"
QUEUE_NAME = "articles-raw"
LOCAL_DEV = os.environ.get("LOCAL_DEV", "true").lower() == "true"
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
DATABASE_URL = os.environ["DATABASE_URL"]
AZURE_SERVICE_BUS_CONN_STR = os.environ.get("AZURE_SERVICE_BUS_CONN_STR", "")
AZURE_APPINSIGHTS_CONN_STR = os.environ.get("AZURE_APPINSIGHTS_CONN_STR", "")
SENTRY_DSN = os.environ.get("SENTRY_DSN", "")

CATEGORY_KEYWORDS = {
    "earnings": [
        "revenue",
        "eps",
        "earnings",
        "profit",
        "quarterly",
        "fiscal",
        "guidance",
        "forecast",
        "beats",
        "misses",
    ],
    "products": [
        "iphone",
        "ipad",
        "macbook",
        "vision pro",
        "airpods",
        "launch",
        "release",
        "announced",
        "new model",
        "update",
    ],
    "legal": [
        "lawsuit",
        "court",
        "sued",
        "antitrust",
        "settlement",
        "fine",
        "penalty",
        "judge",
        "ruling",
        "litigation",
    ],
    "regulatory": [
        "sec",
        "ftc",
        "doj",
        "regulation",
        "compliance",
        "investigation",
        "probe",
        "watchdog",
        "parliament",
    ],
    "macroeconomic": [
        "tariff",
        "trade war",
        "inflation",
        "interest rate",
        "recession",
        "supply chain",
        "china",
        "manufacturing",
    ],
    "competition": [
        "samsung",
        "google",
        "microsoft",
        "meta",
        "amazon",
        "market share",
        "competitor",
        "rival",
    ],
    "executive": [
        "tim cook",
        "ceo",
        "board",
        "appointed",
        "resigned",
        "executive",
        "leadership",
    ],
    "supply_chain": [
        "supplier",
        "foxconn",
        "tsmc",
        "chip shortage",
        "production",
        "factory",
        "component",
    ],
}

PATTERNS = [
    r"\bApple Inc\b",
    r"\bAAPL\b",
    r"\biPhone\b",
    r"\biPad\b",
    r"\bMacBook\b",
    r"\bVision Pro\b",
    r"\bApp Store\b",
    r"\bTim Cook\b",
    r"\bWWDC\b",
    r"\bApple Silicon\b",
    r"\bM[1-4] chip\b",
]

DDL = """
CREATE TABLE IF NOT EXISTS articles (
    id               VARCHAR PRIMARY KEY,
    url              TEXT NOT NULL,
    source           VARCHAR(50),
    title            TEXT,
    summary          TEXT,
    published_at     TIMESTAMP,
    fetched_at       TIMESTAMP,
    sentiment_label  VARCHAR(10),
    sentiment_score  FLOAT,
    sentiment_raw    JSONB,
    category         VARCHAR(50),
    content_chars    INTEGER,
    extractor_used   VARCHAR(20),
    created_at       TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_articles_published  ON articles(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_articles_category   ON articles(category);
CREATE INDEX IF NOT EXISTS idx_articles_sentiment  ON articles(sentiment_label);
"""

INSERT_SQL = """
INSERT INTO articles (
    id,
    url,
    source,
    title,
    summary,
    published_at,
    fetched_at,
    sentiment_label,
    sentiment_score,
    sentiment_raw,
    category,
    content_chars,
    extractor_used
) VALUES (
    %(id)s,
    %(url)s,
    %(source)s,
    %(title)s,
    %(summary)s,
    %(published_at)s,
    %(fetched_at)s,
    %(sentiment_label)s,
    %(sentiment_score)s,
    %(sentiment_raw)s,
    %(category)s,
    %(content_chars)s,
    %(extractor_used)s
)
ON CONFLICT (id) DO NOTHING
"""

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

if SENTRY_DSN:
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        traces_sample_rate=0.2,
        profiles_sample_rate=0.1,
    )

redis_client = redis_sync.from_url(REDIS_URL, decode_responses=True)
sentiment_pipeline = None
nlp_spacy = None
service_bus_client = None
service_bus_receiver = None
service_bus_admin_client = None
metrics_client = None


@dataclass
class QueueMessage:
    payload: dict[str, Any]
    ack: Callable[[], None]
    abandon: Callable[[], None]
    dead_letter: Callable[[str], None]
    delivery_count: int = 1


class MetricsClient:
    def __init__(self, connection_string: str) -> None:
        self.enabled = False
        if not connection_string:
            return

        try:
            from opencensus.ext.azure import metrics_exporter
            from opencensus.stats import aggregation as aggregation_module
            from opencensus.stats import measure as measure_module
            from opencensus.stats import stats as stats_module
            from opencensus.stats import view as view_module
            from opencensus.tags import tag_map as tag_map_module
        except Exception as exc:
            logger.warning(f"AppInsights metrics disabled | {exc}")
            return

        stats = stats_module.stats
        self.view_manager = stats.view_manager
        self.stats_recorder = stats.stats_recorder
        self.tag_map = tag_map_module.TagMap()

        self.processed_measure = measure_module.MeasureInt(
            "articles_processed_total",
            "Total processed articles saved to PostgreSQL",
            "count",
        )
        self.avg_sentiment_measure = measure_module.MeasureFloat(
            "avg_sentiment_24hr",
            "Average composite sentiment over the last 24 hours",
            "score",
        )
        self.latency_measure = measure_module.MeasureFloat(
            "pipeline_latency_ms",
            "Wall-clock NLP pipeline latency in milliseconds",
            "ms",
        )

        exporter = metrics_exporter.new_metrics_exporter(
            connection_string=connection_string,
            export_interval=60,
            enable_standard_metrics=False,
        )
        self.view_manager.register_exporter(exporter)
        self.view_manager.register_view(
            view_module.View(
                "articles_processed_total_view",
                "Processed article counter",
                [],
                self.processed_measure,
                aggregation_module.CountAggregation(),
            )
        )
        self.view_manager.register_view(
            view_module.View(
                "avg_sentiment_24hr_view",
                "Average 24 hour sentiment",
                [],
                self.avg_sentiment_measure,
                aggregation_module.LastValueAggregation(),
            )
        )
        self.view_manager.register_view(
            view_module.View(
                "pipeline_latency_ms_view",
                "Pipeline latency histogram",
                [],
                self.latency_measure,
                aggregation_module.DistributionAggregation(
                    [50, 100, 250, 500, 1000, 2000, 5000, 10000]
                ),
            )
        )
        self.enabled = True

    def record(self, avg_sentiment: float, latency_ms: float) -> None:
        if not self.enabled:
            return

        measurement_map = self.stats_recorder.new_measurement_map()
        measurement_map.measure_int_put(self.processed_measure, 1)
        measurement_map.measure_float_put(self.avg_sentiment_measure, avg_sentiment)
        measurement_map.measure_float_put(self.latency_measure, latency_ms)
        measurement_map.record(self.tag_map)


def load_model() -> None:
    global metrics_client
    global nlp_spacy
    global sentiment_pipeline

    model_dir = Path(MODEL_PATH)
    if not model_dir.exists() or not (model_dir / "config.json").exists():
        logger.info(f"FinBERT not found, downloading to {MODEL_PATH}")
        model_dir.mkdir(parents=True, exist_ok=True)
        tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
        tokenizer.save_pretrained(MODEL_PATH)
        model.save_pretrained(MODEL_PATH)

    logger.info(f"Loading FinBERT from {MODEL_PATH}")
    sentiment_pipeline = hf_pipeline(
        "text-classification",
        model=MODEL_PATH,
        tokenizer=MODEL_PATH,
        return_all_scores=True,
    )
    nlp_spacy = spacy.load("en_core_web_sm")
    metrics_client = MetricsClient(AZURE_APPINSIGHTS_CONN_STR)


def init_db_schema() -> None:
    with psycopg2.connect(DATABASE_URL) as connection:
        with connection.cursor() as cursor:
            cursor.execute(DDL)
        connection.commit()


def init_queue_clients() -> None:
    global service_bus_admin_client
    global service_bus_client
    global service_bus_receiver

    if LOCAL_DEV:
        return

    from azure.servicebus import ServiceBusClient
    from azure.servicebus.management import ServiceBusAdministrationClient

    service_bus_client = ServiceBusClient.from_connection_string(
        AZURE_SERVICE_BUS_CONN_STR
    )
    service_bus_receiver = service_bus_client.get_queue_receiver(
        queue_name=QUEUE_NAME,
        max_wait_time=5,
    )
    service_bus_receiver.__enter__()
    service_bus_admin_client = ServiceBusAdministrationClient.from_connection_string(
        AZURE_SERVICE_BUS_CONN_STR
    )


def close_queue_clients() -> None:
    global service_bus_admin_client
    global service_bus_receiver
    global service_bus_client

    if service_bus_receiver is not None:
        service_bus_receiver.__exit__(None, None, None)
        service_bus_receiver = None
    if service_bus_admin_client is not None:
        service_bus_admin_client.close()
        service_bus_admin_client = None
    if service_bus_client is not None:
        service_bus_client.close()
        service_bus_client = None


def get_queue_depth() -> int:
    if LOCAL_DEV:
        return int(redis_client.llen(QUEUE_NAME))

    if service_bus_admin_client is None:
        return 0

    runtime = service_bus_admin_client.get_queue_runtime_properties(QUEUE_NAME)
    return int(runtime.active_message_count or 0)


def _service_bus_body_to_text(message: Any) -> str:
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


def get_next_message() -> QueueMessage | None:
    if LOCAL_DEV:
        raw = redis_client.rpop(QUEUE_NAME)
        if raw is None:
            return None

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            redis_client.rpush(
                f"{QUEUE_NAME}-dead-letter",
                json.dumps(
                    {
                        "payload": raw,
                        "reason": "invalid_json",
                        "ts": datetime.utcnow().isoformat(),
                    }
                ),
            )
            logger.warning("Invalid local queue payload moved to dead-letter")
            return None

        def ack() -> None:
            return None

        def abandon() -> None:
            return None

        def dead_letter(reason: str) -> None:
            redis_client.rpush(
                f"{QUEUE_NAME}-dead-letter",
                json.dumps(
                    {
                        "payload": payload,
                        "reason": reason,
                        "ts": datetime.utcnow().isoformat(),
                    }
                ),
            )

        return QueueMessage(
            payload=payload,
            ack=ack,
            abandon=abandon,
            dead_letter=dead_letter,
            delivery_count=1,
        )

    if service_bus_receiver is None:
        return None

    messages = service_bus_receiver.receive_messages(max_message_count=1, max_wait_time=5)
    if not messages:
        return None

    message = messages[0]
    try:
        payload = json.loads(_service_bus_body_to_text(message))
    except json.JSONDecodeError as exc:
        service_bus_receiver.dead_letter_message(
            message,
            reason="invalid_json",
            error_description=str(exc)[:1024],
        )
        logger.warning(f"Invalid Service Bus payload dead-lettered | {exc}")
        return None

    def ack() -> None:
        service_bus_receiver.complete_message(message)

    def abandon() -> None:
        service_bus_receiver.abandon_message(message)

    def dead_letter(reason: str) -> None:
        service_bus_receiver.dead_letter_message(
            message,
            reason=reason[:128],
            error_description=reason[:1024],
        )

    return QueueMessage(
        payload=payload,
        ack=ack,
        abandon=abandon,
        dead_letter=dead_letter,
        delivery_count=int(getattr(message, "delivery_count", 1) or 1),
    )


def is_apple_relevant(title: str, content: str) -> bool:
    text = title + " " + content[:500]
    doc = nlp_spacy(text)
    for ent in doc.ents:
        if ent.text.lower() == "apple" and ent.label_ == "ORG":
            return True
    return any(re.search(pattern, text) for pattern in PATTERNS)


def score_sentiment(title: str, content: str) -> dict[str, Any]:
    text = (title + ". " + content)[:1800]
    scores = sentiment_pipeline(text)[0]
    result = {score["label"].lower(): round(score["score"], 4) for score in scores}
    label = max(result, key=result.get)
    composite = round(result.get("positive", 0.0) - result.get("negative", 0.0), 4)
    return {"label": label, "scores": result, "composite": composite}


def summarise(content: str, title: str, sentence_count: int = 3) -> str:
    if not content or len(content) < 100:
        return title
    parser = PlaintextParser.from_string(content, Tokenizer("english"))
    summarizer = LsaSummarizer()
    sentences = summarizer(parser.document, sentence_count)
    summary = " ".join(str(sentence) for sentence in sentences)
    return summary or title


def categorise(title: str, content: str) -> str:
    text = (title + " " + content).lower()
    scores = {
        category: sum(1 for keyword in keywords if keyword in text)
        for category, keywords in CATEGORY_KEYWORDS.items()
    }
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "general"


def coerce_datetime(value: Any) -> Any:
    if not value:
        return None
    if hasattr(value, "isoformat"):
        if getattr(value, "tzinfo", None) is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    value_str = str(value)
    if value_str.endswith("Z"):
        value_str = value_str.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(value_str)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except ValueError:
        return None


def build_record(
    article: dict[str, Any],
    sentiment: dict[str, Any],
    summary: str,
    category: str,
) -> dict[str, Any]:
    return {
        "id": article["id"],
        "url": article["url"],
        "source": article.get("source"),
        "title": article.get("title"),
        "summary": summary,
        "published_at": coerce_datetime(article.get("published_at")),
        "fetched_at": coerce_datetime(article.get("fetched_at")),
        "sentiment_label": sentiment["label"],
        "sentiment_score": sentiment["composite"],
        "sentiment_raw": Json(sentiment["scores"]),
        "category": category,
        "content_chars": int(article.get("content_chars", 0)),
        "extractor_used": article.get("extractor_used"),
    }


def save_article_record(record: dict[str, Any]) -> bool:
    for attempt in range(1, 4):
        try:
            with psycopg2.connect(DATABASE_URL) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(INSERT_SQL, record)
                    inserted = cursor.rowcount > 0
                connection.commit()
            return inserted
        except (OperationalError, InterfaceError) as exc:
            if attempt == 3:
                raise
            logger.warning(
                f"DB_RETRY | id={record['id']} | attempt={attempt} | {exc}"
            )
            time.sleep(2)


def get_avg_sentiment_24hr() -> float:
    try:
        with psycopg2.connect(DATABASE_URL) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COALESCE(AVG(sentiment_score), 0)
                    FROM articles
                    WHERE created_at >= NOW() - INTERVAL '24 HOURS'
                    """
                )
                row = cursor.fetchone()
                return float(row[0] or 0.0)
    except Exception as exc:
        logger.warning(f"AVG_SENTIMENT_QUERY_FAILED | {exc}")
        return 0.0


def increment_counter(name: str) -> None:
    today = datetime.utcnow().strftime("%Y-%m-%d")
    redis_client.incr(f"{name}:{today}")


def set_sentry_article_context(article_id: str, source: str | None) -> None:
    with sentry_sdk.configure_scope() as scope:
        scope.set_tag("article.source", source or "unknown")
        scope.set_context(
            "article",
            {
                "id": article_id,
                "source": source or "unknown",
            },
        )


def capture_pipeline_error(error: Exception, article: dict[str, Any]) -> None:
    with sentry_sdk.push_scope() as scope:
        scope.set_extra("article_id", article.get("id"))
        scope.set_extra("article_url", article.get("url"))
        scope.set_extra("source", article.get("source"))
        sentry_sdk.capture_exception(error)


def publish_processed_article(
    article: dict[str, Any],
    summary: str,
    sentiment: dict[str, Any],
    category: str,
    latency_ms: float,
) -> None:
    payload = {
        "id": article["id"],
        "url": article["url"],
        "source": article.get("source"),
        "title": article.get("title"),
        "summary": summary,
        "published_at": article.get("published_at"),
        "fetched_at": article.get("fetched_at"),
        "sentiment_label": sentiment["label"],
        "sentiment_score": sentiment["composite"],
        "sentiment_raw": sentiment["scores"],
        "category": category,
        "content_chars": int(article.get("content_chars", 0)),
        "extractor_used": article.get("extractor_used"),
    }

    redis_client.set("stats:pipeline_latency_ms", round(latency_ms, 2))

    if LOCAL_DEV:
        redis_client.lpush("articles-processed", json.dumps(payload))
        return

    if not AZURE_SERVICE_BUS_CONN_STR:
        return

    try:
        from azure.servicebus import ServiceBusClient, ServiceBusMessage

        with ServiceBusClient.from_connection_string(AZURE_SERVICE_BUS_CONN_STR) as client:
            with client.get_topic_sender(topic_name="articles-processed") as sender:
                sender.send_messages(ServiceBusMessage(json.dumps(payload)))
    except Exception as exc:
        logger.warning(
            f"ARTICLES_PROCESSED_PUBLISH_FAILED | id={article['id']} | {exc}"
        )


def handle_failed_message(message: QueueMessage, reason: str) -> None:
    if LOCAL_DEV:
        message.dead_letter(reason)
        return

    if message.delivery_count >= 3:
        message.dead_letter(reason)
    else:
        message.abandon()


def process_article(message: QueueMessage) -> None:
    article = message.payload
    article_id = article.get("id", "unknown")
    started_at = time.perf_counter()
    set_sentry_article_context(article_id, article.get("source"))

    try:
        title = article.get("title", "")
        content = article.get("content", "")

        if not is_apple_relevant(title, content):
            increment_counter("stats:nlp_filtered")
            message.ack()
            logger.info(f"NLP_FILTERED | id={article_id}")
            return

        sentiment = score_sentiment(title, content)
        summary = summarise(content, title)
        category = categorise(title, content)
        inserted = save_article_record(
            build_record(article, sentiment, summary, category)
        )

        if inserted:
            increment_counter("stats:nlp_processed")
            latency_ms = (time.perf_counter() - started_at) * 1000
            avg_sentiment = get_avg_sentiment_24hr()
            publish_processed_article(article, summary, sentiment, category, latency_ms)
            if metrics_client is not None:
                metrics_client.record(avg_sentiment, latency_ms)
            logger.info(
                f"NLP_SAVED | id={article_id} | sentiment={sentiment['label']}"
            )
        else:
            logger.info(f"NLP_DUPLICATE | id={article_id}")

        message.ack()
    except Exception as exc:
        increment_counter("stats:nlp_errors")
        capture_pipeline_error(exc, article)
        logger.exception(f"NLP_ERROR | id={article_id} | {exc}")
        handle_failed_message(message, f"{exc.__class__.__name__}: {exc}")


def main() -> None:
    load_model()
    init_db_schema()
    init_queue_clients()
    logger.info("NLP pipeline started")

    try:
        while True:
            depth = get_queue_depth()
            if depth == 0:
                logger.info("Queue empty, sleeping 10s before exit.")
                time.sleep(10)
                logger.info("Queue empty, exiting.")
                break

            message = get_next_message()
            if message is None:
                continue

            process_article(message)
            time.sleep(0.1)
    finally:
        close_queue_clients()


if __name__ == "__main__":
    main()
