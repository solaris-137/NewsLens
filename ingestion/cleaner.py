import asyncio
import json
import logging
import os
import re
from datetime import datetime

import boto3
import redis as redis_sync

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

LOCAL_DEV = os.environ.get("LOCAL_DEV", "true").lower() == "true"
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis_sync.from_url(REDIS_URL, decode_responses=True)


async def load_html(article: dict) -> str | None:
    path = article["raw_html_path"]

    if LOCAL_DEV:
        try:
            with open(path, "r", encoding="utf-8") as file_handle:
                return file_handle.read()
        except FileNotFoundError:
            logger.error(f"HTML file not found | {path}")
            return None
    else:
        try:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                region_name=os.environ.get("AWS_REGION", "us-east-1"),
            )
            response = s3.get_object(
                Bucket=os.environ["AWS_S3_BUCKET"],
                Key=path,
            )
            return response["Body"].read().decode("utf-8")
        except Exception as exc:
            logger.error(f"S3 read failed | {path} | {exc}")
            return None


def extract_with_newspaper(
    url: str,
    raw_html: str,
    rss_title: str,
    rss_date: str,
) -> dict:
    from newspaper import Article

    article = Article(url)
    article.download(input_html=raw_html)
    article.parse()

    return {
        "text": article.text,
        "title": article.title or rss_title,
        "authors": article.authors or [],
        "publish_date": article.publish_date or rss_date,
        "extractor_used": "newspaper3k",
    }


def extract_with_readability(raw_html: str, rss_title: str, rss_date: str) -> dict:
    from bs4 import BeautifulSoup
    from readability import Document

    doc = Document(raw_html)
    summary_html = doc.summary()
    soup = BeautifulSoup(summary_html, "html.parser")
    text = soup.get_text(separator="\n")

    return {
        "text": text,
        "title": doc.title() or rss_title,
        "authors": [],
        "publish_date": rss_date,
        "extractor_used": "readability",
    }


def extract_text(url: str, raw_html: str, rss_title: str, rss_date: str) -> dict:
    result = extract_with_newspaper(url, raw_html, rss_title, rss_date)

    if len(result["text"].strip()) < 200:
        logger.debug(
            f"newspaper3k too short ({len(result['text'])} chars), trying readability | {url}"
        )
        result = extract_with_readability(raw_html, rss_title, rss_date)

    return result


def clean_article_text(raw_text: str) -> str:
    lines = raw_text.split("\n")

    lines = [line.strip() for line in lines]
    lines = [line for line in lines if len(line) >= 40]

    junk = [
        "subscribe",
        "sign up",
        "newsletter",
        "cookies",
        "privacy policy",
        "terms of use",
        "all rights reserved",
        "read more",
        "click here",
        "advertisement",
        "follow us",
        "share this",
        "related articles",
        "you might also like",
        "enable javascript",
        "please enable",
        "this content requires",
        "loading...",
        "skip to content",
        "jump to navigation",
    ]
    lines = [
        line
        for line in lines
        if not any(junk_text in line.lower() for junk_text in junk)
    ]

    cleaned = "\n".join(lines)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = re.sub(r" {2,}", " ", cleaned)

    paragraphs = [paragraph.strip() for paragraph in cleaned.split("\n\n") if paragraph.strip()]
    return "\n\n".join(paragraphs[:5])


def _push_to_service_bus(queue_name: str, payload: dict) -> None:
    from azure.servicebus import ServiceBusClient, ServiceBusMessage

    conn_str = os.environ["AZURE_SERVICE_BUS_CONN_STR"]
    with ServiceBusClient.from_connection_string(conn_str) as client:
        with client.get_queue_sender(queue_name=queue_name) as sender:
            sender.send_messages(ServiceBusMessage(json.dumps(payload)))


def _check_fail_rate_alert(today: str, source: str) -> None:
    failed = int(redis_client.get(f"stats:extraction_failed:{today}") or 0)
    total = int(redis_client.get(f"stats:total_fetched:{today}") or 1)

    fail_rate = failed / total
    if fail_rate > 0.20:
        alert = {
            "type": "pipeline_alert",
            "severity": "warning",
            "message": f"Extraction fail rate {fail_rate:.1%} exceeds 20% threshold today",
            "failed": failed,
            "total": total,
            "source": source,
            "ts": datetime.utcnow().isoformat(),
        }
        if LOCAL_DEV:
            logger.error(f"PIPELINE_ALERT | {alert['message']}")
        else:
            _push_to_service_bus("pipeline-alerts", alert)


def quality_gate(article_id: str, url: str, clean_text: str, source: str) -> bool:
    today = datetime.utcnow().strftime("%Y-%m-%d")
    _ = article_id

    if len(clean_text.strip()) < 200:
        logger.warning(f"EXTRACTION_FAILED | too short ({len(clean_text)} chars) | {url}")
        redis_client.incr(f"stats:extraction_failed:{today}")
        _check_fail_rate_alert(today, source)
        return False

    return True


def build_output(article: dict, extracted: dict, clean_text: str) -> dict:
    authors = extracted["authors"]
    if authors is None:
        authors = []
    elif isinstance(authors, str):
        authors = [authors]
    elif not isinstance(authors, list):
        authors = list(authors)

    published_at = extracted["publish_date"]
    if hasattr(published_at, "isoformat"):
        published_at = published_at.isoformat()
    else:
        published_at = str(published_at)

    return {
        "id": article["id"],
        "url": article["url"],
        "source": article["source"],
        "title": extracted["title"],
        "authors": authors,
        "published_at": published_at,
        "fetched_at": article["fetched_at"],
        "content": clean_text,
        "content_chars": len(clean_text),
        "extractor_used": extracted["extractor_used"],
    }


def push_to_output_queue(payload: dict) -> None:
    message = json.dumps(payload)

    if LOCAL_DEV:
        redis_client.rpush("articles-raw", message)
        logger.info(
            f"PUSHED_TO_REDIS | articles-raw | id={payload['id']} | "
            f"chars={payload['content_chars']}"
        )
    else:
        _push_to_service_bus("articles-raw", payload)
        logger.info(
            f"PUSHED_TO_SERVICE_BUS | articles-raw | id={payload['id']} | "
            f"chars={payload['content_chars']}"
        )


async def _mark_failed(article: dict, reason: str) -> None:
    today = datetime.utcnow().strftime("%Y-%m-%d")
    redis_client.incr(f"stats:extraction_failed:{today}")
    redis_client.setex(
        f"extraction_failed:{article['id']}",
        86400,
        json.dumps(
            {
                "url": article["url"],
                "reason": reason,
                "ts": datetime.utcnow().isoformat(),
            }
        ),
    )
    logger.warning(f"MARKED_FAILED | {article['url']} | reason={reason}")


async def process_article(article: dict) -> None:
    url = article["url"]
    today = datetime.utcnow().strftime("%Y-%m-%d")

    redis_client.incr(f"stats:total_fetched:{today}")

    raw_html = await load_html(article)
    if raw_html is None:
        await _mark_failed(article, "html_load_failed")
        return

    try:
        extracted = extract_text(
            url=url,
            raw_html=raw_html,
            rss_title=article.get("title", ""),
            rss_date=article.get("published_at", ""),
        )
    except Exception as exc:
        logger.error(f"EXTRACTION_ERROR | {url} | {exc}")
        await _mark_failed(article, "extraction_exception")
        return

    clean_text = clean_article_text(extracted["text"])
    logger.info(
        f"EXTRACTED | extractor={extracted['extractor_used']} | "
        f"chars={len(clean_text)} | {url}"
    )

    if not quality_gate(article["id"], url, clean_text, article["source"]):
        return

    output = build_output(article, extracted, clean_text)
    push_to_output_queue(output)


async def main() -> None:
    logger.info("Cleaner service started")
    while True:
        item = redis_client.blpop("extract-queue", timeout=30)
        if item is None:
            continue

        _, payload = item
        article = json.loads(payload)
        await process_article(article)


if __name__ == "__main__":
    asyncio.run(main())
