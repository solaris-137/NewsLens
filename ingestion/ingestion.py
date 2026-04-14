import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from hashlib import sha256

import aiohttp
import feedparser
import redis as redis_sync
from bs4 import BeautifulSoup
from dateutil.parser import parse

BBC_FEEDS = [
    "http://feeds.bbci.co.uk/news/business/rss.xml",
    "http://feeds.bbci.co.uk/news/technology/rss.xml",
]

REUTERS_FEEDS = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.reuters.com/reuters/technologyNews",
]

TECHCRUNCH_FEEDS = [
    "https://techcrunch.com/feed/",
]

VERGE_FEEDS = [
    "https://www.theverge.com/rss/index.xml",
    "https://www.theverge.com/apple/rss/index.xml",
]

SOURCE_MAP = {
    "feeds.bbci.co.uk": "bbc",
    "feeds.reuters.com": "reuters",
    "techcrunch.com": "techcrunch",
    "www.theverge.com": "verge",
}

ALL_FEEDS = BBC_FEEDS + REUTERS_FEEDS + TECHCRUNCH_FEEDS + VERGE_FEEDS

APPLE_KEYWORDS = [
    "apple",
    "aapl",
    "iphone",
    "ipad",
    "macbook",
    "imac",
    "mac mini",
    "mac pro",
    "airpods",
    "apple watch",
    "vision pro",
    "ios",
    "macos",
    "app store",
    "tim cook",
    "apple silicon",
    "m1",
    "m2",
    "m3",
    "m4",
    "apple intelligence",
    "siri",
    "wwdc",
    "apple event",
]

FRUIT_WORDS = [
    "recipe",
    "fruit",
    "cider",
    "orchard",
    "pie",
    "juice",
    "farm",
    "tree",
    "harvest",
]

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
LOCAL_DEV = os.environ.get("LOCAL_DEV", "true").lower() == "true"

redis_client = redis_sync.from_url(REDIS_URL, decode_responses=True)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def make_stats() -> dict[str, int]:
    return {
        "accepted": 0,
        "filtered_age": 0,
        "filtered_dedup": 0,
        "filtered_relevance": 0,
        "total_fetched": 0,
    }


def derive_source(feed_url: str) -> str:
    for feed_host, source_name in SOURCE_MAP.items():
        if feed_host in feed_url:
            return source_name
    return ""


def parse_published_at(entry: feedparser.FeedParserDict, url: str) -> datetime:
    raw_published_at = entry.get("published") or entry.get("updated")
    if not raw_published_at:
        logger.debug(f"published_at fallback | {url}")
        return datetime.utcnow()

    try:
        published_at = parse(raw_published_at)
    except (TypeError, ValueError, OverflowError) as exc:
        logger.debug(f"published_at fallback | {url} | {exc}")
        return datetime.utcnow()

    if published_at.tzinfo is not None:
        published_at = published_at.astimezone(timezone.utc).replace(tzinfo=None)

    return published_at


def clean_rss_summary(summary: str) -> str:
    return BeautifulSoup(summary, "html.parser").get_text().strip()


def is_relevant(title: str, rss_summary: str) -> bool:
    text = (title + " " + rss_summary).lower()
    has_apple = any(keyword in text for keyword in APPLE_KEYWORDS)
    is_fruit = "apple" in text and any(fruit_word in text for fruit_word in FRUIT_WORDS)
    return has_apple and not is_fruit


def process_entry(
    entry: feedparser.FeedParserDict,
    feed_url: str,
    stats: dict[str, int],
) -> None:
    title = entry.get("title", "").strip()
    url = entry.get("link", "")
    published_at = parse_published_at(entry, url)
    rss_summary = clean_rss_summary(entry.get("summary", ""))
    source = derive_source(feed_url)

    if published_at < datetime.utcnow() - timedelta(hours=24):
        stats["filtered_age"] += 1
        logger.debug(f"SKIP age | {url}")
        return

    url_hash = sha256(url.encode()).hexdigest()
    hash_key = "seen:" + url_hash
    if redis_client.exists(hash_key):
        stats["filtered_dedup"] += 1
        logger.debug(f"SKIP dedup | {url}")
        return
    redis_client.setex(hash_key, 172800, "1")

    if not is_relevant(title, rss_summary):
        stats["filtered_relevance"] += 1
        logger.debug(f"SKIP relevance | {url}")
        return

    payload = {
        "id": url_hash,
        "url": url,
        "source": source,
        "title": title,
        "published_at": published_at.isoformat(),
        "rss_summary": rss_summary,
        "queued_at": datetime.utcnow().isoformat(),
    }

    redis_client.rpush("scrape-queue", json.dumps(payload))
    stats["accepted"] += 1
    logger.debug(f"ACCEPT | {url}")


async def poll_feed(
    session: aiohttp.ClientSession,
    feed_url: str,
) -> dict[str, int]:
    stats = make_stats()

    try:
        async with session.get(feed_url) as response:
            response.raise_for_status()
            feed_text = await response.text()
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        logger.warning(f"Failed to fetch feed {feed_url}: {exc}")
        return stats
    except Exception as exc:
        logger.warning(f"Failed to fetch feed {feed_url}: {exc}")
        return stats

    try:
        loop = asyncio.get_event_loop()
        feed = await loop.run_in_executor(None, feedparser.parse, feed_text)
        entries = getattr(feed, "entries", [])
        if not entries:
            logger.debug(f"empty feed: {feed_url}")
            return stats

        stats["total_fetched"] = len(entries)
        for entry in entries:
            try:
                process_entry(entry, feed_url, stats)
            except Exception as exc:
                logger.warning(f"Failed to process entry from {feed_url}: {exc}")
    except Exception as exc:
        logger.warning(f"Failed to parse feed {feed_url}: {exc}")

    return stats


def log_poll_stats(stats: dict[str, int]) -> None:
    logger.info(
        f"Poll complete | accepted={stats['accepted']} "
        f"filtered_age={stats['filtered_age']} "
        f"filtered_dedup={stats['filtered_dedup']} "
        f"filtered_relevance={stats['filtered_relevance']} "
        f"total_fetched={stats['total_fetched']}"
    )


async def poll_once() -> dict[str, int]:
    stats = make_stats()

    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            feed_results = await asyncio.gather(
                *(poll_feed(session, feed_url) for feed_url in ALL_FEEDS)
            )

        for feed_stats in feed_results:
            for key in stats:
                stats[key] += feed_stats[key]
    except Exception:
        logger.exception("poll_once failed")

    return stats


async def main() -> None:
    _ = LOCAL_DEV

    while True:
        stats = await poll_once()
        log_poll_stats(stats)
        await asyncio.sleep(1800)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Poller stopped")
