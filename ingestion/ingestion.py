import asyncio
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from hashlib import sha256

import aiofiles
import aiohttp
import boto3
import feedparser
import redis as redis_sync
from bs4 import BeautifulSoup
from dateutil.parser import parse
from playwright.async_api import async_playwright

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

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

DOMAIN_DELAYS = {
    "bbc": (1.0, 2.0),
    "reuters": (2.0, 3.5),
    "techcrunch": (1.0, 2.5),
    "verge": (1.0, 2.5),
}

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
LOCAL_DEV = os.environ.get("LOCAL_DEV", "true").lower() == "true"

redis_client = redis_sync.from_url(REDIS_URL, decode_responses=True)

_last_fetch: dict[str, float] = {}
_domain_lock = asyncio.Lock()
semaphore = asyncio.Semaphore(3)

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


async def poll_loop() -> None:
    while True:
        stats = await poll_once()
        log_poll_stats(stats)
        await asyncio.sleep(1800)


async def _domain_rate_limit(source: str) -> None:
    async with _domain_lock:
        min_delay, max_delay = DOMAIN_DELAYS.get(source, (1.0, 2.5))
        last = _last_fetch.get(source, 0)
        elapsed = time.time() - last
        wait = random.uniform(min_delay, max_delay)
        if elapsed < wait:
            await asyncio.sleep(wait - elapsed)
        _last_fetch[source] = time.time()


async def _mark_failed(article: dict[str, str], reason: str) -> None:
    today = datetime.utcnow().strftime("%Y-%m-%d")
    redis_client.incr(f"stats:fetch_failed:{today}")
    redis_client.setex(
        f"fetch_failed:{article['id']}",
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


async def _save_html(article: dict[str, str], raw_html: str) -> str:
    source = article["source"]
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    filename = f"{article['id']}.html"

    if LOCAL_DEV:
        path = f"/tmp/apple-sentiment/{source}/{date_str}/{filename}"
        await asyncio.to_thread(os.makedirs, os.path.dirname(path), exist_ok=True)
        async with aiofiles.open(path, "w", encoding="utf-8") as html_file:
            await html_file.write(raw_html)
        logger.debug(f"HTML saved locally | {path}")
        return path

    key = f"raw/{source}/{date_str}/{filename}"
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )
    await asyncio.to_thread(
        s3.put_object,
        Bucket=os.environ["AWS_S3_BUCKET"],
        Key=key,
        Body=raw_html.encode("utf-8"),
        ContentType="text/html",
    )
    logger.debug(f"HTML saved S3 | s3://{os.environ['AWS_S3_BUCKET']}/{key}")
    return key


async def _save_and_queue(article: dict[str, str], raw_html: str) -> None:
    storage_path = await _save_html(article, raw_html)

    payload = {
        **article,
        "raw_html_path": storage_path,
        "fetch_status": "success",
        "fetched_at": datetime.utcnow().isoformat(),
        "html_size_bytes": len(raw_html.encode("utf-8")),
    }

    redis_client.rpush("extract-queue", json.dumps(payload))
    logger.info(
        f"QUEUED_FOR_EXTRACT | {article['url']} | "
        f"size={payload['html_size_bytes']} bytes"
    )


async def _do_scrape(
    playwright,
    article: dict[str, str],
    attempt: int = 1,
) -> None:
    url = article["url"]
    source = article["source"]

    await _domain_rate_limit(source)
    logger.info(f"SCRAPE_START | {url} | source={source} | attempt={attempt}")

    browser = None
    context = None
    raw_html = ""
    retry_once = False
    failure_reason = ""

    try:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            java_script_enabled=True,
        )

        async def handle_route(route) -> None:
            if route.request.resource_type in ["image", "stylesheet", "font", "media"]:
                await route.abort()
            else:
                await route.continue_()

        await context.route("**/*", handle_route)
        page = await context.new_page()

        started_at = time.time()
        await page.goto(url, wait_until="domcontentloaded", timeout=8000)
        raw_html = await page.content()
        elapsed = time.time() - started_at
        logger.info(
            f"SCRAPE_SUCCESS | {url} | size={len(raw_html)} chars | "
            f"elapsed={elapsed:.2f}s"
        )
    except Exception as error:
        logger.warning(f"FETCH_FAILED | {url} | {error}")
        if attempt == 1:
            logger.warning(f"FETCH_FAILED attempt 1 | {url} | {error} | retrying once")
            retry_once = True
        else:
            logger.warning(f"FETCH_FAILED attempt 2 | {url} | {error} | giving up")
            failure_reason = "playwright_error_after_retry"
    finally:
        if context is not None:
            await context.close()
        if browser is not None:
            await browser.close()

    if retry_once:
        await asyncio.sleep(random.uniform(3.0, 5.0))
        await _do_scrape(playwright, article, attempt=2)
        return

    if failure_reason:
        await _mark_failed(article, reason=failure_reason)
        return

    await _save_and_queue(article, raw_html)


async def scrape_article(playwright, article: dict[str, str]) -> None:
    async with semaphore:
        try:
            await _do_scrape(playwright, article)
        except Exception as exc:
            logger.warning(f"FETCH_FAILED | {article['url']} | {exc}")
            await _mark_failed(article, reason="save_or_queue_error")


def _consume_task_result(task: asyncio.Task[None]) -> None:
    try:
        task.result()
    except asyncio.CancelledError:
        return
    except Exception as exc:
        logger.warning(f"SCRAPE_TASK_FAILED | {exc}")


async def scrape_loop() -> None:
    active_tasks: set[asyncio.Task[None]] = set()

    async with async_playwright() as playwright:
        try:
            while True:
                completed = {task for task in active_tasks if task.done()}
                for task in completed:
                    active_tasks.remove(task)
                    _consume_task_result(task)

                if len(active_tasks) >= 3:
                    done, pending = await asyncio.wait(
                        active_tasks,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    active_tasks = set(pending)
                    for task in done:
                        _consume_task_result(task)

                item = await asyncio.to_thread(
                    redis_client.blpop,
                    "scrape-queue",
                    timeout=30,
                )
                if item is None:
                    continue

                _, payload = item
                try:
                    article = json.loads(payload)
                except json.JSONDecodeError as exc:
                    logger.warning(f"Invalid scrape-queue payload: {exc}")
                    continue

                task = asyncio.create_task(scrape_article(playwright, article))
                active_tasks.add(task)
        finally:
            if active_tasks:
                for task in active_tasks:
                    task.cancel()
                await asyncio.gather(*active_tasks, return_exceptions=True)


async def main() -> None:
    await asyncio.gather(
        poll_loop(),
        scrape_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Poller stopped")
