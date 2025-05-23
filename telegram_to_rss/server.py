import asyncio
from typing import Optional
from quart import Quart, render_template
from telegram_to_rss.client import TelegramToRssClient
from telegram_to_rss.config import (
    api_hash,
    api_id,
    session_path,
    password,
    static_path,
    feed_size_limit,
    initial_feed_size,
    update_interval_seconds,
    db_path,
    loglevel,
    max_media_size,
)
from telegram_to_rss.qr_code import get_qr_code_image
from telegram_to_rss.db import init_feeds_db, close_feeds_db
from telegram_to_rss.generate_feed import update_feeds_cache
from telegram_to_rss.poll_telegram import (
    TelegramPoller,
    update_feeds_in_db,
    reset_feeds_in_db,
)
from telegram_to_rss.models import Feed
import logging

logging.basicConfig(
    level=loglevel,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

app = Quart(__name__, static_folder=static_path, static_url_path="/static")
client = TelegramToRssClient(
    session_path=session_path, api_id=api_id, api_hash=api_hash, password=password
)
telegram_poller = TelegramPoller(
    client=client,
    message_limit=feed_size_limit,
    new_feed_limit=initial_feed_size,
    static_path=static_path,
    max_media_size=max_media_size,
)
rss_task: asyncio.Task | None = None


async def start_rss_generation():
    global rss_task

    logging.info("start_rss_generation")

    async def update_rss(inital_delay: Optional[float] = None):
        global rss_task
        if inital_delay is not None:
            await asyncio.sleep(inital_delay)

        should_reschedule = True
        reschedule_delay = None
        try:
            logging.info("update_rss -> db")
            await update_feeds_in_db(telegram_poller=telegram_poller)

            logging.info("update_rss -> cache")
            await update_feeds_cache(telegram_poller=telegram_poller, feed_render_dir=static_path)

            logging.info("update_rss -> sleep")
            await asyncio.sleep(update_interval_seconds)
        except asyncio.CancelledError:
            should_reschedule = False
        except ConnectionError as e:
            reschedule_delay = 5
            logging.warning(f"update_rss -> connection error, reconnecting telethon: {e}")
            await telegram_poller._client._telethon.connect()
        except Exception as e:
            reschedule_delay = 2
            logging.error(f"update_rss -> error: {e}")
            logging.warning("update_rss -> rebuilding feeds from scratch")
            await reset_feeds_in_db(telegram_poller=telegram_poller)
        finally:
            if should_reschedule:
                logging.info("update_rss -> scheduling a new run")
                loop = asyncio.get_event_loop()
                rss_task = loop.create_task(update_rss(reschedule_delay))

    await client.start()

    loop = asyncio.get_event_loop()
    rss_task = loop.create_task(update_rss())

    logging.info("start_rss_generation -> done")


@app.before_serving
async def startup():
    global rss_task

    logging.info("startup")

    await init_feeds_db(db_path=db_path)
    loop = asyncio.get_event_loop()
    rss_task = loop.create_task(start_rss_generation())

    logging.info("startup -> done")


@app.after_serving
async def cleanup():
    logging.info("cleanup")

    if rss_task is not None:
        rss_task.cancel()
    await client.stop()
    await close_feeds_db()

    logging.info("cleanup -> done")


@app.route("/")
async def root():
    logging.debug("GET /root %s", bool(client.qr_code_url))

    if client.qr_code_url is not None:
        qr_code_image = get_qr_code_image(client.qr_code_url)
        return await render_template("qr_code.html", qr_code=qr_code_image)

    feeds = await Feed.all()
    logging.debug("GET /root -> feeds %s", len(feeds))

    return await render_template("feeds.html", user=client.user, feeds=feeds)
