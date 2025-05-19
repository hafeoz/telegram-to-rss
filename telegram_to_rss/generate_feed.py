import logging
import mimetypes
import re
import xml.etree.ElementTree as ET
from pathlib import Path

from tortoise.query_utils import Prefetch

from telegram_to_rss.config import base_url
from telegram_to_rss.models import Feed, FeedEntry
from telegram_to_rss.poll_telegram import TelegramPoller, parse_feed_entry_id

CLEAN_TITLE = re.compile("<.*?>")


def clean_title(raw_html):
    cleantext = re.sub(CLEAN_TITLE, "", raw_html).replace("\n", " ").strip()
    return cleantext


async def generate_feed(
    telegram_poller: TelegramPoller, feed_render_dir: Path, feed: Feed
):
    logging.info("generate_feed %s %s", feed.name, feed.id)

    feed_id = await telegram_poller._client.telethon_dialog_id_to_tg_id_or_username(
        feed.id
    )
    if isinstance(feed_id, int):
        feed_url = f"https://t.me/c/{feed_id}"
    else:
        feed_url = f"https://t.me/{feed_id}"

    rss_root_el = ET.Element("rss", {"version": "2.0"})

    rss_feed_el = ET.SubElement(rss_root_el, "channel")

    ET.SubElement(rss_feed_el, "title").text = feed.name
    ET.SubElement(rss_feed_el, "pubDate").text = feed.last_update.isoformat()
    ET.SubElement(
        rss_feed_el,
        "link",
        {"href": feed_url},
    )
    ET.SubElement(rss_feed_el, "description").text = feed.name

    for feed_entry in feed.entries:
        [feed_id, entry_id] = parse_feed_entry_id(feed_entry.id)
        feed_id_ = (
            await telegram_poller._client.telethon_dialog_id_to_tg_id_or_username(
                feed_id
            )
        )
        if isinstance(feed_id_, int):
            feed_entry_url = f"https://t.me/c/{feed_id_}/{entry_id}"
        else:
            feed_entry_url = f"https://t.me/{feed_id_}/{entry_id}"

        rss_item_el = ET.SubElement(rss_feed_el, "item")

        ET.SubElement(rss_item_el, "guid").text = feed_entry_url

        message_text = clean_title(feed_entry.message)
        title = message_text[:100]
        ET.SubElement(rss_item_el, "title").text = title

        media_content = ""
        media_download_failure = 0
        media_too_large = 0

        # processing mediafiles
        for media_path in feed_entry.media:
            if media_path == "FAIL":
                media_download_failure += 1
            elif media_path.starts_with("TOO_LARGE"):
                media_too_large += 1
            else:
                media_url = "{}/static/{}".format(base_url, media_path)

                # checking file type
                mime = mimetypes.guess_type(media_url)[0] or ""
                mtype = mime.split("/")[0]
                if mtype == "image":
                    media_content += '<br /><img src="{}" alt="media"/>'.format(
                        media_url
                    )
                elif mtype == "video":
                    media_content += (
                        '<br /><video controls poster="{}" style="max-width:100%;">'
                        '<source src="{}" type="{}">'
                        "Your browser does not support the video tag.</video>"
                    ).format(media_url, media_url, mime)
                elif mtype == "audio":
                    media_content += '<br /><audio controls><source src="{}" type="{}"></audio>'.format(
                        media_url, mime
                    )
                else:
                    media_content += (
                        '<br /><a href="{}">{}</a>'.format(
                            media_url, media_path
                        )
                    )

        # creating feed with text and media
        content = feed_entry.message.replace("\n", "<br />") + media_content
        if feed_entry.has_unsupported_media:
            content += "<br /><strong>This message has unsupported attachment. Open Telegram to view it.</strong>"
        if media_download_failure > 0:
            content += f"<br /><strong>{media_download_failure} attachment(s) of this message has failed to download. Open Telegram to view it.</strong>"
        if media_too_large:
            content += f"<br /><strong>{media_too_large} attachment(s) of this message is too large to download. Open Telegram to view it.</strong>"

        ET.SubElement(rss_item_el, "description").text = content
        ET.SubElement(rss_item_el, "pubDate").text = feed_entry.date.isoformat()
        ET.SubElement(rss_item_el, "link", {"href": feed_entry_url}).text = (
            feed_entry_url
        )

    final_feed_file = feed_render_dir.joinpath("{}.xml".format(feed.id))

    rss_xml_tree = ET.ElementTree(rss_root_el)
    rss_xml_tree.write(
        file_or_filename=final_feed_file, encoding="UTF-8", short_empty_elements=True
    )

    logging.info("generate_feed -> done %s %s", feed.name, feed.id)


async def update_feeds_cache(telegram_poller: TelegramPoller, feed_render_dir: str):
    feeds = await Feed.all().prefetch_related(
        Prefetch("entries", queryset=FeedEntry.all().order_by("-date"))
    )

    for feed in feeds:
        await generate_feed(telegram_poller, feed_render_dir, feed)
