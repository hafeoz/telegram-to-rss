from typing import Union
from telethon.tl.custom import Message
from telethon.types import Document, Photo
from telegram_to_rss.FastTelethon import download_file
from telegram_to_rss.client import TelegramToRssClient, custom, types
from telegram_to_rss.models import Feed, FeedEntry
from tortoise.expressions import Q
from tortoise.transactions import atomic
from pathlib import Path
import logging


class TelegramPoller:
    _client: TelegramToRssClient
    _message_limit: int
    _new_feed_limit: int
    _static_path: Path
    _max_media_size: int

    def __init__(
        self,
        client: TelegramToRssClient,
        message_limit: int,
        new_feed_limit: int,
        static_path: Path,
        max_media_size: int,
    ) -> None:
        self._client = client
        self._message_limit = message_limit
        self._new_feed_limit = new_feed_limit
        self._static_path = static_path
        self._max_media_size = max_media_size


    async def fetch_dialogs(self):
        tg_dialogs = await self._client.list_dialogs()
        db_feeds = await Feed.all()

        tg_dialogs_ids = set([dialog.id for dialog in tg_dialogs])
        db_feeds_ids = set([feed.id for feed in db_feeds])

        feed_ids_to_delete = db_feeds_ids - tg_dialogs_ids
        feed_ids_to_create = tg_dialogs_ids - db_feeds_ids
        feed_ids_to_update = db_feeds_ids.intersection(tg_dialogs_ids)

        feeds_to_create = [
            dialog for dialog in tg_dialogs if dialog.id in feed_ids_to_create
        ]
        feeds_to_update = [
            dialog for dialog in tg_dialogs if dialog.id in feed_ids_to_update
        ]

        return (list(feed_ids_to_delete), feeds_to_create, feeds_to_update)

    async def bulk_delete_feeds(self, ids: list[int] | None):
        if ids is None:
            await Feed.all().delete()
            return
        if len(ids) != 0:
            await Feed.filter(Q(id__in=list(ids))).delete()

    @atomic()
    async def create_feed(self, dialog: custom.Dialog):
        logging.debug("TelegramPoller.create_feed %s %s", dialog.name, dialog.id)

        feed = await Feed.create(id=dialog.id, name=dialog.name)

        logging.debug("TelegramPoller.create_feed -> get_dialog_messages")
        dialog_messages = await self._client.get_dialog_messages(
            dialog=dialog, limit=self._new_feed_limit
        )
        logging.debug("TelegramPoller.create_feed -> _process_new_dialog_messages")
        feed_entries = await self._process_new_dialog_messages(feed, dialog_messages)

        logging.debug("TelegramPoller.create_feed -> bulk_create")
        await FeedEntry.bulk_create(feed_entries)

    @atomic()
    async def update_feed(self, dialog: custom.Dialog):
        feed = await Feed.get(id=dialog.id)
        last_feed_entry = await FeedEntry.filter(feed=feed).order_by("-date").first()
        logging.debug(
            f"TelegramPoller.update_feed -> last feed entry {last_feed_entry}",
        )

        get_dialog_messages_args = {}
        if last_feed_entry:
            [_, tg_message_id] = parse_feed_entry_id(last_feed_entry.id)
            get_dialog_messages_args["min_message_id"] = tg_message_id
        else:
            get_dialog_messages_args["limit"] = self._new_feed_limit
            logging.warning(
                f"TelegramPoller.update_feed -> feed {feed.name} ({feed.id}) does not have associated feed entries"
            )

        new_dialog_messages = await self._client.get_dialog_messages(
            dialog=dialog, **get_dialog_messages_args
        )

        for new_message in new_dialog_messages:
            if new_message.date is None:
                logging.warning(
                    f"TelegramPoller.update_feed {feed.name} ({feed.id}) -> message without a date! WTF? {new_message.id} {new_message.message}"
                )
                continue
            if last_feed_entry and new_message.date <= last_feed_entry.date:
                logging.warning(
                    f"TelegramPoller.update_feed {feed.name} ({feed.id}) -> TG sent a message older than we requested! WTF? TG sent ut {new_message.date} {new_message.message}, our last known message {last_feed_entry.date} {last_feed_entry.message}"
                )
                continue

        feed_entries = await self._process_new_dialog_messages(
            feed, new_dialog_messages
        )

        await FeedEntry.bulk_create(feed_entries)
        # Save even if unchanged to update date
        await feed.save()

        old_feed_entries = (
            await FeedEntry.filter(feed=feed)
            .order_by("-date")
            .limit(self._message_limit)
            .offset(self._message_limit)
        )

        for entry in old_feed_entries:
            logging.debug(f"Deleting FeedEntry with id: {entry.id}")
            await entry.delete()

    async def _process_new_dialog_messages(
        self, feed: Feed, dialog_messages: list[custom.Message]
    ):
        filtered_dialog_messages: list[custom.Message] = []
        logging.info(f"Processing {len(dialog_messages)} messages from {feed.name}")

        for dialog_message in dialog_messages:
            try:
                logging.debug(
                    "Processing message ID: %s, grouped_id: %s, has photo: %s, has media: %s, text: %s",
                    dialog_message.id,
                    dialog_message.grouped_id,
                    dialog_message.photo is not None,
                    dialog_message.media is not None,
                    dialog_message.text,
                )

                if dialog_message.text is None:
                    continue

                dialog_message.downloaded_media = []

                if (
                    dialog_message.grouped_id is None
                    or len(filtered_dialog_messages) == 0
                    or dialog_message.grouped_id != filtered_dialog_messages[-1].grouped_id
                ):
                    filtered_dialog_messages.append(dialog_message)
                else:
                    if len(dialog_message.text) > len(filtered_dialog_messages[-1].text):
                        filtered_dialog_messages[-1].text = dialog_message.text

                last_processed_message = filtered_dialog_messages[-1]

                if dialog_message.photo:
                    await self._download_media(dialog_message, dialog_message.photo, last_processed_message, feed, 'photo')

                document = dialog_message.document
                if document is not None:
                    mime_type = document.mime_type.split("/")[0]
                    logging.debug(f"Document mime type: {mime_type}")
                    if document.size > self._max_media_size:
                        logging.info(f"Media in message {dialog_message.id} is too large ({document.size} bytes). Skipping download.")
                        last_processed_message.downloaded_media.append("TOO_LARGE")
                        continue
                    await self._download_media(dialog_message, document, last_processed_message, feed, mime_type)

            except Exception as e:
                logging.error(f"Error processing message {dialog_message.id}: {e}", exc_info=True)
                continue

        feed_entries: list[FeedEntry] = []
        for dialog_message in filtered_dialog_messages:
            feed_entry_id = to_feed_entry_id(feed, dialog_message)
            feed_entries.append(
                FeedEntry(
                    id=feed_entry_id,
                    feed=feed,
                    message=dialog_message.text,
                    date=dialog_message.date,
                    media=dialog_message.downloaded_media,
                    has_unsupported_media=getattr(dialog_message, 'has_unsupported_media', False),
                )
            )
        return feed_entries

    async def _download_media(self, dialog_message: Message, media: Union[Document, Photo], last_processed_message, feed, media_type):
        try:
            feed_entry_media_id = "{}-{}".format(
                to_feed_entry_id(feed, dialog_message),
                len(last_processed_message.downloaded_media),
            )
            media_path = self._static_path.joinpath(feed_entry_media_id)

            def progress_callback(current, total, media_path=media_path):
                logging.debug(
                    "Downloading %s %s: %s out of %s",
                    media_type,
                    media_path,
                    current,
                    total,
                )

            if isinstance(media, Photo):
                await dialog_message.downloaded_media(media_path, progress_callback=progress_callback)
            else:
                with open(media_path, "wb") as out:
                    await download_file(
                        dialog_message.client,
                        media,
                        out,
                        progress_callback=progress_callback
                    )
            last_processed_message.downloaded_media.append(Path(media_path).name)
            logging.info(f"Downloaded {media_type} to {media_path}")
        except Exception as e:
            logging.warning(
                f"Downloading {media_type} failed with {e} for message {dialog_message.id} {dialog_message.date} {dialog_message.text}",
            )
            last_processed_message.downloaded_media.append("FAIL")


def to_feed_entry_id(feed: Feed, dialog_message: custom.Message):
    return "{}--{}".format(feed.id, dialog_message.id)


def parse_feed_entry_id(id: str):
    [channel_id, message_id] = id.split("--")
    return int(channel_id), int(message_id)


async def reset_feeds_in_db(telegram_poller: TelegramPoller):
    logging.debug("reset_feeds_in_db")

    await telegram_poller.bulk_delete_feeds(ids=None)

    logging.debug("reset_feeds_in_db -> done")


async def update_feeds_in_db(telegram_poller: TelegramPoller):
    logging.debug("update_feeds_in_db")

    [feed_ids_to_delete, feeds_to_create, feeds_to_update] = (
        await telegram_poller.fetch_dialogs()
    )
    logging.debug(
        "update_feeds_in_db -> fetched dialogs %s %s %s",
        feed_ids_to_delete,
        [dialog.id for dialog in feeds_to_create],
        [dialog.id for dialog in feeds_to_update],
    )

    await telegram_poller.bulk_delete_feeds(feed_ids_to_delete)
    logging.debug("update_feeds_in_db -> deleted feeds %s", feed_ids_to_delete)

    for feed_to_create in feeds_to_create:
        logging.debug(
            "update_feeds_in_db.create_feed %s %s",
            feed_to_create.id,
            feed_to_create.name,
        )
        await telegram_poller.create_feed(feed_to_create)
        logging.debug("update_feeds_in_db.create_feed -> done")

    for feed_to_update in feeds_to_update:
        logging.debug(
            "update_feeds_in_db.update_feed %s %s",
            feed_to_update.id,
            feed_to_update.name,
        )
        await telegram_poller.update_feed(feed_to_update)
        logging.debug("update_feeds_in_db.update_feed -> done")
