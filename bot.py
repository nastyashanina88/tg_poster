import asyncio
import json
import os
import re
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer

from telethon import TelegramClient, events
from telethon.errors import (
    ChatWriteForbiddenError,
    FloodWaitError,
    UserAlreadyParticipantError,
    UserBannedInChannelError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest


API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SOURCE_CHANNEL = int(os.environ["SOURCE_CHANNEL"])
SESSION1 = os.environ["SESSION1"]
SESSION2 = os.environ["SESSION2"]
DELAY = int(os.environ.get("DELAY", "5"))
SEND_LATEST_ON_START = os.environ.get("SEND_LATEST_ON_START", "0") == "1"
SOURCE_CATCHUP_ENABLED = os.environ.get("SOURCE_CATCHUP_ENABLED", "1") == "1"
SOURCE_CATCHUP_LIMIT = int(os.environ.get("SOURCE_CATCHUP_LIMIT", "5"))
SOURCE_CATCHUP_WINDOW_MINUTES = int(os.environ.get("SOURCE_CATCHUP_WINDOW_MINUTES", "180"))
STARTUP_DELAY = int(os.environ.get("STARTUP_DELAY", "45"))
MANUAL_DAILY_ENABLED = os.environ.get("MANUAL_DAILY_ENABLED", "0") == "1"
MANUAL_MEDIA_PATH = os.environ.get("MANUAL_MEDIA_PATH", "manual_posts/post_daily.png")
MANUAL_CAPTION = os.environ.get(
    "MANUAL_CAPTION",
    "@awardy_bot\n@awardy_bot\n@awardy_bot\n@awardy_bot\n@awardy_bot",
)
MANUAL_CATCHUP_WINDOW_MINUTES = int(os.environ.get("MANUAL_CATCHUP_WINDOW_MINUTES", "20"))
MSK = timezone(timedelta(hours=3))
SENT_MARKER_PATH = Path(os.environ.get("SENT_MARKER_PATH", "/tmp/tg_poster_manual_sent.json"))
SOURCE_MARKER_PATH = Path(os.environ.get("SOURCE_MARKER_PATH", "/tmp/tg_poster_source_sent.json"))


def load_channels(env_name):
    return [ch.strip() for ch in os.environ.get(env_name, "").split(",") if ch.strip()]


ACCOUNTS = [
    {
        "name": "my_account",
        "session": SESSION1,
        "channels": load_channels("CHANNELS_ACC1"),
        "suffix": "\n\n@nasty_ff",
    },
    {
        "name": "my_account2",
        "session": SESSION2,
        "channels": load_channels("CHANNELS_ACC2"),
        "suffix": "\n\n@awardy_bot",
    },
]


def extract_invite_hash(channel):
    match = re.match(r"https?://t\.me/\+([A-Za-z0-9_-]+)", channel)
    if match:
        return match.group(1)
    match = re.match(r"https?://t\.me/joinchat/([A-Za-z0-9_-]+)", channel)
    if match:
        return match.group(1)
    return None


def is_media_invalid(reason):
    return "media object is invalid" in (reason or "").lower()


async def join_if_needed(client, channel):
    try:
        invite_hash = extract_invite_hash(channel)
        if invite_hash:
            await client(ImportChatInviteRequest(invite_hash))
        else:
            entity = await client.get_entity(channel)
            await client(JoinChannelRequest(entity))
        return True
    except UserAlreadyParticipantError:
        return True
    except FloodWaitError as exc:
        print(f"  ! join flood {exc.seconds}s: {channel}", flush=True)
        return False
    except Exception as exc:
        print(f"  ! join failed {channel}: {exc}", flush=True)
        return False


async def send_message_or_file(client, channel, msg, text):
    if msg.media:
        try:
            await client.send_file(channel, msg.media, caption=text)
            return
        except Exception as exc:
            if not is_media_invalid(str(exc)):
                raise
            await client.send_message(channel, text)
            return
    await client.send_message(channel, text)


async def send_to_channels(client, account, msg):
    channels = account["channels"]
    text = (msg.text or msg.message or "") + account["suffix"]
    ok = 0
    failed = 0

    print(f"[{account['name']}] post_id={msg.id}: {len(channels)} chats", flush=True)
    for index, channel in enumerate(channels, 1):
        print(f"[{account['name']}] [{index}/{len(channels)}] {channel}", flush=True)
        await join_if_needed(client, channel)

        try:
            await send_message_or_file(client, channel, msg, text)
            ok += 1
            print(f"[{account['name']}] ✓ {channel}", flush=True)
        except FloodWaitError as exc:
            failed += 1
            if exc.seconds <= 60:
                print(f"[{account['name']}] flood {exc.seconds}s, wait: {channel}", flush=True)
                await asyncio.sleep(exc.seconds + 3)
                try:
                    await send_message_or_file(client, channel, msg, text)
                    ok += 1
                    failed -= 1
                    print(f"[{account['name']}] ↻ ✓ {channel}", flush=True)
                except Exception as retry_exc:
                    print(f"[{account['name']}] retry failed {channel}: {retry_exc}", flush=True)
            else:
                print(f"[{account['name']}] slowmode {exc.seconds}s: {channel}", flush=True)
        except (ChatWriteForbiddenError, UserBannedInChannelError) as exc:
            failed += 1
            print(f"[{account['name']}] no access {channel}: {exc}", flush=True)
        except Exception as exc:
            failed += 1
            print(f"[{account['name']}] failed {channel}: {exc}", flush=True)

        if index < len(channels):
            await asyncio.sleep(DELAY)

    print(f"[{account['name']}] done: {ok} ok, {failed} failed", flush=True)


async def send_manual_to_channels(client, account, media_path, caption, schedule_key):
    channels = account["channels"]
    ok = 0
    failed = 0

    print(f"[{account['name']}] manual {schedule_key}: {len(channels)} chats", flush=True)
    for index, channel in enumerate(channels, 1):
        print(f"[{account['name']}] manual [{index}/{len(channels)}] {channel}", flush=True)
        await join_if_needed(client, channel)
        try:
            await client.send_file(channel, media_path, caption=caption)
            ok += 1
            print(f"[{account['name']}] manual ✓ {channel}", flush=True)
        except FloodWaitError as exc:
            failed += 1
            print(f"[{account['name']}] manual flood {exc.seconds}s: {channel}", flush=True)
        except (ChatWriteForbiddenError, UserBannedInChannelError) as exc:
            failed += 1
            print(f"[{account['name']}] manual no access {channel}: {exc}", flush=True)
        except Exception as exc:
            failed += 1
            print(f"[{account['name']}] manual failed {channel}: {exc}", flush=True)

        if index < len(channels):
            await asyncio.sleep(DELAY)

    print(f"[{account['name']}] manual done {schedule_key}: {ok} ok, {failed} failed", flush=True)


def load_sent_markers():
    try:
        return set(json.loads(SENT_MARKER_PATH.read_text()))
    except Exception:
        return set()


def save_sent_markers(markers):
    SENT_MARKER_PATH.write_text(json.dumps(sorted(markers)))


def load_source_markers():
    try:
        return set(json.loads(SOURCE_MARKER_PATH.read_text()))
    except Exception:
        return set()


def save_source_markers(markers):
    SOURCE_MARKER_PATH.write_text(json.dumps(sorted(markers)))


async def run_due_manual_tasks(clients, markers, now):
    media_path = Path(MANUAL_MEDIA_PATH)
    if not media_path.exists():
        print(f"Manual daily media not found: {media_path}", flush=True)
        return markers

    schedule = {
        "my_account": {"09:00", "15:00"},
        "my_account2": {"11:00", "17:00"},
    }
    day = now.strftime("%Y-%m-%d")
    current_minutes = now.hour * 60 + now.minute

    for client, account in clients:
        due_minutes = []
        for minute in sorted(schedule.get(account["name"], set())):
            hour, mins = [int(part) for part in minute.split(":")]
            scheduled_minutes = hour * 60 + mins
            if scheduled_minutes > current_minutes:
                continue
            key = f"{day}:{account['name']}:{minute}"
            if key in markers:
                continue
            if current_minutes - scheduled_minutes > MANUAL_CATCHUP_WINDOW_MINUTES:
                print(f"[{account['name']}] manual skip stale catch-up {key}", flush=True)
                markers.add(key)
                save_sent_markers(markers)
                continue
            due_minutes.append((minute, key))

        if not due_minutes:
            continue

        for minute, key in due_minutes[:-1]:
            print(f"[{account['name']}] manual skip stale catch-up {key}", flush=True)
            markers.add(key)
            save_sent_markers(markers)

        minute, key = due_minutes[-1]
        markers.add(key)
        save_sent_markers(markers)
        await send_manual_to_channels(client, account, str(media_path), MANUAL_CAPTION, key)
    return markers


async def manual_daily_scheduler(clients):
    if not MANUAL_DAILY_ENABLED:
        return

    media_path = Path(MANUAL_MEDIA_PATH)
    if not media_path.exists():
        print(f"Manual daily media not found: {media_path}", flush=True)
        return

    schedule = {
        "my_account": {"09:00", "15:00"},
        "my_account2": {"11:00", "17:00"},
    }
    print("Manual daily scheduler enabled: MSK 09/15 account1, 11/17 account2", flush=True)

    while True:
        now = datetime.now(MSK)
        markers = load_sent_markers()
        markers = await run_due_manual_tasks(clients, markers, now)

        await asyncio.sleep(30)


async def connect_account(account):
    client = TelegramClient(StringSession(account["session"]), API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError(f"{account['name']} session is not authorized")
    me = await client.get_me()
    print(f"[{account['name']}] ready: @{me.username}", flush=True)
    return client


async def connect_available_accounts():
    clients = []
    for account in ACCOUNTS:
        try:
            clients.append((await connect_account(account), account))
        except Exception as exc:
            print(f"[{account['name']}] connect failed: {type(exc).__name__}: {exc}", flush=True)
    if not clients:
        raise RuntimeError("No Telegram accounts connected")
    return clients


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *args):
        pass


def start_health_server():
    port = int(os.environ.get("PORT", "8080"))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    print(f"Health server on port {port}", flush=True)
    server.serve_forever()


async def main():
    threading.Thread(target=start_health_server, daemon=True).start()
    if STARTUP_DELAY > 0:
        print(f"Startup delay {STARTUP_DELAY}s before Telegram connect", flush=True)
        await asyncio.sleep(STARTUP_DELAY)

    clients = await connect_available_accounts()

    listener = clients[0][0]
    source_entity = await listener.get_entity(SOURCE_CHANNEL)
    processed = load_source_markers()

    async def handle_post(msg):
        key = str(msg.id)
        if key in processed:
            return
        processed.add(key)
        save_source_markers(processed)
        preview = (msg.text or msg.message or "").replace("\n", " ")[:100]
        print(f"New post id={msg.id}: {preview}", flush=True)
        await asyncio.gather(
            *(send_to_channels(client, account, msg) for client, account in clients)
        )
        print(f"Post id={msg.id} handled", flush=True)

    @listener.on(events.NewMessage(chats=source_entity))
    async def handler(event):
        await handle_post(event.message)

    if SEND_LATEST_ON_START:
        latest = await listener.get_messages(SOURCE_CHANNEL, limit=1)
        if latest:
            await handle_post(latest[0])

    if SOURCE_CATCHUP_ENABLED:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=SOURCE_CATCHUP_WINDOW_MINUTES)
        recent = await listener.get_messages(SOURCE_CHANNEL, limit=SOURCE_CATCHUP_LIMIT)
        catchup_posts = [msg for msg in reversed(recent) if msg.date and msg.date >= cutoff]
        print(
            f"Source catch-up enabled: last {SOURCE_CATCHUP_LIMIT}, "
            f"window {SOURCE_CATCHUP_WINDOW_MINUTES}m, due {len(catchup_posts)}",
            flush=True,
        )
        for msg in catchup_posts:
            await handle_post(msg)

    print(f"Watching source: {SOURCE_CHANNEL}", flush=True)
    await asyncio.gather(
        manual_daily_scheduler(clients),
        *(client.run_until_disconnected() for client, _ in clients),
    )


if __name__ == "__main__":
    asyncio.run(main())
