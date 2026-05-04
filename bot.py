import asyncio
import os
import re
import threading
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


async def connect_account(account):
    client = TelegramClient(StringSession(account["session"]), API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError(f"{account['name']} session is not authorized")
    me = await client.get_me()
    print(f"[{account['name']}] ready: @{me.username}", flush=True)
    return client


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

    clients = []
    for account in ACCOUNTS:
        clients.append((await connect_account(account), account))

    listener = clients[0][0]
    source_entity = await listener.get_entity(SOURCE_CHANNEL)
    processed = set()

    async def handle_post(msg):
        if msg.id in processed:
            return
        processed.add(msg.id)
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

    print(f"Watching source: {SOURCE_CHANNEL}", flush=True)
    await asyncio.gather(*(client.run_until_disconnected() for client, _ in clients))


if __name__ == "__main__":
    asyncio.run(main())
