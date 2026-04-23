"""
tg_poster bot for Render.
Watches SOURCE_CHANNEL and forwards new posts to two account lists.
"""
import asyncio
import os
import csv
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, ChatWriteForbiddenError, UserBannedInChannelError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

API_ID = int(os.environ['API_ID'])
API_HASH = os.environ['API_HASH']
SOURCE_CHANNEL = int(os.environ['SOURCE_CHANNEL'])
SESSION1 = os.environ['SESSION1']
SESSION2 = os.environ['SESSION2']
DELAY = int(os.environ.get('DELAY', '5'))

CHANNELS_ACC1 = [ch.strip() for ch in os.environ['CHANNELS_ACC1'].split(',') if ch.strip()]
CHANNELS_ACC2 = [ch.strip() for ch in os.environ['CHANNELS_ACC2'].split(',') if ch.strip()]


def extract_invite_hash(channel):
    import re
    m = re.match(r'https?://t\.me/\+([A-Za-z0-9_-]+)', channel)
    if m:
        return m.group(1)
    m = re.match(r'https?://t\.me/joinchat/([A-Za-z0-9_-]+)', channel)
    if m:
        return m.group(1)
    return None


async def join_if_needed(client, channel):
    try:
        invite_hash = extract_invite_hash(channel)
        if invite_hash:
            await client(ImportChatInviteRequest(invite_hash))
        else:
            entity = await client.get_entity(channel)
            await client(JoinChannelRequest(entity))
    except Exception:
        pass


async def send_to_channels(client, channels, msg, suffix):
    text = (msg.text or msg.message or '') + suffix
    ok = 0
    for channel in channels:
        await join_if_needed(client, channel)
        try:
            if msg.media:
                await client.send_file(channel, msg.media, caption=text)
            else:
                await client.send_message(channel, text)
            ok += 1
        except FloodWaitError as e:
            if e.seconds <= 60:
                await asyncio.sleep(e.seconds + 3)
                try:
                    if msg.media:
                        await client.send_file(channel, msg.media, caption=text)
                    else:
                        await client.send_message(channel, text)
                    ok += 1
                except Exception:
                    pass
        except (ChatWriteForbiddenError, UserBannedInChannelError):
            pass
        except Exception:
            pass
        await asyncio.sleep(DELAY)
    print(f'[{client.session.__class__.__name__}] Отправлено: {ok}/{len(channels)}')


async def run_client(session_str, channels, suffix, source_entity=None):
    client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError('Session not authorized! Re-generate StringSession.')
    print(f'Клиент запущен: {(await client.get_me()).first_name}')

    if source_entity is None:
        source_entity = await client.get_entity(SOURCE_CHANNEL)

    @client.on(events.NewMessage(chats=source_entity))
    async def handler(event):
        print(f'Новый пост! Рассылаю ({len(channels)} каналов)...')
        await send_to_channels(client, channels, event.message, suffix)

    await client.run_until_disconnected()


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'OK')

    def log_message(self, *args):
        pass


def start_health_server():
    port = int(os.environ.get('PORT', 8080))
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    print(f'Health server on port {port}')
    server.serve_forever()


async def main():
    threading.Thread(target=start_health_server, daemon=True).start()
    await asyncio.gather(
        run_client(SESSION1, CHANNELS_ACC1, '\n\n@nasty_ff'),
        run_client(SESSION2, CHANNELS_ACC2, '\n\n@awardy_bot'),
    )


if __name__ == '__main__':
    asyncio.run(main())
