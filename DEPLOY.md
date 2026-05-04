# Render deploy

This bot runs Telegram forwarding in the cloud, so it continues while the Mac is off.

## Render settings

- Runtime: Python
- Build command: `pip install -r requirements.txt`
- Start command: `python -u bot.py`
- Health check path: `/`

## Environment variables

Required:

- `API_ID`
- `API_HASH`
- `SOURCE_CHANNEL`
- `SESSION1`
- `SESSION2`
- `CHANNELS_ACC1`
- `CHANNELS_ACC2`

Optional:

- `DELAY`, default `5`
- `SEND_LATEST_ON_START`, default `0`

Use fresh Telegram StringSession values for `SESSION1` and `SESSION2`. Do not run the same
session on the Mac and in Render at the same time.

## Local Mac

After Render is confirmed working, stop the local launchd watcher to avoid duplicate sending:

```bash
launchctl bootout gui/501 /Users/possum/Library/LaunchAgents/com.possum.tg-poster.watch.plist
```
