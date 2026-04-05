import os
import re
import json
import asyncio
import logging
import urllib.parse
import shutil
from datetime import datetime
from pathlib import Path
from dotenv import dotenv_values
from telethon import TelegramClient
from telethon.sessions.sqlite import SQLiteSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class LockableSQLiteSession(SQLiteSession):
    _busy_timeout_set = False

    def _cursor(self):
        cursor = super()._cursor()
        if not LockableSQLiteSession._busy_timeout_set:
            self._conn.execute("PRAGMA busy_timeout = 5000")
            LockableSQLiteSession._busy_timeout_set = True
        return cursor


config = dotenv_values("config.env")

API_ID = os.environ.get("TELEGRAM_API_ID") or config.get("TELEGRAM_API_ID")
API_HASH = os.environ.get("TELEGRAM_API_HASH") or config.get("TELEGRAM_API_HASH")
PHONE = os.environ.get("TELEGRAM_PHONE") or config.get("TELEGRAM_PHONE")
CHANNEL_IDS = [
    int(x.strip())
    for x in (
        os.environ.get("CHANNEL_IDS") or config.get("CHANNEL_IDS", "1769374379")
    ).split(",")
]
OUTPUT_DIR = Path(
    os.environ.get("OUTPUT_DIR") or config.get("OUTPUT_DIR", "/app/output")
)
MESSAGE_LIMIT = int(
    os.environ.get("MESSAGE_LIMIT") or config.get("MESSAGE_LIMIT", "1000")
)

SESSION_FILE = OUTPUT_DIR / "session.session"

M3U_URL_PATTERN = re.compile(r'https?://[^\s<>"{}|\\^`\[\]]+\.m3u[8]?', re.IGNORECASE)


def extract_credentials(text):
    results = {"m3u_urls": [], "xtream_credentials": []}

    # Method 1: Direct .m3u8 URLs (rare in this channel)
    m3u_urls = M3U_URL_PATTERN.findall(text)

    # Method 2: "Link M3U" format
    m3u_link_match = re.findall(r"Link M3U.*?http[^\s]+", text)
    for match in m3u_link_match:
        urls = re.findall(r"http[^\s]+", match)
        m3u_urls.extend(urls)

    # Method 3: Extract from get.php URLs (MAIN METHOD for this channel)
    # Format: http://host:port/get.php?username=X&password=Y
    getphp_urls = re.findall(
        r"(http[^\s]+get\.php\?username=[^\s&]+&password=[^\s&]+)", text, re.IGNORECASE
    )
    for url in getphp_urls:
        url = url.split()[0]  # Clean trailing characters
        try:
            parsed_url = urllib.parse.urlparse(url)
            params = urllib.parse.parse_qs(parsed_url.query)
            username = params.get("username", [""])[0]
            password = params.get("password", [""])[0]

            if username and password:
                # Decode URL-encoded password (e.g., %40 -> @)
                password = urllib.parse.unquote(password)

                # Build server URL
                server = f"http://{parsed_url.netloc}"

                results["m3u_urls"].append(url)
                results["xtream_credentials"].append(
                    {
                        "server": server,
                        "username": username,
                        "password": password,
                        "source": "getphp_url",
                    }
                )
        except Exception:
            pass

    # Method 4: WPLAY format from Riacho Grande channel
    # Format: USERNAMES ➨ username | PASSWORD ➨ password
    wplay_users = re.findall(r"USERNAMES\s*➨\s*(\S+)", text)
    wplay_pass = re.findall(r"PASSWORD\s*➨\s*(\S+)", text)

    if wplay_users and wplay_pass:
        for i, username in enumerate(wplay_users):
            if i < len(wplay_pass):
                password = wplay_pass[i]
                if len(username) > 2 and len(password) > 2:
                    results["xtream_credentials"].append(
                        {
                            "server": "http://wplay.unknown",
                            "username": username,
                            "password": password,
                            "source": "wplay_format",
                        }
                    )

    results["m3u_urls"] = list(set(m3u_urls))

    text_normalized = text.replace("\n", " ").replace("\r", " ")

    server = None
    port = None
    username = None
    password = None

    server_match = re.search(
        r"(?:Servidor|Server|servidor|http://|https://)(?:\s*Real\s*)?\s*[❥|:]\s*(https?://)?([a-zA-Z0-9.-]+)",
        text,
        re.IGNORECASE,
    )
    if server_match:
        server = server_match.group(2)

    port_match = re.search(
        r"(?:Porta|Porta\s*❥\s*|port)\s*[:|❥]\s*(\d+)", text, re.IGNORECASE
    )
    if port_match:
        port = port_match.group(1)

    user_match = re.search(
        r"(?:Usuário|Usuário\s*❥\s*|Usuário\s*❥|user|username|user:)\s*[:|❥]\s*(\S+)",
        text,
        re.IGNORECASE,
    )
    if user_match:
        username = user_match.group(1).strip()
        username = username.split("&")[0]

    pass_match = re.search(
        r"(?:Senha|Senha\s*❥\s*|Senha\s*❥|password|pass|senha:)\s*[:|❥]\s*(\S+)",
        text,
        re.IGNORECASE,
    )
    if pass_match:
        password = pass_match.group(1).strip()
        password = password.split("&")[0]

    if server and username and password:
        port_str = f":{port}" if port else ""
        server_url = f"http://{server}{port_str}"

        results["xtream_credentials"].append(
            {
                "server": server_url,
                "username": username,
                "password": password,
                "source": "structured",
            }
        )

    xtream_url_match = re.search(
        r"http[s]?://([a-zA-Z0-9.-]+)(?::(\d+))?/get\.php\?username=(\S+)&password=(\S+)",
        text,
        re.IGNORECASE,
    )
    if xtream_url_match:
        host, port, user, passwd = xtream_url_match.groups()
        port_str = f":{port}" if port else ""
        user = user.split("&")[0]
        passwd = passwd.split("&")[0]
        results["xtream_credentials"].append(
            {
                "server": f"http://{host}{port_str}",
                "username": user,
                "password": passwd,
                "source": "m3u_url",
            }
        )

    simple_pattern = re.findall(
        r"(https?://[a-zA-Z0-9.-]+:\d+)[^\s]*\s+(\w+)\s+(\w+)", text
    )
    for match in simple_pattern:
        host_port, user, passwd = match
        if len(user) > 2 and len(passwd) > 2:
            results["xtream_credentials"].append(
                {
                    "server": host_port,
                    "username": user,
                    "password": passwd,
                    "source": "simple",
                }
            )

    seen = set()
    unique = []
    for cred in results["xtream_credentials"]:
        key = (
            cred.get("server", ""),
            cred.get("username", ""),
            cred.get("password", ""),
        )
        if (
            key not in seen
            and key[0]
            and key[1]
            and key[2]
            and len(key[1]) > 2
            and len(key[2]) > 2
        ):
            seen.add(key)
            unique.append(cred)
    results["xtream_credentials"] = unique

    return results


def extract_credentials_from_text(text):
    """Extract credentials from raw text content (like .txt files)"""
    results = {"m3u_urls": [], "xtream_credentials": []}

    # Get .m3u8 URLs
    m3u_urls = M3U_URL_PATTERN.findall(text)
    results["m3u_urls"].extend(m3u_urls)

    # Get get.php URLs with credentials
    getphp_urls = re.findall(
        r"(http[^\s]+get\.php\?username=[^\s&]+&password=[^\s&]+)", text, re.IGNORECASE
    )
    for url in getphp_urls:
        url = url.split()[0]
        try:
            parsed_url = urllib.parse.urlparse(url)
            params = urllib.parse.parse_qs(parsed_url.query)
            username = params.get("username", [""])[0]
            password = params.get("password", [""])[0]
            if username and password:
                password = urllib.parse.unquote(password).split("/")[0]
                results["xtream_credentials"].append(
                    {
                        "server": f"http://{parsed_url.netloc}",
                        "username": username,
                        "password": password,
                        "source": "txt_file",
                    }
                )
        except:
            pass

    # Get server:port|user|pass format
    pipe_pattern = re.findall(
        r"([a-zA-Z0-9.-]+):(\d+)\s*[|]\s*(\S+)\s*[|]\s*(\S+)", text
    )
    for match in pipe_pattern:
        host, port, user, passwd = match
        if len(user) > 2 and len(passwd) > 2:
            results["xtream_credentials"].append(
                {
                    "server": f"http://{host}:{port}",
                    "username": user,
                    "password": passwd,
                    "source": "txt_pipe",
                }
            )

    # Get user:pass@server format
    user_pass_pattern = re.findall(r"(\S+):(\S+)@([a-zA-Z0-9.-]+)", text)
    for match in user_pass_pattern:
        user, passwd, host = match
        if len(user) > 2 and len(passwd) > 2:
            results["xtream_credentials"].append(
                {
                    "server": f"http://{host}",
                    "username": user,
                    "password": passwd,
                    "source": "txt_userpass",
                }
            )

    return results


async def download_and_process_txt(
    client, message, txt_files_dir, creds_with_dates, processed_txt_files, logger
):
    """Download a single .txt file and extract credentials"""
    try:
        # Check if already processed
        file_key = f"{message.chat_id}_{message.id}"
        if file_key in processed_txt_files:
            return []
        processed_txt_files.add(file_key)

        # Get filename
        filename = None
        for attr in message.document.attributes:
            if hasattr(attr, "file_name"):
                filename = attr.file_name
                break

        if not filename or not filename.endswith(".txt"):
            return None

        # Skip COMBO files (they have no server info, just username:password)
        if "COMBO" in filename.upper():
            logger.info(f"Skipping COMBO file: {filename}")
            return None

        # Skip files larger than 2MB
        if message.document.size and message.document.size > 2 * 1024 * 1024:
            logger.info(f"Skipping large .txt file ({message.document.size} bytes)")
            return None

        # Download file
        file_path = txt_files_dir / f"{message.chat_id}_{message.id}_{filename}"
        await client.download_media(message.media, file=str(file_path))

        # Read and parse
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            txt_content = f.read()

        txt_creds = extract_credentials_from_text(txt_content)

        # Add credentials
        msg_date = message.date.isoformat() if message.date else None
        for cred in txt_creds.get("xtream_credentials", []):
            key = (
                cred.get("server", ""),
                cred.get("username", ""),
                cred.get("password", ""),
            )
            if key not in creds_with_dates:
                creds_with_dates[key] = {
                    "server": cred.get("server", ""),
                    "username": cred.get("username", ""),
                    "password": cred.get("password", ""),
                    "first_seen": msg_date,
                    "source_message_id": message.id,
                    "source_file": filename,
                }

        logger.info(
            f"Extracted {len(txt_creds.get('xtream_credentials', []))} credentials from {filename}"
        )
        return txt_creds.get("m3u_urls", [])
    except Exception as e:
        logger.warning(f"Error processing txt file: {e}")
        return None


async def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting Telegram scraper for channel IDs: {CHANNEL_IDS}")

    client = TelegramClient(LockableSQLiteSession(str(SESSION_FILE)), API_ID, API_HASH)

    await client.start(PHONE)

    logger.info("Client started, looking up channels...")

    # Look up all channels by ID
    channels = {}
    async for dialog in client.iter_dialogs():
        if dialog.is_channel and dialog.entity.id in CHANNEL_IDS:
            channels[dialog.entity.id] = dialog.entity
            logger.info(f"Found channel: {dialog.name} (ID: {dialog.entity.id})")

    # Check which channels were not found
    for channel_id in CHANNEL_IDS:
        if channel_id not in channels:
            logger.warning(f"Channel ID {channel_id} not found!")

    if not channels:
        logger.error("No channels found!")
        await client.disconnect()
        return

    # Create directory for txt files
    txt_files_dir = OUTPUT_DIR / "txt_files"
    txt_files_dir.mkdir(parents=True, exist_ok=True)

    # Track which txt files we've already processed
    processed_txt_files = set()

    messages_data = []
    all_credentials = {"m3u_urls": [], "xtream_credentials": []}

    # Track credentials with timestamps - keep oldest (first seen)
    creds_with_dates = {}  # key -> {cred, date, msg_id}

    # First pass: collect all messages
    all_messages = []
    txt_file_messages = []

    for channel_id, channel in channels.items():
        logger.info(f"Scraping channel ID {channel_id}...")

        async for message in client.iter_messages(channel, limit=MESSAGE_LIMIT):
            msg_date = message.date.isoformat() if message.date else None

            # Store for text processing
            all_messages.append((channel_id, message, msg_date))

            # Check for .txt file attachments
            if hasattr(message, "document") and message.document:
                if message.document.mime_type == "text/plain":
                    # Skip files larger than 2MB
                    if (
                        message.document.size
                        and message.document.size > 2 * 1024 * 1024
                    ):
                        continue
                    # Get filename
                    filename = None
                    for attr in message.document.attributes:
                        if hasattr(attr, "file_name"):
                            filename = attr.file_name
                            break
                    if filename and filename.endswith(".txt"):
                        txt_file_messages.append(
                            (channel_id, message, msg_date, filename)
                        )

    logger.info(
        f"Found {len(all_messages)} messages and {len(txt_file_messages)} .txt files"
    )

    # Process text messages (sequential is fine - fast)
    messages_data = []
    m3u_urls = []

    for channel_id, message, msg_date in all_messages:
        if message.text:
            msg_data = {
                "id": message.id,
                "date": msg_date,
                "channel_id": channel_id,
                "text": message.text[:2000],
            }
            creds = extract_credentials(message.text)
            msg_data["credentials"] = creds
            messages_data.append(msg_data)
            m3u_urls.extend(creds["m3u_urls"])

            # Track credentials
            for cred in creds.get("xtream_credentials", []):
                key = (
                    cred.get("server", ""),
                    cred.get("username", ""),
                    cred.get("password", ""),
                )
                if key not in creds_with_dates:
                    creds_with_dates[key] = {
                        "server": cred.get("server", ""),
                        "username": cred.get("username", ""),
                        "password": cred.get("password", ""),
                        "first_seen": msg_date,
                        "source_message_id": message.id,
                    }

    # Process .txt files in PARALLEL
    logger.info(f"Downloading {len(txt_file_messages)} .txt files in parallel...")

    async def download_one(item):
        channel_id, message, msg_date, filename = item
        return await download_and_process_txt(
            client,
            message,
            txt_files_dir,
            creds_with_dates,
            processed_txt_files,
            logger,
        )

    # Run downloads in parallel (limited concurrency to avoid overwhelming)
    semaphore = asyncio.Semaphore(10)  # Max 10 concurrent downloads

    async def download_with_limit(item):
        async with semaphore:
            return await download_one(item)

    tasks = [download_with_limit(item) for item in txt_file_messages]
    results = await asyncio.gather(*tasks)

    # Collect M3U URLs from txt files
    for result in results:
        if result:
            m3u_urls.extend(result)

    all_credentials["m3u_urls"] = list(set(m3u_urls))

    # Also add M3U URLs as Xtream format (extract credentials from URL)
    for m3u_url in all_credentials["m3u_urls"]:
        try:
            from urllib.parse import urlparse, parse_qs

            parsed = urlparse(m3u_url)
            params = parse_qs(parsed.query)
            username = params.get("username", [""])[0]
            password = params.get("password", [""])[0]
            if username and password:
                key = (parsed.netloc, username, password.split("/")[0])
                if key not in creds_with_dates:
                    creds_with_dates[key] = {
                        "server": f"http://{parsed.netloc}",
                        "username": username,
                        "password": password.split("/")[0],
                        "first_seen": None,
                        "source_message_id": None,
                    }
        except:
            pass

    # Convert to list and sort by first_seen (newest first - most recent at beginning)
    # Entries with dates come first (sorted newest to oldest), None goes at end
    def sort_key(x):
        date = x.get("first_seen")
        if date is None:
            return ("",)  # Empty string sorts after actual dates
        return (date,)  # Sort by date (will be sorted descending)

    xtream_list = list(creds_with_dates.values())
    xtream_list.sort(key=sort_key, reverse=True)

    unique_xtream = []
    seen = set()
    for cred in xtream_list:
        key = (
            cred.get("server", ""),
            cred.get("username", ""),
            cred.get("password", ""),
        )
        if (
            key not in seen
            and key[0]
            and key[1]
            and key[2]
            and len(key[1]) > 2
            and len(key[2]) > 2
        ):
            seen.add(key)
            unique_xtream.append(cred)
    all_credentials["xtream_credentials"] = unique_xtream

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    messages_file = OUTPUT_DIR / f"messages_{timestamp}.json"
    with open(messages_file, "w", encoding="utf-8") as f:
        json.dump(messages_data, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved {len(messages_data)} messages to {messages_file}")

    credentials_file = OUTPUT_DIR / f"credentials_{timestamp}.json"
    with open(credentials_file, "w", encoding="utf-8") as f:
        json.dump(all_credentials, f, indent=2, ensure_ascii=False)
    logger.info(
        f"Found {len(all_credentials['m3u_urls'])} M3U URLs and {len(all_credentials['xtream_credentials'])} Xtream credentials"
    )

    channel_label = f"Telegram Channels {CHANNEL_IDS}"

    sources_file = OUTPUT_DIR / "sources.json"
    sources = {"sources": []}

    for m3u_url in all_credentials["m3u_urls"]:
        sources["sources"].append(
            {"name": f"M3U from {channel_label}", "type": "m3u", "url": m3u_url}
        )

    for cred in all_credentials["xtream_credentials"]:
        sources["sources"].append(
            {
                "name": f"Xtream from {channel_label}",
                "type": "xtream",
                "server": cred.get("server", ""),
                "username": cred.get("username", ""),
                "password": cred.get("password", ""),
            }
        )

    with open(sources_file, "w", encoding="utf-8") as f:
        json.dump(sources, f, indent=2)
    logger.info(f"Saved sources config to {sources_file}")

    # Generate clean_credentials.json with timestamps
    clean_output = {
        "xtream_credentials": unique_xtream,
        "metadata": {
            "scrape_date": datetime.now().isoformat(),
            "total_xtream": len(unique_xtream),
            "unique_servers": len(set(c["server"] for c in unique_xtream)),
        },
    }

    clean_file = OUTPUT_DIR / "clean_credentials.json"
    with open(clean_file, "w", encoding="utf-8") as f:
        json.dump(clean_output, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved clean credentials to {clean_file}")

    # Load existing sources_clean.json if present (append behavior)
    sources_clean_file = OUTPUT_DIR / "sources_clean.json"
    existing_sources = []
    if sources_clean_file.exists():
        with open(sources_clean_file, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
            existing_sources = existing_data.get("sources", [])
        logger.info(f"Loaded {len(existing_sources)} existing sources for append")

    # Build dedup set from existing: "server:username:password"
    existing_keys = {
        f"{s['server']}:{s['username']}:{s['password']}" for s in existing_sources
    }

    # Append only new unique credentials
    new_count = 0
    for cred in unique_xtream:
        key = f"{cred.get('server', '')}:{cred.get('username', '')}:{cred.get('password', '')}"
        if key not in existing_keys:
            existing_keys.add(key)
            existing_sources.append(
                {
                    "name": f"Xtream from {channel_label}",
                    "type": "xtream",
                    "server": cred.get("server", ""),
                    "username": cred.get("username", ""),
                    "password": cred.get("password", ""),
                    "first_seen": cred.get("first_seen"),
                }
            )
            new_count += 1

    # Sort by first_seen ascending (oldest first)
    existing_sources.sort(key=lambda x: x.get("first_seen", ""))

    # Save updated file
    sources_clean = {"sources": existing_sources}
    with open(sources_clean_file, "w", encoding="utf-8") as f:
        json.dump(sources_clean, f, indent=2)
    logger.info(
        f"Updated sources_clean.json: {new_count} new, {len(existing_sources)} total"
    )

    latest_file = OUTPUT_DIR / "latest_credentials.json"
    with open(latest_file, "w", encoding="utf-8") as f:
        json.dump(all_credentials, f, indent=2, ensure_ascii=False)
    logger.info(f"Updated latest credentials file")

    # Clean up downloaded txt files to save space
    if txt_files_dir.exists():
        shutil.rmtree(txt_files_dir)
        logger.info(f"Cleaned up {txt_files_dir}")

    await client.disconnect()
    logger.info("Scraper finished successfully")


if __name__ == "__main__":
    asyncio.run(main())
