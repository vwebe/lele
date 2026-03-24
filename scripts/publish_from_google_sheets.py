#!/usr/bin/env python3
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials

LOCAL_TZ = ZoneInfo("Asia/Jakarta")
DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
]
GRACE_MINUTES = int(os.getenv("PUBLISH_GRACE_MINUTES", "15"))


def log(msg: str) -> None:
    print(msg, flush=True)


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def now_local() -> datetime:
    return datetime.now(LOCAL_TZ)


def parse_publish_time(raw: str) -> datetime:
    raw = (raw or "").strip()
    if not raw:
        raise ValueError("publish_time is empty")

    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=LOCAL_TZ)
        return dt.astimezone(LOCAL_TZ)
    except Exception:
        pass

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=LOCAL_TZ)
        except ValueError:
            continue

    raise ValueError(f"Unrecognized publish_time format: {raw}")


def slugify(text: str) -> str:
    text = (text or "").lower().strip()
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text or "untitled-post"


def yaml_escape(value: str) -> str:
    value = (value or "").replace('"', '\\"')
    return f'"{value}"'


def normalize_multiline(text: str) -> str:
    return (text or "").replace("\r\n", "\n").strip()


def front_matter(row: dict, post_date: datetime, slug: str) -> str:
    tags_raw = (row.get("tags") or "").strip()
    tags = [t.strip() for t in tags_raw.split(",") if t.strip()]
    fm = [
        "---",
        f"title: {yaml_escape((row.get('title') or '').strip())}",
        f"date: {post_date.strftime('%Y-%m-%d %H:%M:%S %z')}",
        f"slug: {yaml_escape(slug)}",
        f"category: {yaml_escape((row.get('category') or '').strip())}",
        "tags:",
    ]
    if tags:
        fm.extend([f"  - {yaml_escape(t)}" for t in tags])
    else:
        fm.append("  - travel")

    for key in ["excerpt", "description", "image", "language"]:
        value = (row.get(key) or "").strip()
        if value:
            fm.append(f"{key}: {yaml_escape(value)}")

    fm.append("---")
    return "\n".join(fm)


def ensure_dirs() -> None:
    Path("_posts").mkdir(parents=True, exist_ok=True)
    Path("assets/images/auto").mkdir(parents=True, exist_ok=True)


def post_path(post_date: datetime, slug: str) -> Path:
    return Path("_posts") / f"{post_date.strftime('%Y-%m-%d')}-{slug}.md"


def write_post(row: dict, publish_dt: datetime) -> str:
    ensure_dirs()
    title = (row.get("title") or "").strip()
    slug = ((row.get("slug") or "").strip()) or slugify(title)
    content = normalize_multiline(row.get("content") or row.get("prompt") or "")

    if not title:
        raise ValueError("title is empty")
    if not content:
        raise ValueError("content/prompt is empty")

    path = post_path(publish_dt, slug)
    body = front_matter(row, publish_dt, slug) + "\n\n" + content + "\n"

    if path.exists():
        log(f"Post already exists, overwriting: {path}")
    else:
        log(f"Creating post: {path}")

    path.write_text(body, encoding="utf-8")
    return str(path)


def open_sheet():
    creds_json = require_env("GOOGLE_SERVICE_ACCOUNT_JSON")
    spreadsheet_id = require_env("GOOGLE_SHEETS_SPREADSHEET_ID")
    worksheet_name = require_env("GOOGLE_SHEETS_WORKSHEET")

    info = json.loads(creds_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet_name)
    return ws


def update_row_status(ws, row_number: int, header_index: dict, status: str, output_file: str = "") -> None:
    updates = []
    now_str = now_local().strftime("%Y-%m-%d %H:%M:%S")
    if "status" in header_index:
        updates.append({"range": gspread.utils.rowcol_to_a1(row_number, header_index["status"]), "values": [[status]]})
    if "published_at" in header_index:
        updates.append({"range": gspread.utils.rowcol_to_a1(row_number, header_index["published_at"]), "values": [[now_str]]})
    if output_file and "output_file" in header_index:
        updates.append({"range": gspread.utils.rowcol_to_a1(row_number, header_index["output_file"]), "values": [[output_file]]})

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")


def main() -> int:
    log(f"Current local time: {now_local().strftime('%Y-%m-%d %H:%M:%S %Z')}")
    log(f"Grace window: {GRACE_MINUTES} minutes")

    ws = open_sheet()
    rows = ws.get_all_records()
    headers = ws.row_values(1)
    header_index = {h.strip().lower(): i + 1 for i, h in enumerate(headers)}

    if not rows:
        log("No rows found.")
        return 0

    current = now_local()
    published_count = 0

    for idx, row in enumerate(rows, start=2):
        normalized = {str(k).strip().lower(): v for k, v in row.items()}
        title = str(normalized.get("title", "")).strip()
        status = str(normalized.get("status", "")).strip().lower()
        publish_time_raw = str(normalized.get("publish_time", "")).strip()

        log(f"Row {idx}: title='{title}', status='{status}', publish_time='{publish_time_raw}'")

        if status != "ready":
            log(f"Row {idx}: skipped, status is '{status}' not ready")
            continue

        if not title:
            log(f"Row {idx}: skipped, title is empty")
            continue

        if not publish_time_raw:
            log(f"Row {idx}: skipped, publish_time is empty")
            continue

        try:
            publish_dt = parse_publish_time(publish_time_raw)
        except Exception as e:
            log(f"Row {idx}: skipped, invalid publish_time: {e}")
            continue

        if publish_dt > current:
            log(f"Row {idx}: waiting until {publish_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            continue

        too_old_cutoff = current - timedelta(minutes=GRACE_MINUTES)
        if publish_dt < too_old_cutoff and os.getenv("PUBLISH_OLD_READY_ROWS", "true").lower() != "true":
            log(f"Row {idx}: skipped, older than grace window")
            continue

        try:
            output_file = write_post(normalized, publish_dt)
            update_row_status(ws, idx, header_index, "published", output_file)
            log(f"Row {idx}: READY TO PUBLISH")
            log(f"Row {idx}: published -> {output_file}")
            published_count += 1
        except Exception as e:
            log(f"Row {idx}: ERROR: {e}")

    log(f"Done. Published {published_count} post(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
