import json
import os
import re
from pathlib import Path
from datetime import datetime, timezone

import gspread
from dateutil import parser as date_parser
from google.oauth2.service_account import Credentials

POSTS_DIR = Path("_posts")
POSTS_DIR.mkdir(exist_ok=True)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

REQUIRED_COLUMNS = [
    "title",
    "slug",
    "date",
    "category",
    "tags",
    "excerpt",
    "content",
    "status",
    "published_at",
    "output_file",
]


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def load_client():
    raw_json = require_env("GOOGLE_SERVICE_ACCOUNT_JSON")
    creds_info = json.loads(raw_json)
    credentials = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return gspread.authorize(credentials)


def normalize_header(value: str) -> str:
    return re.sub(r"\s+", "_", str(value or "").strip().lower())


def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def parse_sheet_date(value: str):
    value = str(value or "").strip()
    if not value:
        return None
    dt = date_parser.parse(value)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.date()


def ensure_headers(worksheet):
    headers = worksheet.row_values(1)
    normalized = [normalize_header(h) for h in headers]
    if not headers:
        worksheet.append_row(REQUIRED_COLUMNS)
        return REQUIRED_COLUMNS
    for col in REQUIRED_COLUMNS:
        if col not in normalized:
            headers.append(col)
            normalized.append(col)
    if headers != worksheet.row_values(1):
        worksheet.update("A1", [headers])
    return headers


def load_records(worksheet):
    headers = ensure_headers(worksheet)
    rows = worksheet.get_all_values()
    if len(rows) <= 1:
        return headers, []
    normalized = [normalize_header(h) for h in rows[0]]
    records = []
    for idx, raw in enumerate(rows[1:], start=2):
        row = dict(zip(normalized, raw + [""] * (len(normalized) - len(raw))))
        row["_row_number"] = idx
        records.append(row)
    return rows[0], records


def build_front_matter(title, date_str, category, tags, excerpt, content, layout="post"):
    fm = [
        "---",
        f"layout: {layout}",
        f'title: "{title.replace(chr(34), chr(39))}"',
        f"date: {date_str} 08:00:00 +0700",
    ]
    if category:
        fm.append(f"categories: [{category}]")
    if tags:
        fm.append("tags: [" + ", ".join(tags) + "]")
    if excerpt:
        fm.append(f'excerpt: "{excerpt.replace(chr(34), chr(39))}"')
    fm.append("---")
    fm.append("")
    fm.append(content.strip())
    fm.append("")
    return "\n".join(fm)


def main():
    client = load_client()
    spreadsheet_id = require_env("GOOGLE_SHEETS_SPREADSHEET_ID")
    worksheet_name = os.getenv("GOOGLE_SHEETS_WORKSHEET", "Posts").strip() or "Posts"

    spreadsheet = client.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.worksheet(worksheet_name)

    headers, records = load_records(worksheet)
    now_local = datetime.now()
    published_count = 0

    for record in records:
        status = str(record.get("status", "")).strip().lower()
        if status != "ready":
            continue

        title = str(record.get("title", "")).strip()
        content = str(record.get("content", "")).strip()
        if not title or not content:
            print(f"Skipping row {record['_row_number']}: missing title/content")
            continue

        slug = str(record.get("slug", "")).strip() or slugify(title)
        category = str(record.get("category", "")).strip()
        tags = [t.strip() for t in str(record.get("tags", "")).split(",") if t.strip()]
        excerpt = str(record.get("excerpt", "")).strip()
        date_raw = str(record.get("date", "")).strip()

        publish_date = parse_sheet_date(date_raw) if date_raw else now_local.date()
        if publish_date and publish_date > now_local.date():
            print(f"Skipping future row {record['_row_number']}: {publish_date}")
            continue

        date_str = publish_date.isoformat() if publish_date else now_local.strftime("%Y-%m-%d")
        output_file = f"{date_str}-{slug}.md"
        output_path = POSTS_DIR / output_file

        if output_path.exists():
            print(f"File already exists, marking published: {output_file}")
        else:
            body = build_front_matter(
                title=title,
                date_str=date_str,
                category=category,
                tags=tags,
                excerpt=excerpt,
                content=content,
                layout="post",
            )
            output_path.write_text(body, encoding="utf-8")
            print(f"Created {output_file}")

        row_num = record["_row_number"]
        header_index = {normalize_header(h): i + 1 for i, h in enumerate(headers)}
        worksheet.update_cell(row_num, header_index["status"], "published")
        worksheet.update_cell(row_num, header_index["published_at"], now_local.strftime("%Y-%m-%d %H:%M:%S"))
        worksheet.update_cell(row_num, header_index["output_file"], output_file)
        published_count += 1

    print(f"Done. Published or confirmed {published_count} row(s).")


if __name__ == "__main__":
    main()
