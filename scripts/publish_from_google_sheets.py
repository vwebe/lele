import os
import re
import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
import requests
from google.oauth2.service_account import Credentials

TIMEZONE = "Asia/Phnom_Penh"
DEFAULT_LAYOUT = "post"
DEFAULT_CATEGORY = "blog"
POSTS_DIR = Path("_posts")
IMAGES_DIR = Path("assets/images/auto")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]

PEXELS_API_KEY = os.environ.get("PEXELS_API_KEY", "").strip()


def now_local() -> datetime:
    return datetime.now(ZoneInfo(TIMEZONE))


def slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    text = re.sub(r"[\s_-]+", "-", text)
    return text.strip("-")


def parse_publish_time(value: str):
    value = (value or "").strip()
    if not value:
        return None

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=ZoneInfo(TIMEZONE))
        except ValueError:
            pass

    raise ValueError(
        f"Invalid publish_time format: '{value}'. "
        "Use YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM or YYYY-MM-DD."
    )


def quote_yaml(value: str) -> str:
    value = (value or "").replace('"', '\\"')
    return f'"{value}"'


def split_csv(value: str):
    if not value:
        return []
    return [item.strip() for item in str(value).split(",") if item.strip()]


def dedupe_keep_order(items):
    seen = set()
    out = []
    for item in items:
        key = item.lower().strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item.strip())
    return out


def build_search_keywords(row: dict):
    title = str(row.get("title") or "").strip()
    category = str(row.get("category") or "").strip()
    tags = split_csv(row.get("tags", ""))
    excerpt = str(row.get("excerpt") or "").strip()
    image_query = str(row.get("image_query") or "").strip()

    if image_query:
        return image_query, "manual image_query"

    chunks = []
    if title:
        chunks.append(title)
    if category:
        chunks.append(category)
    chunks.extend(tags)

    if excerpt:
        words = re.findall(r"[A-Za-zÀ-ỹ0-9]{4,}", excerpt)
        chunks.extend(words[:6])

    cleaned = []
    for c in chunks:
        c = re.sub(r"\s+", " ", str(c)).strip()
        if c:
            cleaned.append(c)

    keywords = dedupe_keep_order(cleaned)
    primary = ", ".join(keywords[:6]) if keywords else "travel landscape"
    search_query = " ".join(keywords[:4]) if keywords else "travel landscape"
    return search_query, primary


def fetch_pexels_image(query: str, slug: str):
    if not PEXELS_API_KEY:
        print("PEXELS_API_KEY not set; skipping image fetch.")
        return None, None

    endpoint = "https://api.pexels.com/v1/search"
    headers = {"Authorization": PEXELS_API_KEY}
    params = {"query": query, "per_page": 1, "orientation": "landscape"}

    resp = requests.get(endpoint, headers=headers, params=params, timeout=30)
    if resp.status_code != 200:
        print(f"Pexels API error: HTTP {resp.status_code}")
        return None, None

    data = resp.json()
    photos = data.get("photos") or []
    if not photos:
        print(f"No Pexels result for query: {query}")
        return None, None

    photo = photos[0]
    image_url = (
        photo.get("src", {}).get("large2x")
        or photo.get("src", {}).get("large")
        or photo.get("src", {}).get("original")
    )
    if not image_url:
        print("No usable image URL in Pexels response.")
        return None, None

    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    local_filename = f"{slug}.jpg"
    local_path = IMAGES_DIR / local_filename

    if not local_path.exists():
        img_resp = requests.get(image_url, timeout=60)
        img_resp.raise_for_status()
        local_path.write_bytes(img_resp.content)
        print(f"Downloaded image -> {local_path}")
    else:
        print(f"Image already exists -> {local_path}")

    image_path = f"/assets/images/auto/{local_filename}"
    credit = photo.get("photographer_url") or photo.get("url") or ""
    return image_path, credit


def build_front_matter(
    *,
    title: str,
    publish_dt: datetime,
    layout: str,
    category: str,
    tags: list,
    excerpt: str,
    description: str,
    image: str,
):
    lines = [
        "---",
        f"layout: {layout}",
        f"title: {quote_yaml(title)}",
        f"date: {publish_dt.strftime('%Y-%m-%d %H:%M:%S %z')}",
    ]

    if category:
        lines.append(f"categories: [{category}]")
    if tags:
        lines.append("tags: [" + ", ".join(tags) + "]")
    if excerpt:
        lines.append(f"excerpt: {quote_yaml(excerpt)}")
    if description:
        lines.append(f"description: {quote_yaml(description)}")
    if image:
        lines.append(f"image: {quote_yaml(image)}")

    lines.append("---")
    return "\n".join(lines)


def make_post_content(row: dict, publish_dt: datetime):
    title = (row.get("title") or "").strip()
    if not title:
        raise ValueError("Missing title")

    slug = (row.get("slug") or "").strip() or slugify(title)
    layout = (row.get("layout") or DEFAULT_LAYOUT).strip() or DEFAULT_LAYOUT
    category = (row.get("category") or DEFAULT_CATEGORY).strip()
    tags = split_csv(row.get("tags", ""))
    excerpt = (row.get("excerpt") or "").strip()
    description = (row.get("description") or "").strip()
    image = (row.get("image") or "").strip()
    content = (row.get("content") or "").strip()

    if not content:
        raise ValueError("Missing content")

    ai_keywords = ""
    image_credit = ""

    if not image:
        image_query, ai_keywords = build_search_keywords(row)
        image, image_credit = fetch_pexels_image(image_query, slug)

    filename = f"{publish_dt.strftime('%Y-%m-%d')}-{slug}.md"

    front_matter = build_front_matter(
        title=title,
        publish_dt=publish_dt,
        layout=layout,
        category=category,
        tags=tags,
        excerpt=excerpt,
        description=description,
        image=image or "",
    )

    image_block = f"![{title}]({image})\n\n" if image else ""
    credit_block = ""
    if image_credit:
        credit_block = f"\n\n_Image source: Pexels ({image_credit})_\n"

    keyword_block = ""
    if ai_keywords:
        keyword_block = f"<!-- ai_keywords: {ai_keywords} -->\n\n"

    full_content = f"{front_matter}\n\n{keyword_block}{image_block}{content}{credit_block}\n"
    return filename, full_content


def get_google_client():
    raw_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    info = json.loads(raw_json)
    credentials = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(credentials)


def get_worksheet():
    spreadsheet_id = os.environ["GOOGLE_SHEETS_SPREADSHEET_ID"]
    worksheet_name = os.environ["GOOGLE_SHEETS_WORKSHEET"]
    client = get_google_client()
    spreadsheet = client.open_by_key(spreadsheet_id)
    return spreadsheet.worksheet(worksheet_name)


def update_sheet_row(worksheet, headers, row_index, data: dict):
    for key, value in data.items():
        if key in headers:
            col_index = headers.index(key) + 1
            worksheet.update_cell(row_index, col_index, value)


def main():
    POSTS_DIR.mkdir(exist_ok=True)
    worksheet = get_worksheet()
    rows = worksheet.get_all_records()
    headers = worksheet.row_values(1)

    local_now = now_local()
    print(f"Current local time: {local_now.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    published_count = 0
    skipped_count = 0

    for row_index, row in enumerate(rows, start=2):
        title_raw = str(row.get("title") or "").strip()
        status = str(row.get("status", "")).strip().lower()
        publish_time_raw = str(row.get("publish_time") or row.get("date") or "").strip()
        content_raw = str(row.get("content") or "").strip()

        print(
            f"Row {row_index}: "
            f"title={title_raw!r}, status={status!r}, publish_time={publish_time_raw!r}"
        )

        if status != "ready":
            print(f"Row {row_index}: skipped, status is '{status}' not ready")
            skipped_count += 1
            continue

        if not title_raw:
            print(f"Row {row_index}: skipped, missing title")
            skipped_count += 1
            continue

        if not content_raw:
            print(f"Row {row_index}: skipped, missing content")
            skipped_count += 1
            continue

        if not publish_time_raw:
            print(f"Row {row_index}: skipped, missing publish_time/date")
            skipped_count += 1
            continue

        try:
            publish_dt = parse_publish_time(publish_time_raw)
        except ValueError as e:
            print(f"Row {row_index}: skipped, {e}")
            skipped_count += 1
            continue

        if publish_dt > local_now:
            print(
                f"Row {row_index}: waiting until "
                f"{publish_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}"
            )
            skipped_count += 1
            continue

        try:
            filename, post_body = make_post_content(row, publish_dt)
        except Exception as e:
            print(f"Row {row_index}: skipped, build error -> {e}")
            skipped_count += 1
            continue

        filepath = POSTS_DIR / filename

        if filepath.exists():
            print(f"Row {row_index}: file already exists -> {filename}")
        else:
            filepath.write_text(post_body, encoding="utf-8")
            print(f"Row {row_index}: created -> {filename}")

        published_at = local_now.strftime("%Y-%m-%d %H:%M:%S")
        update_sheet_row(
            worksheet,
            headers,
            row_index,
            {
                "status": "published",
                "published_at": published_at,
                "output_file": filename,
            },
        )

        published_count += 1

    print(f"Done. Published: {published_count}, Skipped: {skipped_count}")


if __name__ == "__main__":
    main()
