#!/usr/bin/env python3
"""Publish Jekyll posts from Google Sheets.

Required GitHub secrets / variables:
- secrets.GOOGLE_SERVICE_ACCOUNT_JSON
- vars.GOOGLE_SHEETS_SPREADSHEET_ID
- vars.GOOGLE_SHEETS_WORKSHEET, default Posts
- secrets.LLM_API_KEY, optional if every row already has content
- secrets.PEXELS_API_KEY, optional

Sheet columns supported:
title, slug, publish_time, category, tags, excerpt, description, image_query,
image, prompt, language, min_words, content, status, published_at, output_file, post_url
"""

import json
import os
import random
import re
import sys
import time
import traceback
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import gspread
import requests
from google.oauth2.service_account import Credentials
from openai import OpenAI

def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    try:
        return int(str(raw).strip()) if str(raw).strip() else default
    except ValueError:
        print(f"Invalid integer for {name}={raw!r}; using {default}.", flush=True)
        return default


TIMEZONE = os.environ.get("TIMEZONE", "Asia/Jakarta") or "Asia/Jakarta"
SITE_URL = (os.environ.get("SITE_URL", "https://travel.zynelion.com") or "https://travel.zynelion.com").rstrip("/")
DEFAULT_LAYOUT = "post"
DEFAULT_CATEGORY = "travel"
DEFAULT_LANGUAGE = "id"
DEFAULT_MIN_WORDS = env_int("DEFAULT_MIN_WORDS", 1200)
MIN_ACCEPTABLE_WORDS = env_int("MIN_ACCEPTABLE_WORDS", 700)
PUBLISH_INTERVAL_MINUTES = env_int("PUBLISH_INTERVAL_MINUTES", 90)
MAX_POSTS_PER_RUN = env_int("MAX_POSTS_PER_RUN", 1)
LLM_RETRIES = env_int("LLM_RETRIES", 3)

POSTS_DIR = Path("_posts")
IMAGES_DIR = Path("assets/images/auto")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

PEXELS_API_KEY = os.environ.get("PEXELS_API_KEY", "").strip()
LLM_API_KEY = os.environ.get("LLM_API_KEY", "").strip()
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "https://openrouter.ai/api/v1").strip()
LLM_MODEL = os.environ.get("LLM_MODEL", "openrouter/auto").strip()
LLM_SITE_URL = os.environ.get("LLM_SITE_URL", SITE_URL).strip()
LLM_APP_NAME = os.environ.get("LLM_APP_NAME", "Travel Zynelion").strip()

REQUIRED_HEADERS = [
    "title", "slug", "publish_time", "category", "tags", "excerpt", "description",
    "image_query", "image", "prompt", "language", "min_words", "content", "status",
    "published_at", "output_file", "post_url",
]

STATUS_READY = {"ready", "draft", "pending", ""}
STATUS_PUBLISHED = "published"


def log(message: str) -> None:
    print(message, flush=True)


def now_local() -> datetime:
    return datetime.now(ZoneInfo(TIMEZONE))


def normalize_header(header: str) -> str:
    key = str(header or "").strip().lower()
    key = re.sub(r"\s+", "_", key)
    aliases = {
        "keyword": "title",
        "image_url": "image",
        "posted_at": "publish_time",
        "labels": "tags",
        "url_post": "post_url",
        "search_console_status": "search_console_status",
        "search_console_submitted_at": "search_console_submitted_at",
        "date": "publish_time",
        "published": "published_at",
        "url": "post_url",
        "link": "post_url",
        "links": "post_url",
        "linkssss": "post_url",
    }
    return aliases.get(key, key)


def slugify(text: str) -> str:
    text = str(text or "").strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"[\s_-]+", "-", text)
    return text.strip("-") or "post"


def parse_publish_time(value):
    if value is None:
        return None
    value = str(value).strip()
    if not value:
        return None

    # Google Sheets can return serial dates when formatted as raw values.
    if re.fullmatch(r"\d+(\.\d+)?", value):
        serial = float(value)
        base = datetime(1899, 12, 30, tzinfo=ZoneInfo(TIMEZONE))
        return base + timedelta(days=serial)

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=ZoneInfo(TIMEZONE))
        except ValueError:
            continue

    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=ZoneInfo(TIMEZONE))
        return dt.astimezone(ZoneInfo(TIMEZONE))
    except Exception as exc:
        raise ValueError(f"Invalid publish_time/date: {value!r}") from exc


def format_sheet_dt(dt: datetime) -> str:
    return dt.astimezone(ZoneInfo(TIMEZONE)).strftime("%Y-%m-%d %H:%M:%S")


def split_csv(value: str):
    if not value:
        return []
    return [item.strip() for item in str(value).split(",") if item.strip()]


def quote_yaml(value: str) -> str:
    value = str(value or "").replace("\\", "\\\\").replace('"', '\\"')
    return f'"{value}"'


def count_words(markdown: str) -> int:
    text = re.sub(r"```.*?```", " ", markdown or "", flags=re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[#>*_`\[\]()]", " ", text)
    return len(re.findall(r"[A-Za-zÀ-ỹ0-9]+", text))


def clean_generated_article(content: str) -> str:
    content = (content or "").strip()
    content = re.sub(r"^```(?:markdown)?\s*", "", content, flags=re.I)
    content = re.sub(r"\s*```$", "", content)
    unwanted_openings = [
        r"^Tentu[,!\s]*",
        r"^Berikut(?: adalah)?[^\n:]*:\s*",
        r"^Ini adalah[^\n:]*:\s*",
        r"^Artikel berikut[^\n:]*:\s*",
        r"^Baik[,!\s]*",
    ]
    for pattern in unwanted_openings:
        content = re.sub(pattern, "", content, flags=re.I)
    content = re.sub(r"\n{3,}", "\n\n", content)
    return content.strip()


def dedupe_keep_order(items):
    seen = set()
    out = []
    for item in items:
        key = str(item or "").lower().strip()
        if key and key not in seen:
            seen.add(key)
            out.append(str(item).strip())
    return out


def build_search_keywords(row: dict):
    image_query = str(row.get("image_query") or "").strip()
    if image_query:
        return image_query
    chunks = [row.get("title"), row.get("category")]
    chunks.extend(split_csv(row.get("tags", "")))
    excerpt = str(row.get("excerpt") or "").strip()
    if excerpt:
        chunks.extend(re.findall(r"[A-Za-zÀ-ỹ0-9]{4,}", excerpt)[:8])
    keywords = dedupe_keep_order([c for c in chunks if c])
    return " ".join(keywords[:5]) if keywords else "travel landscape"


def fetch_pexels_image(query: str, slug: str):
    if not PEXELS_API_KEY:
        log("PEXELS_API_KEY not set; image fetch skipped.")
        return "", ""

    try:
        resp = requests.get(
            "https://api.pexels.com/v1/search",
            headers={"Authorization": PEXELS_API_KEY},
            params={"query": query, "per_page": 1, "orientation": "landscape"},
            timeout=30,
        )
        if resp.status_code != 200:
            log(f"Pexels HTTP {resp.status_code}; image fetch skipped.")
            return "", ""
        photos = resp.json().get("photos") or []
        if not photos:
            log(f"No Pexels image found for: {query}")
            return "", ""
        photo = photos[0]
        image_url = (
            photo.get("src", {}).get("large2x")
            or photo.get("src", {}).get("large")
            or photo.get("src", {}).get("original")
        )
        if not image_url:
            return "", ""
        IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        local_path = IMAGES_DIR / f"{slug}.jpg"
        if not local_path.exists():
            img = requests.get(image_url, timeout=60)
            img.raise_for_status()
            local_path.write_bytes(img.content)
            log(f"Downloaded image: {local_path}")
        return f"/assets/images/auto/{local_path.name}", photo.get("url") or ""
    except Exception as exc:
        log(f"Pexels fetch failed: {exc}")
        return "", ""


def get_openai_client():
    if not LLM_API_KEY:
        raise RuntimeError("LLM_API_KEY is not set and this row has no content")
    headers = {}
    if "openrouter.ai" in LLM_BASE_URL:
        headers["HTTP-Referer"] = LLM_SITE_URL or SITE_URL
        headers["X-Title"] = LLM_APP_NAME or "Travel Zynelion"
    return OpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL, default_headers=headers or None)


def generate_article_once(row: dict, min_words: int, previous_issue: str = "") -> str:
    title = str(row.get("title") or "").strip()
    prompt = str(row.get("prompt") or "").strip() or title
    category = str(row.get("category") or DEFAULT_CATEGORY).strip()
    tags = split_csv(row.get("tags", ""))
    excerpt = str(row.get("excerpt") or "").strip()
    description = str(row.get("description") or "").strip()
    language = str(row.get("language") or DEFAULT_LANGUAGE).strip().lower()
    lang_name = "Bahasa Indonesia" if language in {"id", "indonesian", "bahasa indonesia"} else language

    repair_note = f"\nCatatan revisi: {previous_issue}\n" if previous_issue else ""
    system_prompt = f"""
You are a skilled human travel blogger and editor.
Write in {lang_name}. Output only the final article in markdown.
The article must be practical, readable, specific, and relevant to the title.
Do not mention AI, prompts, drafts, or that you are generating content.
Avoid generic filler, fake exact facts, fake prices, and template-like conclusions.
""".strip()
    user_prompt = f"""
Tulis artikel blog travel dalam {lang_name}.

Data artikel:
- Judul: {title}
- Kategori: {category}
- Tags: {", ".join(tags) if tags else "-"}
- Excerpt: {excerpt or "-"}
- Deskripsi SEO: {description or "-"}
- Brief/topik utama: {prompt}

Target wajib:
- Minimal {min_words} kata.
- Mulai langsung dengan paragraf pembuka, bukan kalimat meta.
- Gunakan subjudul markdown yang natural.
- Berikan tips konkret dan relevan.
- Jangan membuat klaim harga, jadwal, aturan, atau data spesifik jika tidak ada di brief.
- Jangan keluar dari topik judul.
{repair_note}
Keluaran hanya artikel markdown.
""".strip()
    client = get_openai_client()
    response = client.chat.completions.create(
        model=LLM_MODEL,
        temperature=0.86,
        top_p=0.9,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    return clean_generated_article(response.choices[0].message.content or "")


def generate_article(row: dict) -> str:
    title = str(row.get("title") or "").strip()
    if not title and not str(row.get("prompt") or "").strip():
        raise ValueError("missing title/prompt")
    try:
        min_words = max(MIN_ACCEPTABLE_WORDS, int(str(row.get("min_words") or DEFAULT_MIN_WORDS).strip()))
    except ValueError:
        min_words = DEFAULT_MIN_WORDS

    best = ""
    best_count = 0
    issue = ""
    for attempt in range(1, LLM_RETRIES + 1):
        log(f"Generating article attempt {attempt}/{LLM_RETRIES}: {title}")
        content = generate_article_once(row, min_words, issue)
        wc = count_words(content)
        log(f"Generated word count: {wc}")
        if wc > best_count:
            best, best_count = content, wc
        if wc >= min_words:
            return content
        issue = f"Versi sebelumnya terlalu pendek ({wc} kata). Tulis ulang lebih lengkap, minimal {min_words} kata."
        time.sleep(1)

    if best_count >= MIN_ACCEPTABLE_WORDS:
        log(f"Using best acceptable article: {best_count} words (< requested {min_words}, >= floor {MIN_ACCEPTABLE_WORDS}).")
        return best
    raise RuntimeError(f"Generated article too thin after retries: {best_count} words")


def build_front_matter(title, publish_dt, layout, category, tags, excerpt, description, image):
    lines = [
        "---",
        f"layout: {layout or DEFAULT_LAYOUT}",
        f"title: {quote_yaml(title)}",
        f"date: {publish_dt.strftime('%Y-%m-%d %H:%M:%S %z')}",
    ]
    if category:
        lines.append("categories: [" + ", ".join([slugify(category)]) + "]")
    if tags:
        clean_tags = [slugify(t) for t in tags if slugify(t)]
        if clean_tags:
            lines.append("tags: [" + ", ".join(clean_tags) + "]")
    if excerpt:
        lines.append(f"excerpt: {quote_yaml(excerpt)}")
    if description:
        lines.append(f"description: {quote_yaml(description)}")
    if image:
        lines.append(f"image: {quote_yaml(image)}")
    lines.append("---")
    return "\n".join(lines)


def make_post_url(filename: str) -> str:
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})-(.+)\.md$", filename)
    if not m:
        return f"{SITE_URL}/{filename}"
    y, mo, d, slug = m.groups()
    return f"{SITE_URL}/{y}/{mo}/{d}/{slug}.html"


def load_anchor_and_urls(spreadsheet):
    for name in ("External_Links", "Anchor", "Anchors", "Config"):
        try:
            ws = spreadsheet.worksheet(name)
            values = ws.get_all_values()
            if not values:
                continue
            anchors = []
            urls = []
            headers = [normalize_header(h) for h in values[0]] if values else []
            for row in values[1:]:
                first = str(row[0]).strip() if len(row) > 0 else ""
                second = str(row[1]).strip() if len(row) > 1 else ""
                if first and ("anchor" in headers[:1] or name != "Config"):
                    anchors.extend([a.strip() for a in first.split(",") if a.strip()])
                if second.startswith("http"):
                    urls.append(second)
            anchors = dedupe_keep_order(anchors)
            urls = dedupe_keep_order(urls)
            if anchors and urls:
                log(f"Loaded {len(anchors)} anchors and {len(urls)} URLs from {name}.")
                return anchors, urls
        except Exception:
            continue
    return [], []


def inject_anchor_links(content: str, anchors: list, urls: list, max_links: int = 3):
    if not content or not anchors or not urls:
        return content
    updated = content
    linked_count = 0
    for anchor in random.sample(anchors, k=min(len(anchors), max_links * 3)):
        if linked_count >= max_links:
            break
        if f"](" in updated and re.search(rf"\[[^\]]*{re.escape(anchor)}[^\]]*\]\(", updated, re.I):
            continue
        chosen_url = random.choice(urls)
        pattern = re.compile(rf"(?<![\]\[])(?i:\b{re.escape(anchor)}\b)")
        updated, n = pattern.subn(lambda m: f"[{m.group(0)}]({chosen_url})", updated, count=1)
        linked_count += 1 if n else 0
    log(f"Injected {linked_count} anchor link(s).")
    return updated


def load_footer(spreadsheet):
    try:
        ws = spreadsheet.worksheet("Footer")
        values = ws.get_all_values()
    except Exception:
        return [], ""
    footer_texts = []
    footer_url = ""
    for row in values[1:]:
        text = str(row[0]).strip() if len(row) > 0 else ""
        url = str(row[1]).strip() if len(row) > 1 else ""
        if text:
            footer_texts.append(text)
        if url.startswith("http") and not footer_url:
            footer_url = url
    return footer_texts, footer_url


def build_footer_block(footer_texts: list, footer_url: str):
    if not footer_texts or not footer_url:
        return ""
    text = random.choice(footer_texts).strip()
    words = text.split()
    if len(words) < 4:
        return f"\n\n---\n\n[{text}]({footer_url})\n"
    start = random.randint(0, max(0, len(words) - 3))
    length = random.randint(2, min(4, len(words) - start))
    anchor_text = " ".join(words[start:start + length])
    linked_text = text.replace(anchor_text, f"[{anchor_text}]({footer_url})", 1)
    return f"\n\n---\n\n{linked_text}\n"


def make_post_content(row: dict, publish_dt: datetime, anchors=None, urls=None, footer_texts=None, footer_url=None):
    title = str(row.get("title") or "").strip()
    if not title:
        raise ValueError("missing title")
    raw_slug = str(row.get("slug") or "").strip()
    slug = slugify(raw_slug or title)
    layout = str(row.get("layout") or DEFAULT_LAYOUT).strip() or DEFAULT_LAYOUT
    category = str(row.get("category") or DEFAULT_CATEGORY).strip()
    tags = split_csv(row.get("tags", ""))
    excerpt = str(row.get("excerpt") or "").strip()
    description = str(row.get("description") or "").strip()
    image = str(row.get("image") or "").strip()
    content = str(row.get("content") or "").strip()

    if not content:
        content = generate_article(row)
    if count_words(content) < MIN_ACCEPTABLE_WORDS:
        raise ValueError(f"content too thin: {count_words(content)} words")

    if anchors and urls:
        content = inject_anchor_links(content, anchors, urls, max_links=3)

    image_credit = ""
    if not image:
        image, image_credit = fetch_pexels_image(build_search_keywords(row), slug)

    filename = f"{publish_dt.strftime('%Y-%m-%d')}-{slug}.md"
    front_matter = build_front_matter(title, publish_dt, layout, category, tags, excerpt, description, image)
    image_block = ""
    if image:
        image_path_for_html = image.lstrip("/")
        image_block = f'<img src="{{{{ "{image_path_for_html}" | relative_url }}}}" alt="{title}" loading="lazy" decoding="async">\n\n'
    credit_block = f'\n\n<p class="image-credit"><em>Image source: Pexels ({image_credit})</em></p>\n' if image_credit else ""
    footer_block = build_footer_block(footer_texts or [], footer_url or "")
    return filename, f"{front_matter}\n\n{image_block}{content}{credit_block}{footer_block}\n"


def get_google_client():
    raw_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw_json:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON secret")
    info = json.loads(raw_json)
    credentials = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(credentials)


def get_spreadsheet():
    spreadsheet_id = os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    if not spreadsheet_id:
        raise RuntimeError("Missing GOOGLE_SHEETS_SPREADSHEET_ID variable")
    return get_google_client().open_by_key(spreadsheet_id)


def ensure_headers(worksheet):
    current = worksheet.row_values(1)
    normalized = [normalize_header(h) for h in current]
    if not any(normalized):
        worksheet.update("A1:Q1", [REQUIRED_HEADERS])
        return REQUIRED_HEADERS
    changed = False
    for header in REQUIRED_HEADERS:
        if header not in normalized:
            normalized.append(header)
            changed = True
    if changed or normalized != current[:len(normalized)]:
        worksheet.update(f"A1:{chr(64 + len(normalized))}1", [normalized])
    return normalized


def get_rows(worksheet, headers):
    values = worksheet.get_all_values()
    rows = []
    for values_row in values[1:]:
        padded = values_row + [""] * (len(headers) - len(values_row))
        rows.append({headers[i]: padded[i] if i < len(padded) else "" for i in range(len(headers))})
    return rows


def update_sheet_row(worksheet, headers, row_index, data: dict):
    updates = []
    for key, value in data.items():
        key = normalize_header(key)
        if key in headers:
            col = headers.index(key) + 1
            updates.append({"range": gspread.utils.rowcol_to_a1(row_index, col), "values": [[value]]})
    if updates:
        worksheet.batch_update(updates, value_input_option="USER_ENTERED")


def latest_publish_time_from_sheet(rows):
    latest = None
    for row in rows:
        try:
            dt = parse_publish_time(row.get("publish_time") or row.get("date"))
        except Exception:
            continue
        if dt and (latest is None or dt > latest):
            latest = dt
    return latest


def next_auto_publish_time(rows, local_now):
    latest = latest_publish_time_from_sheet(rows)
    if latest is None:
        return local_now
    candidate = latest + timedelta(minutes=PUBLISH_INTERVAL_MINUTES)
    if candidate < local_now:
        return local_now
    return candidate


def main():
    POSTS_DIR.mkdir(exist_ok=True)
    spreadsheet = get_spreadsheet()
    worksheet_name = os.environ.get("GOOGLE_SHEETS_WORKSHEET", "Sheet1").strip() or "Sheet1"
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
    except Exception:
        worksheet = spreadsheet.worksheet("Posts")
    headers = ensure_headers(worksheet)
    rows = get_rows(worksheet, headers)
    anchors, urls = load_anchor_and_urls(spreadsheet)
    footer_texts, footer_url = load_footer(spreadsheet)
    local_now = now_local()
    log(f"Site URL: {SITE_URL}")
    log(f"Current local time: {format_sheet_dt(local_now)} {TIMEZONE}")

    published_count = 0
    skipped_count = 0
    auto_time = next_auto_publish_time(rows, local_now)

    for row_index, row in enumerate(rows, start=2):
        if published_count >= MAX_POSTS_PER_RUN:
            break
        title = str(row.get("title") or "").strip()
        if not title:
            skipped_count += 1
            continue
        status = str(row.get("status") or "").strip().lower()
        if status == STATUS_PUBLISHED:
            skipped_count += 1
            continue
        if status not in STATUS_READY:
            log(f"Row {row_index}: skipped, status={status!r}")
            skipped_count += 1
            continue
        if not str(row.get("content") or "").strip() and not str(row.get("prompt") or "").strip():
            # Title-only rows are valid: use title as prompt.
            row["prompt"] = title

        publish_raw = str(row.get("publish_time") or row.get("date") or "").strip()
        if publish_raw:
            try:
                publish_dt = parse_publish_time(publish_raw)
            except ValueError as exc:
                log(f"Row {row_index}: skipped, {exc}")
                skipped_count += 1
                continue
        else:
            publish_dt = auto_time
            auto_time = auto_time + timedelta(minutes=PUBLISH_INTERVAL_MINUTES)
            update_sheet_row(worksheet, headers, row_index, {"publish_time": format_sheet_dt(publish_dt)})

        if publish_dt > local_now:
            log(f"Row {row_index}: waiting until {format_sheet_dt(publish_dt)}")
            skipped_count += 1
            continue

        try:
            filename, post_body = make_post_content(row, publish_dt, anchors, urls, footer_texts, footer_url)
            filepath = POSTS_DIR / filename
            if filepath.exists():
                log(f"Row {row_index}: file already exists -> {filename}; marking published.")
            else:
                filepath.write_text(post_body, encoding="utf-8")
                log(f"Row {row_index}: created -> {filename}")
            update_sheet_row(
                worksheet,
                headers,
                row_index,
                {
                    "slug": slugify(row.get("slug") or title),
                    "status": STATUS_PUBLISHED,
                    "published_at": format_sheet_dt(local_now),
                    "output_file": filename,
                    "post_url": make_post_url(filename),
                },
            )
            published_count += 1
        except Exception as exc:
            log(f"Row {row_index}: error -> {exc}")
            update_sheet_row(worksheet, headers, row_index, {"status": f"error: {str(exc)[:80]}"})
            skipped_count += 1
            continue

    log(f"Done. Published: {published_count}, skipped/waiting: {skipped_count}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("FATAL ERROR:", flush=True)
        traceback.print_exc()
        sys.exit(1)
