import os
import re
import json
import random
import traceback
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
import requests
from google.oauth2.service_account import Credentials
from openai import OpenAI

TIMEZONE = os.environ.get("TZ", "Asia/Phnom_Penh").strip() or "Asia/Phnom_Penh"
DEFAULT_LAYOUT = "post"
DEFAULT_CATEGORY = "blog"
DEFAULT_LANGUAGE = "id"
DEFAULT_MIN_WORDS = 1200

POSTS_DIR = Path(os.environ.get("POSTS_DIR", "_posts"))
IMAGES_DIR = Path(os.environ.get("IMAGES_DIR", "assets/images/auto"))

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]

PEXELS_API_KEY = os.environ.get("PEXELS_API_KEY", "").strip()
LLM_API_KEY = os.environ.get("LLM_API_KEY", "").strip()
DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", DEFAULT_BASE_URL).strip() or DEFAULT_BASE_URL
DEFAULT_MODEL = "openrouter/auto"
LLM_MODEL = os.environ.get("LLM_MODEL", DEFAULT_MODEL).strip() or DEFAULT_MODEL
LLM_SITE_URL = os.environ.get("LLM_SITE_URL", "https://example.com").strip()
LLM_APP_NAME = os.environ.get("LLM_APP_NAME", "Auto Blog Publisher").strip()


REQUIRED_ENV_VARS = [
    "GOOGLE_SHEETS_SPREADSHEET_ID",
    "GOOGLE_SHEETS_WORKSHEET",
    "GOOGLE_SERVICE_ACCOUNT_JSON",
]


def now_local() -> datetime:
    return datetime.now(ZoneInfo(TIMEZONE))


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def validate_required_env() -> None:
    for name in REQUIRED_ENV_VARS:
        require_env(name)


def slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    text = re.sub(r"[\s_-]+", "-", text)
    return text.strip("-")


def parse_publish_time(value: str):
    raw = str(value or "").strip()
    if not raw:
        return None

    normalized = raw.replace("T", " ")
    normalized = re.sub(r"\s+", " ", normalized).strip()

    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
    ]

    for fmt in fmts:
        try:
            dt = datetime.strptime(normalized, fmt)
            return dt.replace(tzinfo=ZoneInfo(TIMEZONE))
        except ValueError:
            pass

    try:
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=ZoneInfo(TIMEZONE))
        return dt.astimezone(ZoneInfo(TIMEZONE))
    except ValueError:
        pass

    raise ValueError(
        f"Invalid publish_time format: '{raw}'. "
        "Use YYYY-MM-DD HH:MM:SS, YYYY-MM-DD HH:MM, YYYY-MM-DD, or ISO format."
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


def clean_generated_article(content: str) -> str:
    if not content:
        return content

    unwanted_openings = [
        r"^Tentu[,!\s]*",
        r"^Berikut( adalah)? .*?:\s*",
        r"^Ini adalah .*?:\s*",
        r"^Artikel berikut .*?:\s*",
        r"^Baik[,!\s]*",
    ]

    for pattern in unwanted_openings:
        content = re.sub(pattern, "", content, flags=re.IGNORECASE)

    content = re.sub(r"\n{3,}", "\n\n", content)

    replacements = {
        "Selain itu,": "Di sisi lain,",
        "Tidak hanya itu,": "Yang sering terlupa,",
        "Pada dasarnya,": "",
        "Perlu diketahui bahwa": "",
        "Dengan demikian,": "Jadi,",
        "Sebagai penutup,": "",
        "Sebagai kesimpulan,": "",
    }

    for old, new in replacements.items():
        content = content.replace(old, new)

    return content.strip()


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
        chunks.extend(words[:8])

    cleaned = []
    for c in chunks:
        c = re.sub(r"\s+", " ", str(c)).strip()
        if c:
            cleaned.append(c)

    keywords = dedupe_keep_order(cleaned)
    primary = ", ".join(keywords[:8]) if keywords else "travel landscape"
    search_query = " ".join(keywords[:5]) if keywords else "travel landscape"
    return search_query, primary


def fetch_pexels_image(query: str, slug: str):
    if not PEXELS_API_KEY:
        print("PEXELS_API_KEY not set; skipping image fetch.")
        return None, None

    endpoint = "https://api.pexels.com/v1/search"
    headers = {"Authorization": PEXELS_API_KEY}
    params = {"query": query, "per_page": 1, "orientation": "landscape"}

    try:
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

        image_path = f"/{IMAGES_DIR.as_posix()}/{local_filename}"
        credit = photo.get("photographer_url") or photo.get("url") or ""
        return image_path, credit

    except Exception as e:
        print(f"Pexels fetch failed: {e}")
        return None, None


def get_openai_client():
    if not LLM_API_KEY:
        raise RuntimeError("LLM_API_KEY is not set")

    extra_headers = {}
    if "openrouter.ai" in LLM_BASE_URL:
        if LLM_SITE_URL:
            extra_headers["HTTP-Referer"] = LLM_SITE_URL
        if LLM_APP_NAME:
            extra_headers["X-Title"] = LLM_APP_NAME

    return OpenAI(
        api_key=LLM_API_KEY,
        base_url=LLM_BASE_URL,
        default_headers=extra_headers or None,
    )


def generate_article(row: dict):
    title = str(row.get("title") or "").strip()
    prompt = str(row.get("prompt") or "").strip()
    category = str(row.get("category") or DEFAULT_CATEGORY).strip()
    tags = split_csv(row.get("tags", ""))
    excerpt = str(row.get("excerpt") or "").strip()
    description = str(row.get("description") or "").strip()
    language = str(row.get("language") or DEFAULT_LANGUAGE).strip().lower()
    min_words_raw = str(row.get("min_words") or DEFAULT_MIN_WORDS).strip()

    try:
        min_words = max(700, int(min_words_raw))
    except ValueError:
        min_words = DEFAULT_MIN_WORDS

    if not prompt and not title:
        raise ValueError("Missing title/prompt for article generation")

    lang_name = "Bahasa Indonesia" if language in {"id", "indonesian", "bahasa indonesia"} else language

    system_prompt = f"""
You are a skilled human travel blogger and editor.

Write in {lang_name} with a natural, believable, non-robotic style.
The article must feel like it was written by a real blogger with practical travel awareness.

Writing style rules:
- Sound natural, warm, and specific.
- Avoid generic filler, cliché travel phrases, and empty motivational lines.
- Avoid sounding like a template or AI-generated article.
- Do not write meta introductions like "Tentu", "Berikut artikelnya", "Artikel ini akan membahas", or similar.
- Start directly with a strong opening paragraph.
- Vary sentence length naturally.
- Use short-to-medium paragraphs.
- Use markdown headings naturally, but do not over-structure the article.
- Do not force numbered sections unless they fit naturally.
- Do not overuse transition phrases.
- Avoid repetitive wording.
- Avoid sounding too formal or textbook-like.
- Do not mention AI, prompts, drafts, or generated content.
- Do not invent exact prices, regulations, or factual claims unless implied by the prompt.
- Focus on practical usefulness and readability.
""".strip()

    user_prompt = f"""
Tulis artikel blog perjalanan dalam {lang_name}.

Data artikel:
- Judul: {title}
- Kategori: {category}
- Tags: {", ".join(tags) if tags else "-"}
- Excerpt: {excerpt or "-"}
- Deskripsi SEO: {description or "-"}
- Brief/topik utama: {prompt or title}

Target hasil:
- Panjang minimal sekitar {min_words} kata.
- Gaya tulisan harus terasa bersih, hidup, dan enak dibaca.
- Artikel harus terasa seperti blog travel asli, bukan konten generik.
- Isi harus relevan dengan judul.
- Jika topiknya berupa panduan, berikan tips yang konkret.
- Jika topiknya berupa itinerary atau destinasi, beri gambaran suasana dan hal yang benar-benar berguna untuk pembaca.
- Jika topiknya berupa packing list atau tips, buat penjelasan realistis dan tidak bertele-tele.

Keluaran:
- hanya artikel dalam format markdown
- tanpa penjelasan tambahan
- tanpa komentar pembuka
""".strip()

    client = get_openai_client()
    response = client.chat.completions.create(
        model=LLM_MODEL,
        temperature=0.95,
        top_p=0.9,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )

    content = response.choices[0].message.content or ""
    content = clean_generated_article(content.strip())

    if not content:
        raise RuntimeError("LLM returned empty content")

    return content


def build_front_matter(*, title: str, publish_dt: datetime, layout: str, category: str, tags: list, excerpt: str, description: str, image: str):
    lines = [
        "---",
        f"layout: {layout}",
        f'title: {quote_yaml(title)}',
        f"date: {publish_dt.strftime('%Y-%m-%d %H:%M:%S %z')}",
    ]

    if category:
        lines.append(f"categories: [{category}]")
    if tags:
        lines.append("tags: [" + ", ".join(tags) + "]")
    if excerpt:
        lines.append(f'excerpt: {quote_yaml(excerpt)}')
    if description:
        lines.append(f'description: {quote_yaml(description)}')
    if image:
        lines.append(f'image: {quote_yaml(image)}')

    lines.append("---")
    return "\n".join(lines)


def get_google_client():
    raw_json = require_env("GOOGLE_SERVICE_ACCOUNT_JSON")
    info = json.loads(raw_json)
    credentials = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(credentials)


def get_spreadsheet():
    spreadsheet_id = require_env("GOOGLE_SHEETS_SPREADSHEET_ID")
    client = get_google_client()
    return client.open_by_key(spreadsheet_id)


def get_worksheet_by_name(spreadsheet, worksheet_name: str):
    return spreadsheet.worksheet(worksheet_name)


def get_clean_rows_and_headers_from_worksheet(worksheet):
    raw_headers = worksheet.row_values(1)
    headers = [str(h).strip() for h in raw_headers]

    all_values = worksheet.get_all_values()
    data_rows = all_values[1:]

    rows = []
    for values in data_rows:
        padded = values + [""] * (len(headers) - len(values))
        row = {}
        for i, header in enumerate(headers):
            if header:
                row[header] = padded[i]
        rows.append(row)

    return headers, rows


def update_sheet_row(worksheet, headers, row_index, data: dict):
    normalized_headers = [str(h).strip() for h in headers]

    updates = []
    for key, value in data.items():
        key = str(key).strip()
        if key in normalized_headers:
            col_index = normalized_headers.index(key) + 1
            updates.append({
                "range": gspread.utils.rowcol_to_a1(row_index, col_index),
                "values": [[value]],
            })

    if updates:
        worksheet.batch_update(updates, value_input_option="USER_ENTERED")


def load_anchor_and_urls(spreadsheet):
    try:
        ws = get_worksheet_by_name(spreadsheet, "Anchor")
    except Exception:
        print("Anchor sheet not found.")
        return [], []

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return [], []

    anchor_cell = values[1][0] if len(values[1]) > 0 else ""
    anchors = [a.strip() for a in str(anchor_cell).split(",") if a.strip()]

    urls = []
    for row in values[1:]:
        if len(row) > 1 and str(row[1]).strip():
            urls.append(str(row[1]).strip())

    print(f"Loaded {len(anchors)} anchors and {len(urls)} URLs.")
    return anchors, urls


def inject_anchor_links(content: str, anchors: list, urls: list, max_links: int = 3):
    if not content or not anchors or not urls:
        return content

    updated = content
    linked_count = 0
    shuffled_anchors = anchors[:]
    random.shuffle(shuffled_anchors)

    for anchor in shuffled_anchors:
        if linked_count >= max_links:
            break

        chosen_url = random.choice(urls)
        pattern = re.compile(rf"(?i)\b({re.escape(anchor)})\b")

        def replacer(match):
            return f"[{match.group(1)}]({chosen_url})"

        updated, replacements = pattern.subn(replacer, updated, count=1)

        if replacements > 0:
            linked_count += 1

    print(f"Injected {linked_count} anchor link(s).")
    return updated


def load_footer(spreadsheet):
    try:
        ws = get_worksheet_by_name(spreadsheet, "Footer")
    except Exception:
        print("Footer sheet not found.")
        return [], ""

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return [], ""

    raw_text = values[1][0] if len(values[1]) > 0 else ""
    footer_url = values[1][1] if len(values[1]) > 1 else ""

    raw_text = str(raw_text).strip()
    footer_url = str(footer_url).strip()

    if not raw_text or not footer_url:
        return [], ""

    footer_texts = [t.strip() for t in raw_text.split(",") if t.strip()]
    print(f"Loaded {len(footer_texts)} footer text(s).")
    return footer_texts, footer_url


def build_footer_block(footer_texts: list, footer_url: str):
    if not footer_texts or not footer_url:
        return ""

    text = random.choice(footer_texts)
    words = text.split()

    if len(words) < 4:
        return f"\n\n---\n\n[{text}]({footer_url})\n"

    start = random.randint(1, max(1, len(words) - 3))
    length = random.randint(2, min(4, len(words) - start))
    anchor_words = words[start:start + length]
    anchor_text = " ".join(anchor_words)

    linked_text = text.replace(anchor_text, f"[{anchor_text}]({footer_url})", 1)
    return f"\n\n---\n\n{linked_text}\n"


def make_post_content(row: dict, publish_dt: datetime, anchors=None, urls=None, footer_texts=None, footer_url=None):
    title = (row.get("title") or "").strip()
    if not title:
        raise ValueError("Missing title")

    slug = (row.get("slug") or "").strip() or slugify(title)
    layout = (row.get("layout") or DEFAULT_LAYOUT).strip() or DEFAULT_LAYOUT
    category = (row.get("category") or DEFAULT_CATEGORY).strip() or DEFAULT_CATEGORY
    tags = split_csv(row.get("tags", ""))
    excerpt = (row.get("excerpt") or "").strip()
    description = (row.get("description") or "").strip()
    image = (row.get("image") or "").strip()
    content = (row.get("content") or "").strip()

    if not content:
        print(f"Generating article for: {title}")
        content = generate_article(row)

    if not content:
        raise ValueError("Missing content")

    if anchors and urls:
        content = inject_anchor_links(content, anchors, urls, max_links=3)

    image_credit = ""

    if not image:
        image_query, _ = build_search_keywords(row)
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

    image_block = ""
    if image:
        image_path_for_html = image.lstrip("/")
        image_block = (
            f'<img src="{{{{ "{image_path_for_html}" | relative_url }}}}" '
            f'alt="{title}" loading="lazy" decoding="async">\n\n'
        )

    credit_block = (
        f'\n\n<p style="font-size:0.95em; color:#666;"><em>Image source: Pexels ({image_credit})</em></p>\n'
        if image_credit else ""
    )

    footer_block = ""
    if footer_texts and footer_url:
        footer_block = build_footer_block(footer_texts, footer_url)

    full_content = f"{front_matter}\n\n{image_block}{content}{credit_block}{footer_block}\n"
    return filename, full_content


def main():
    validate_required_env()

    POSTS_DIR.mkdir(parents=True, exist_ok=True)

    spreadsheet = get_spreadsheet()
    worksheet_name = require_env("GOOGLE_SHEETS_WORKSHEET")
    worksheet = get_worksheet_by_name(spreadsheet, worksheet_name)
    headers, rows = get_clean_rows_and_headers_from_worksheet(worksheet)

    anchors, urls = load_anchor_and_urls(spreadsheet)
    footer_texts, footer_url = load_footer(spreadsheet)

    print(f"Worksheet: {worksheet_name}")
    print(f"Headers: {headers}")

    local_now = now_local()
    print(f"Current local time: {local_now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("Rule: publish every row where status=ready and publish_time <= now")

    published_count = 0
    skipped_count = 0

    for row_index, row in enumerate(rows, start=2):
        title_raw = str(row.get("title") or "").strip()
        status = str(row.get("status", "")).strip().lower()
        publish_time_raw = str(row.get("publish_time") or row.get("date") or "").strip()
        content_raw = str(row.get("content") or "").strip()
        prompt_raw = str(row.get("prompt") or "").strip()

        print(
            f"Row {row_index}: title={title_raw!r}, status={status!r}, publish_time={publish_time_raw!r}"
        )

        if status != "ready":
            print(f"Row {row_index}: skipped, status is '{status}' not ready")
            skipped_count += 1
            continue

        if not title_raw:
            print(f"Row {row_index}: skipped, missing title")
            skipped_count += 1
            continue

        if not content_raw and not prompt_raw:
            print(f"Row {row_index}: skipped, missing content and prompt")
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
            print(f"Row {row_index}: waiting until {publish_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            skipped_count += 1
            continue

        try:
            filename, post_body = make_post_content(
                row,
                publish_dt,
                anchors=anchors,
                urls=urls,
                footer_texts=footer_texts,
                footer_url=footer_url,
            )
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

        published_at = now_local().strftime("%Y-%m-%d %H:%M:%S")
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
    try:
        main()
    except Exception:
        print("FATAL ERROR:")
        traceback.print_exc()
        raise
