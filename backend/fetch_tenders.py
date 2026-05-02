import requests
import psycopg
import json
import math
import time
import random
from datetime import datetime, timedelta, timezone
import os

from normalizer import normalize_release


API_URL = "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages"


# -----------------------------
# DB
# -----------------------------

def get_connection():
    return psycopg.connect(os.environ["DATABASE_URL"])


# -----------------------------
# GET LAST RUN
# -----------------------------

def get_last_run():
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT last_run_utc FROM ingest_state WHERE id = 1;")
        result = cur.fetchone()
    conn.close()
    return result[0] if result else None


# -----------------------------
# UPDATE LAST RUN
# -----------------------------

def update_last_run(run_started_at):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ingest_state (id, last_run_utc)
            VALUES (1, %s)
            ON CONFLICT (id) DO UPDATE SET last_run_utc = EXCLUDED.last_run_utc;
        """, (run_started_at,))
    conn.commit()
    conn.close()


# -----------------------------
# JSON SANITIZER
# -----------------------------

def clean_json_values(obj):
    if isinstance(obj, dict):
        return {k: clean_json_values(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_json_values(v) for v in obj]
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    return obj


# -----------------------------
# SAFE REQUEST
# -----------------------------

def safe_get_json(url, params=None, timeout=60, max_retries=10):
    attempt = 0

    while True:
        try:
            response = requests.get(url, params=params, timeout=timeout)

            if response.status_code == 429:
                if attempt >= max_retries:
                    response.raise_for_status()

                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait_time = max(float(retry_after), 10.0)
                    except ValueError:
                        wait_time = 15.0
                else:
                    wait_time = min(180, (2 ** attempt) * 5 + random.uniform(2, 6))

                print(f"429 rate limit. Sleeping {wait_time:.1f}s...")
                time.sleep(wait_time)
                attempt += 1
                continue

            response.raise_for_status()

            try:
                return response.json()
            except requests.exceptions.JSONDecodeError:
                if attempt >= max_retries:
                    raise

                wait_time = min(180, (2 ** attempt) * 5 + random.uniform(2, 6))
                content_type = response.headers.get("Content-Type", "")
                preview = (response.text or "")[:400].replace("\n", " ")
                print(
                    f"JSON decode failed. Retrying... attempt {attempt + 1}/{max_retries}; "
                    f"wait {wait_time:.1f}s; status={response.status_code}; "
                    f"content_type={content_type}; preview={preview!r}"
                )
                time.sleep(wait_time)
                attempt += 1
                continue

        except requests.exceptions.RequestException as e:
            if attempt >= max_retries:
                raise

            wait_time = min(180, (2 ** attempt) * 5 + random.uniform(2, 6))
            print(f"Request error: {e}. Retry in {wait_time:.1f}s...")
            time.sleep(wait_time)
            attempt += 1


def format_api_datetime(value):
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


# -----------------------------
# FETCH FROM API
# -----------------------------

def build_daily_windows(start, end):
    windows = []
    current = start

    while current < end:
        window_end = min(current + timedelta(days=1), end)
        windows.append((current, window_end))
        current = window_end

    return windows


def fetch_releases_for_window(window_start, window_end, window_number, window_attempt):
    print(f"Window {window_number} attempt {window_attempt}/2")

    releases_for_window = []
    url = API_URL
    params = {
        "limit": 50,
        "updatedFrom": format_api_datetime(window_start),
        "updatedTo": format_api_datetime(window_end),
    }
    page = 1

    while True:
        print(f"Window {window_number} page {page}...")
        data = safe_get_json(url, params=params)

        releases = data.get("releases", [])
        releases_for_window.extend(releases)
        print(f"Window {window_number} page {page}: fetched {len(releases)} releases")

        next_link = data.get("links", {}).get("next")
        if not next_link:
            break

        url = next_link
        params = None
        page += 1
        time.sleep(random.uniform(2, 4))

    print(f"Window {window_number}: fetched total {len(releases_for_window)} releases")
    return releases_for_window


# -----------------------------
# INSERT NOTICE
# -----------------------------

def insert_notice(cur, notice):
    cleaned_raw_json = clean_json_values(notice["raw_json"])

    cur.execute("""
        INSERT INTO notices (
            id,
            ocid,
            notice_type,
            stage,

            title,
            description,
            buyer_name,

            published_at,
            deadline,

            value_amount,
            value_currency,

            cpv_code,
            cpv_description,
            cpv_codes,

            industry_source_main,
            industry_source_additional,
            industry_bucket,

            region,
            location,

            procurement_method,
            procurement_method_details,
            is_framework,
            submission_url,

            is_suitable_for_sme,
            is_suitable_for_vco,

            economic_criteria,
            economic_minimum,
            technical_criteria,
            technical_minimum,

            raw_json
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s
        )
        ON CONFLICT (id) DO NOTHING;
    """, (
        notice["id"],
        notice["ocid"],
        notice["notice_type"],
        notice["stage"],

        notice["title"],
        notice["description"],
        notice["buyer_name"],

        notice["published_at"],
        notice["deadline"],

        notice["value_amount"],
        notice["value_currency"],

        notice["cpv_code"],
        notice["cpv_description"],
        notice["cpv_codes"],

        notice["industry_source_main"],
        notice["industry_source_additional"],
        notice["industry_bucket"],

        notice["region"],
        notice["location"],

        notice["procurement_method"],
        notice["procurement_method_details"],
        notice["is_framework"],
        notice["submission_url"],

        notice["is_suitable_for_sme"],
        notice["is_suitable_for_vco"],

        notice["economic_criteria"],
        notice["economic_minimum"],
        notice["technical_criteria"],
        notice["technical_minimum"],

        json.dumps(cleaned_raw_json, allow_nan=False)
    ))


# -----------------------------
# UPSERT PROCUREMENT
# -----------------------------

def upsert_procurement(cur, p):
    cur.execute("""
        INSERT INTO procurements (
            ocid,
            title,
            buyer_name,
            latest_notice_id,
            latest_notice_type,
            latest_stage,
            published_at,
            deadline,
            value_amount,
            value_currency,
            cpv_code,
            cpv_description,
            cpv_codes,
            industry_source_main,
            industry_source_additional,
            industry_bucket,
            region,
            is_live,
            updated_at,
            description,
            submission_url
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (ocid) DO UPDATE SET
            title = EXCLUDED.title,
            buyer_name = EXCLUDED.buyer_name,
            latest_notice_id = EXCLUDED.latest_notice_id,
            latest_notice_type = EXCLUDED.latest_notice_type,
            latest_stage = EXCLUDED.latest_stage,
            published_at = EXCLUDED.published_at,
            deadline = EXCLUDED.deadline,
            value_amount = EXCLUDED.value_amount,
            value_currency = EXCLUDED.value_currency,
            cpv_code = EXCLUDED.cpv_code,
            cpv_description = EXCLUDED.cpv_description,
            cpv_codes = EXCLUDED.cpv_codes,
            industry_source_main = EXCLUDED.industry_source_main,
            industry_source_additional = EXCLUDED.industry_source_additional,
            industry_bucket = EXCLUDED.industry_bucket,
            region = EXCLUDED.region,
            is_live = EXCLUDED.is_live,
            updated_at = EXCLUDED.updated_at,
            description = EXCLUDED.description,
            submission_url = EXCLUDED.submission_url;
    """, (
        p["ocid"],
        p["title"],
        p["buyer_name"],
        p["latest_notice_id"],
        p["latest_notice_type"],
        p["latest_stage"],
        p["published_at"],
        p["deadline"],
        p["value_amount"],
        p["value_currency"],
        p["cpv_code"],
        p["cpv_description"],
        p["cpv_codes"],
        p["industry_source_main"],
        p["industry_source_additional"],
        p["industry_bucket"],
        p["region"],
        p["is_live"],
        p["updated_at"],
        p["description"],
        p["submission_url"]
    ))


# -----------------------------
# PROCESS WINDOW
# -----------------------------

def process_releases_for_window(releases, window_number):
    conn = get_connection()
    cur = conn.cursor()

    processed = 0
    failed = 0

    try:
        for r in releases:
            try:
                data = normalize_release(r)

                notice = data["notice"]
                procurement = data["procurement"]

                insert_notice(cur, notice)
                upsert_procurement(cur, procurement)

                conn.commit()
                processed += 1

            except Exception as e:
                failed += 1
                conn.rollback()
                cur = conn.cursor()
                release_id = r.get("id") or r.get("ocid") or "unknown"
                print(f"Window {window_number}: error processing release {release_id}: {e}")
    finally:
        conn.close()

    print(
        f"Window {window_number}: processed {processed} releases; "
        f"failed release records {failed}"
    )
    return processed, failed


# -----------------------------
# MAIN INGEST
# -----------------------------

def run_ingestion():
    run_started_at = datetime.now(timezone.utc)
    last_run = get_last_run()

    if last_run:
        start = last_run - timedelta(days=1)
    else:
        start = run_started_at - timedelta(days=7)

    end = run_started_at

    print(f"Checkpoint last_run_utc: {last_run}")
    print(f"Overall ingestion period: {format_api_datetime(start)} -> {format_api_datetime(end)}")

    windows = build_daily_windows(start, end)
    failed_windows = []
    successful_windows = 0
    total_processed = 0
    total_failed_records = 0

    for window_number, (window_start, window_end) in enumerate(windows, start=1):
        print(f"=== WINDOW {window_number}: {format_api_datetime(window_start)} -> {format_api_datetime(window_end)} ===")

        window_error = None
        window_succeeded = False

        for window_attempt in range(1, 3):
            try:
                releases = fetch_releases_for_window(
                    window_start,
                    window_end,
                    window_number,
                    window_attempt,
                )
                processed, failed = process_releases_for_window(releases, window_number)
                total_processed += processed
                total_failed_records += failed
                successful_windows += 1
                window_succeeded = True
                print(
                    f"Window {window_number}: success; fetched {len(releases)} releases; "
                    f"processed {processed}; failed release records {failed}"
                )
                break
            except Exception as e:
                window_error = e
                print(f"Window {window_number} attempt {window_attempt}/2 failed: {e}")
                if window_attempt < 2:
                    wait_time = random.uniform(30, 60)
                    print(f"Retrying window {window_number} from page 1 in {wait_time:.1f}s...")
                    time.sleep(wait_time)

        if not window_succeeded:
            failed_windows.append({
                "window_number": window_number,
                "window_start": format_api_datetime(window_start),
                "window_end": format_api_datetime(window_end),
                "error": str(window_error),
            })

    update_last_run(run_started_at)

    print(f"Total windows attempted: {len(windows)}")
    print(f"Successful windows: {successful_windows}")
    print(f"Failed windows: {len(failed_windows)}")
    print(f"Total processed records: {total_processed}")
    print(f"Total failed release records: {total_failed_records}")

    if failed_windows:
        print("WARNING: Some ingestion windows failed. Manual backfill required.")
        for failed_window in failed_windows:
            print(
                f"Failed window {failed_window['window_number']}: "
                f"{failed_window['window_start']} -> {failed_window['window_end']} | "
                f"error={failed_window['error']}"
            )
        print(f"Checkpoint updated to {format_api_datetime(run_started_at)}")
    else:
        print(f"Checkpoint updated to {format_api_datetime(run_started_at)}")


# -----------------------------
# RUN
# -----------------------------

if __name__ == "__main__":
    run_ingestion()
