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
# UPDATE LAST RUN
# -----------------------------

def update_last_run():
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ingest_state (id, last_run_utc)
            VALUES (1, NOW())
            ON CONFLICT (id) DO UPDATE SET last_run_utc = NOW();
        """)
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
# TRUNCATE TABLES
# -----------------------------

def truncate_tables():
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE notices, procurements RESTART IDENTITY CASCADE;")
    conn.commit()
    conn.close()
    print("Tables truncated.")


# -----------------------------
# SAFE REQUEST
# -----------------------------

def safe_get(url, params=None, timeout=60, max_retries=10):
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
                    except:
                        wait_time = 15.0
                else:
                    wait_time = min(180, (2 ** attempt) * 5 + random.uniform(2, 6))

                print(f"429 rate limit. Sleeping {wait_time:.1f}s...")
                time.sleep(wait_time)
                attempt += 1
                continue

            response.raise_for_status()
            return response

        except requests.exceptions.RequestException as e:
            if attempt >= max_retries:
                raise e

            wait_time = min(180, (2 ** attempt) * 5 + random.uniform(2, 6))
            print(f"Error: {e}. Retry in {wait_time:.1f}s...")
            time.sleep(wait_time)
            attempt += 1


# -----------------------------
# FETCH WINDOW
# -----------------------------

def fetch_releases_for_window(start, end):
    from_str = start.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    to_str = end.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    print(f"\nWindow: {from_str} → {to_str}")

    all_releases = []
    url = API_URL
    params = {
        "limit": 50,
        "updatedFrom": from_str,
        "updatedTo": to_str
    }

    page = 1

    while True:
        print(f"Page {page}...")

        response = safe_get(url, params=params)
        data = response.json()

        releases = data.get("releases", [])
        if not releases:
            break

        all_releases.extend(releases)
        print(f"Fetched {len(releases)}")

        next_link = data.get("links", {}).get("next")
        if not next_link:
            break

        time.sleep(random.uniform(3, 6))

        url = next_link
        params = None
        page += 1

    print(f"Window total: {len(all_releases)}")
    return all_releases


# -----------------------------
# FETCH 15 DAYS
# -----------------------------

def fetch_all():
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=15)

    current = start
    all_releases = []
    window = 1

    while current < end:
        next_point = min(current + timedelta(days=1), end)

        print(f"\n=== WINDOW {window} ===")
        data = fetch_releases_for_window(current, next_point)
        all_releases.extend(data)

        sleep_time = random.uniform(8, 15)
        print(f"Sleeping {sleep_time:.1f}s...")
        time.sleep(sleep_time)

        current = next_point
        window += 1

    print(f"\nTOTAL fetched: {len(all_releases)}")
    return all_releases


# -----------------------------
# INSERT NOTICE
# -----------------------------

def insert_notice(cur, notice):
    cleaned_raw_json = clean_json_values(notice["raw_json"])

    cur.execute("""
        INSERT INTO notices (
            id, ocid, notice_type, stage,
            title, description, buyer_name,
            published_at, deadline,
            value_amount, value_currency,
            cpv_code, cpv_description, cpv_codes,
            industry_source_main, industry_source_additional, industry_bucket,
            region, location,
            procurement_method, procurement_method_details,
            is_framework, submission_url,
            is_suitable_for_sme, is_suitable_for_vco,
            economic_criteria, economic_minimum,
            technical_criteria, technical_minimum,
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
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s
        )
        ON CONFLICT (id) DO NOTHING;
    """, (
        notice["id"], notice["ocid"], notice["notice_type"], notice["stage"],
        notice["title"], notice["description"], notice["buyer_name"],
        notice["published_at"], notice["deadline"],
        notice["value_amount"], notice["value_currency"],
        notice["cpv_code"], notice["cpv_description"], notice["cpv_codes"],
        notice["industry_source_main"], notice["industry_source_additional"], notice["industry_bucket"],
        notice["region"], notice["location"],
        notice["procurement_method"], notice["procurement_method_details"],
        notice["is_framework"], notice["submission_url"],
        notice["is_suitable_for_sme"], notice["is_suitable_for_vco"],
        notice["economic_criteria"], notice["economic_minimum"],
        notice["technical_criteria"], notice["technical_minimum"],
        json.dumps(cleaned_raw_json, allow_nan=False)
    ))


# -----------------------------
# UPSERT PROCUREMENT
# -----------------------------

def upsert_procurement(cur, p):
    cur.execute("""
        INSERT INTO procurements (
            ocid, title, buyer_name,
            latest_notice_id, latest_notice_type, latest_stage,
            published_at, deadline,
            value_amount, value_currency,
            cpv_code, cpv_description, cpv_codes,
            industry_source_main, industry_source_additional, industry_bucket,
            region, is_live, updated_at, description, submission_url
        )
        VALUES (
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s
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
        p["ocid"], p["title"], p["buyer_name"],
        p["latest_notice_id"], p["latest_notice_type"], p["latest_stage"],
        p["published_at"], p["deadline"],
        p["value_amount"], p["value_currency"],
        p["cpv_code"], p["cpv_description"], p["cpv_codes"],
        p["industry_source_main"], p["industry_source_additional"], p["industry_bucket"],
        p["region"], p["is_live"], p["updated_at"], p["description"], p["submission_url"]
    ))


# -----------------------------
# MAIN
# -----------------------------

def run_rebuild():
    releases = fetch_all()

    if not releases:
        print("No data fetched. Abort.")
        return

    truncate_tables()

    conn = get_connection()
    cur = conn.cursor()

    count = 0

    for r in releases:
        try:
            data = normalize_release(r)

            insert_notice(cur, data["notice"])
            upsert_procurement(cur, data["procurement"])

            conn.commit()
            count += 1

            if count % 100 == 0:
                print(f"Inserted {count}")

        except Exception as e:
            conn.rollback()
            cur = conn.cursor()
            print(f"Error: {e}")

    conn.close()
    update_last_run()

    print(f"\nDONE. Inserted {count}")


if __name__ == "__main__":
    run_rebuild()
    
