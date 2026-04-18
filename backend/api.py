import os
import re
from datetime import datetime, timezone
from typing import List, Optional

import psycopg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------
# DB
# -----------------------------


def get_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg.connect(database_url)


# -----------------------------
# MODELS
# -----------------------------


class CompanyProfilePayload(BaseModel):
    profile_name: str = "default"

    core_capabilities: List[str] = []
    secondary_capabilities: List[str] = []
    industry_focus: List[str] = []
    technologies_vendors: List[str] = []
    excluded_sectors: List[str] = []
    preferred_regions: List[str] = []

    acceptable_min_tender_value: Optional[float] = None
    closing_within_days: Optional[int] = None


# -----------------------------
# STAGE A FILTER MAPPING
# -----------------------------


FRONTEND_REGION_OPTIONS = {
    "uk": "UK",
    "europe ex uk": "Europe ex-UK",
    "world ex europe uk": "World ex-Europe, UK",
    "whole world": "Whole world",
}

INDUSTRY_CPV_PREFIXES = {
    "it software": ["72", "48"],
    "telecom connectivity": ["32", "324", "325", "5033", "50332", "45314"],
    "engineering technical": ["71", "73", "713", "765"],
    "construction works": ["45"],
    "healthcare": ["85", "33"],
    "education training": ["80"],
    "transport logistics": ["60", "34"],
    "cleaning facilities": ["909", "797"],
    "waste environmental": ["905", "90", "09"],
    "legal professional services": ["79", "66"],
}


# -----------------------------
# SAFE HELPERS
# -----------------------------


def normalize_text(value) -> str:
    text = str(value or "").lower().strip()
    text = re.sub(r"[_/|]+", " ", text)
    text = re.sub(r"[^a-z0-9\s\-\&]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_cpv_code(cpv_code) -> str:
    return re.sub(r"[^0-9]", "", str(cpv_code or "").strip())


def parse_deadline(deadline_value):
    if not deadline_value:
        return None

    if isinstance(deadline_value, datetime):
        if deadline_value.tzinfo is None:
            return deadline_value.replace(tzinfo=timezone.utc)
        return deadline_value

    deadline_str = str(deadline_value).strip()
    if not deadline_str:
        return None

    try:
        if deadline_str.endswith("Z"):
            deadline_str = deadline_str.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(deadline_str)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


def payload_to_dict(payload: CompanyProfilePayload) -> dict:
    if hasattr(payload, "model_dump"):
        return payload.model_dump()
    return payload.dict()


def profile_for_scoring(payload: CompanyProfilePayload) -> dict:
    data = payload_to_dict(payload)
    return {
        "core_capabilities": data.get("core_capabilities", []),
        "secondary_capabilities": data.get("secondary_capabilities", []),
        "industry_focus": data.get("industry_focus", []),
        "technologies_vendors": data.get("technologies_vendors", []),
        "excluded_sectors": data.get("excluded_sectors", []),
        "preferred_regions": data.get("preferred_regions", []),
        "acceptable_min_tender_value": data.get("acceptable_min_tender_value"),
        "closing_within_days": data.get("closing_within_days"),
    }


def canonical_region_option(region_value: str) -> str:
    region_norm = normalize_text(region_value)
    return FRONTEND_REGION_OPTIONS.get(region_norm, str(region_value or "").strip())


def tender_matches_region_bucket(region: str, selected_region: str) -> bool:
    region_raw = str(region or "").strip()
    region_norm = normalize_text(region_raw)
    selected_norm = normalize_text(selected_region)

    if not selected_norm:
        return False

    if selected_norm == "whole world":
        return True

    if selected_norm == "uk":
        return region_raw.upper().startswith("UK")

    if selected_norm == "europe ex uk":
        return (
            bool(region_raw)
            and not region_raw.upper().startswith("UK")
            and (
                region_norm.startswith("eu")
                or "europe" in region_norm
                or "european" in region_norm
            )
        )

    if selected_norm == "world ex europe uk":
        return (
            bool(region_raw)
            and not region_raw.upper().startswith("UK")
            and "europe" not in region_norm
            and not region_norm.startswith("eu")
        )

    return normalize_text(selected_region) == region_norm


def passes_region_filter(tender: dict, preferred_regions: List[str]) -> bool:
    if not preferred_regions:
        return True

    tender_region = tender.get("region")
    for selected_region in preferred_regions:
        if tender_matches_region_bucket(tender_region, selected_region):
            return True

    return False


def tender_matches_industry_bucket(cpv_code: str, selected_industry: str) -> bool:
    cpv = normalize_cpv_code(cpv_code)
    industry_norm = normalize_text(selected_industry)

    if not cpv or not industry_norm:
        return False

    prefixes = INDUSTRY_CPV_PREFIXES.get(industry_norm, [])
    return any(cpv.startswith(prefix) for prefix in prefixes)


def get_matching_industry_buckets(cpv_code: str, selected_industries: List[str]) -> List[str]:
    matches = []

    for item in selected_industries or []:
        if tender_matches_industry_bucket(cpv_code, item):
            matches.append(str(item).strip())

    seen = set()
    result = []
    for item in matches:
        key = normalize_text(item)
        if key and key not in seen:
            seen.add(key)
            result.append(item)

    return result


def passes_industry_filter(tender: dict, industry_focus: List[str]) -> bool:
    if not industry_focus:
        return True

    cpv_code = tender.get("cpv_code")
    matches = get_matching_industry_buckets(cpv_code, industry_focus)
    return len(matches) > 0


def passes_value_filter(tender: dict, acceptable_min_tender_value: Optional[float]) -> bool:
    if acceptable_min_tender_value in (None, ""):
        return True

    tender_value = tender.get("value_amount")
    if tender_value in (None, "", "—"):
        return False

    try:
        return float(tender_value) >= float(acceptable_min_tender_value)
    except Exception:
        return False


def passes_deadline_filter(tender: dict, closing_within_days: Optional[int]) -> bool:
    if closing_within_days in (None, ""):
        return True

    deadline = parse_deadline(tender.get("deadline"))
    if not deadline:
        return False

    try:
        max_days = int(closing_within_days)
    except Exception:
        return False

    now_utc = datetime.now(timezone.utc)
    days_left = (deadline - now_utc).days

    if days_left < 0:
        return False

    return days_left <= max_days


def apply_stage_a_filters(tenders: list[dict], company_profile: dict) -> list[dict]:
    preferred_regions = company_profile.get("preferred_regions", []) or []
    industry_focus = company_profile.get("industry_focus", []) or []
    acceptable_min_tender_value = company_profile.get("acceptable_min_tender_value")
    closing_within_days = company_profile.get("closing_within_days")

    filtered = []
    for tender in tenders:
        if not passes_region_filter(tender, preferred_regions):
            continue
        if not passes_industry_filter(tender, industry_focus):
            continue
        if not passes_value_filter(tender, acceptable_min_tender_value):
            continue
        if not passes_deadline_filter(tender, closing_within_days):
            continue
        filtered.append(tender)

    return filtered


# TODO: re-enable vector ranking endpoint after DB and imports are verified safe on Render.


# -----------------------------
# ROUTES
# -----------------------------


@app.get("/")
def root():
    return {"message": "TenderZilla API is running"}


@app.get("/health")
def health():
    return {"status": "ok", "service": "tenderzilla-api"}


@app.get("/test")
def test():
    return {"test": "success", "message": "API is working correctly"}


@app.get("/db-test")
def db_test():
    try:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()[0]
        finally:
            conn.close()
        return {"db": "ok", "result": result}
    except Exception as e:
        return {"db": "error", "details": str(e)}


@app.get("/tenders-all")
def get_tenders_all(limit: int = 10000):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    p.ocid,
                    p.title,
                    p.description,
                    p.buyer_name,
                    p.latest_notice_type,
                    p.deadline,
                    p.value_amount,
                    p.value_currency,
                    p.value_amount AS value,
                    p.cpv_code,
                    p.cpv_description,
                    p.region,
                    p.submission_url
                FROM procurements p
                WHERE p.is_live = TRUE
                  AND p.latest_notice_type = 'TENDER'
                ORDER BY p.deadline ASC NULLS LAST
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
            columns = [d.name for d in cur.description]
    finally:
        conn.close()

    return [dict(zip(columns, row)) for row in rows]
