from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any


MIN_TOKEN_LENGTH = 3
GENERIC_LOW_SIGNAL_TERMS = {
    "it",
    "uk",
    "aws",
    "api",
    "crm",
    "sap",
}

FRONTEND_REGION_OPTIONS = {
    "uk": "UK",
    "europe ex uk": "Europe ex-UK",
    "world ex europe uk": "World ex-Europe, UK",
    "whole world": "Whole world",
}

INDUSTRY_CPV_PREFIXES = {
    "it / software": ["72", "48"],
    "telecom / connectivity": ["32", "324", "325", "5033", "50332", "45314"],
    "engineering / technical": ["71", "73", "713", "765"],
    "construction / works": ["45"],
    "healthcare": ["85", "33"],
    "education / training": ["80"],
    "transport / logistics": ["60", "34"],
    "cleaning / facilities": ["909", "797"],
    "waste / environmental": ["905", "90", "09"],
    "legal / professional services": ["79", "66"],
}


def normalize_text(text: Any) -> str:
    """
    Lowercase + remove punctuation-ish noise + collapse whitespace.
    """
    value = str(text or "").lower().strip()
    value = re.sub(r"[_/|]+", " ", value)
    value = re.sub(r"[^a-z0-9\s\-\&]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def tokenize(text: Any) -> list[str]:
    """
    Tokenize normalized text into useful tokens.
    """
    text_norm = normalize_text(text)
    if not text_norm:
        return []

    tokens = re.findall(r"[a-z0-9]+", text_norm)
    return [t for t in tokens if len(t) >= MIN_TOKEN_LENGTH]


def normalize_cpv_code(cpv_code: Any) -> str:
    """
    Normalize CPV code to digits only so prefix checks are stable.
    """
    return re.sub(r"[^0-9]", "", str(cpv_code or "").strip())


def canonical_region_option(region_value: Any) -> str:
    """
    Normalize region bucket names coming from frontend/profile.
    """
    region_norm = normalize_text(region_value)
    return FRONTEND_REGION_OPTIONS.get(region_norm, str(region_value or "").strip())


def canonical_industry_option(industry_value: Any) -> str:
    """
    Normalize industry bucket names coming from frontend/profile.
    """
    return normalize_text(industry_value)


def tender_matches_region_bucket(region: Any, selected_region: Any) -> bool:
    """
    Map raw tender region codes to one of the 4 frontend region options.
    Current dataset is mainly UK-coded regions such as UK, UKI32, UKC, etc.
    """
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


def get_matching_region_buckets(
    region: Any,
    preferred_regions: list[str],
) -> list[str]:
    """
    Return all preferred frontend region buckets that match this tender region.
    """
    matches: list[str] = []

    for item in preferred_regions or []:
        if tender_matches_region_bucket(region, item):
            matches.append(canonical_region_option(item))

    return unique_preserve_order(matches)


def tender_matches_industry_bucket(cpv_code: Any, selected_industry: Any) -> bool:
    """
    Check whether a tender CPV code matches a frontend industry bucket
    using CPV prefix logic.
    """
    cpv = normalize_cpv_code(cpv_code)
    industry_norm = canonical_industry_option(selected_industry)

    if not cpv or not industry_norm:
        return False

    prefixes = INDUSTRY_CPV_PREFIXES.get(industry_norm, [])
    return any(cpv.startswith(prefix) for prefix in prefixes)


def get_matching_industry_buckets(
    cpv_code: Any,
    selected_industries: list[str],
) -> list[str]:
    """
    Multi-match helper:
    one CPV may match more than one frontend industry bucket.
    """
    matches: list[str] = []

    for item in selected_industries or []:
        if tender_matches_industry_bucket(cpv_code, item):
            matches.append(str(item).strip())

    return unique_preserve_order(matches)


def parse_deadline(deadline_value: Any) -> datetime | None:
    """
    Safely parse deadline values coming from DB / API.
    Supports datetime objects and ISO strings.
    """
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


def build_matching_text(tender: dict) -> str:
    """
    Build the text used for lexical rule checks.
    This is separate from the embedding text if needed.
    """
    parts = [
        tender.get("title", ""),
        tender.get("description", ""),
        tender.get("cpv_description", ""),
        tender.get("buyer_name", ""),
        tender.get("region", ""),
        tender.get("latest_notice_type", ""),
    ]
    return normalize_text(" ".join(str(p or "") for p in parts if p))


def unique_preserve_order(values: list[str]) -> list[str]:
    seen = set()
    result = []
    for v in values:
        k = normalize_text(v)
        if k and k not in seen:
            seen.add(k)
            result.append(v)
    return result


def token_overlap_ratio(phrase: str, text: str) -> float:
    """
    Soft phrase match:
    - exact substring gives 1.0
    - otherwise compute overlap of phrase tokens found in text tokens
    """
    phrase_norm = normalize_text(phrase)
    text_norm = normalize_text(text)

    if not phrase_norm or not text_norm:
        return 0.0

    if phrase_norm in text_norm:
        return 1.0

    phrase_tokens = [
        t for t in tokenize(phrase_norm)
        if t not in GENERIC_LOW_SIGNAL_TERMS
    ]
    text_tokens = set(tokenize(text_norm))

    if not phrase_tokens:
        return 0.0

    overlap = sum(1 for t in phrase_tokens if t in text_tokens)
    return overlap / len(phrase_tokens)


def phrase_match_strength(phrase: str, text: str) -> tuple[float, str]:
    """
    Return match strength and match type.
    """
    ratio = token_overlap_ratio(phrase, text)

    if ratio >= 0.999:
        return 1.0, "exact"
    if ratio >= 0.66:
        return 0.65, "soft"
    if ratio >= 0.5:
        return 0.4, "partial"
    return 0.0, "none"


def weighted_phrase_score(
    phrases: list[str],
    text: str,
    exact_points_per_hit: int,
    soft_points_per_hit: int,
    partial_points_per_hit: int,
    max_score: int,
) -> tuple[int, list[str], dict[str, str]]:
    """
    Phrase matching with:
    - exact match
    - soft token overlap match
    - partial token overlap match
    """
    matched: list[str] = []
    match_types: dict[str, str] = {}
    score = 0

    for phrase in phrases or []:
        phrase_norm = normalize_text(phrase)
        if not phrase_norm:
            continue

        _, match_type = phrase_match_strength(phrase_norm, text)

        if match_type == "exact":
            matched.append(phrase)
            match_types[phrase] = "exact"
            score += exact_points_per_hit
        elif match_type == "soft":
            matched.append(phrase)
            match_types[phrase] = "soft"
            score += soft_points_per_hit
        elif match_type == "partial" and partial_points_per_hit > 0:
            matched.append(phrase)
            match_types[phrase] = "partial"
            score += partial_points_per_hit

    score = min(score, max_score)
    return score, unique_preserve_order(matched), match_types


def semantic_points(similarity: float) -> int:
    """
    Convert cosine similarity-ish number into stable business points.
    Expect similarity roughly in [0, 1].
    """
    try:
        s = float(similarity)
    except Exception:
        return 0

    if s < 0.20:
        return 0
    if s < 0.30:
        return 10
    if s < 0.40:
        return 20
    if s < 0.50:
        return 30
    if s < 0.60:
        return 40
    if s < 0.70:
        return 50
    return 60


def geography_score(region: Any, preferred_regions: list[str]) -> tuple[int, list[str]]:
    """
    Reward preferred geography matches.

    Supports the 4 frontend buckets:
    - UK
    - Europe ex-UK
    - World ex-Europe, UK
    - Whole world

    Also keeps legacy direct text matching for non-bucket region values.
    """
    region_norm = normalize_text(region)
    if not region_norm:
        return 0, []

    matched = []
    score = 0

    bucket_matches = get_matching_region_buckets(region, preferred_regions)
    if bucket_matches:
        matched.extend(bucket_matches)
        score += min(6, len(bucket_matches) * 6)

    for item in preferred_regions or []:
        item_norm = normalize_text(item)
        if not item_norm:
            continue

        if item_norm in FRONTEND_REGION_OPTIONS:
            continue

        if item_norm == region_norm:
            matched.append(item)
            score += 6
        elif item_norm in region_norm:
            matched.append(item)
            score += 4

    return min(score, 10), unique_preserve_order(matched)


def value_score(
    tender_value: Any,
    acceptable_min_tender_value: Any,
) -> tuple[int, str]:
    """
    Value scoring with a correct explanatory note.
    """
    if tender_value in (None, "", "—"):
        return 0, "Tender value not available"

    if acceptable_min_tender_value in (None, ""):
        return 0, "No minimum tender value specified"

    try:
        tv = float(tender_value)
        mv = float(acceptable_min_tender_value)

        if tv >= mv:
            return 10, "Tender value meets the acceptable minimum"

        if tv >= mv * 0.75:
            return 4, "Tender value is slightly below the acceptable minimum"

        return 0, "Tender value is below the acceptable minimum"
    except Exception:
        return 0, "Tender value could not be evaluated"


def deadline_score(deadline_value: Any, closing_within_days: Any) -> tuple[int, str]:
    """
    Business logic:
    - expired tender => 0
    - if user specified closing window and tender falls inside it => positive
    - otherwise 0
    """
    deadline = parse_deadline(deadline_value)
    if not deadline:
        return 0, "No valid deadline available"

    try:
        if closing_within_days in (None, ""):
            return 0, "No closing window specified"

        now_utc = datetime.now(timezone.utc)
        days_left = (deadline - now_utc).days

        if days_left < 0:
            return 0, "Tender deadline has already passed"

        max_days = int(closing_within_days)

        if days_left <= max_days:
            if days_left <= 7:
                return 8, f"Closing soon ({days_left} days left)"
            return 6, f"Within preferred closing window ({days_left} days left)"

        return 0, f"Outside preferred closing window ({days_left} days left)"
    except Exception:
        return 0, "Could not evaluate deadline fit"


def exclusion_penalty(
    excluded_sectors: list[str],
    text: str,
) -> tuple[int, list[str]]:
    """
    Penalize explicit excluded sector matches.
    Soft overlap is allowed for multi-token excluded phrases.
    """
    hits = []
    penalty = 0

    for phrase in excluded_sectors or []:
        phrase_norm = normalize_text(phrase)
        if not phrase_norm:
            continue

        _, match_type = phrase_match_strength(phrase_norm, text)

        if match_type == "exact":
            hits.append(phrase)
            penalty += 12
        elif match_type == "soft":
            hits.append(phrase)
            penalty += 8

    return min(penalty, 30), unique_preserve_order(hits)


def notice_type_score(notice_type: Any) -> tuple[int, str]:
    """
    Reward live tender-like notices, penalize award/other if needed.
    """
    nt = normalize_text(notice_type)

    if not nt:
        return 0, "No notice type available"

    if "tender" in nt:
        return 6, "Notice type supports active tendering"
    if "award" in nt:
        return -8, "Award notice is less useful for active bidding"
    if "contract" in nt:
        return 1, "Contract-related notice may still be commercially relevant"
    if "other" in nt:
        return -4, "Other notice type is less directly actionable"

    return 0, "Neutral notice type"


def score_to_band(score: int) -> str:
    if score >= 75:
        return "Strong fit"
    if score >= 50:
        return "Moderate fit"
    return "Weak fit"


def format_match_notes(match_types: dict[str, str]) -> list[str]:
    notes = []
    for phrase, match_type in match_types.items():
        if match_type == "exact":
            notes.append(f"{phrase} (exact)")
        elif match_type == "soft":
            notes.append(f"{phrase} (soft)")
        elif match_type == "partial":
            notes.append(f"{phrase} (partial)")
    return notes


def compute_pre_score_v2(
    tender: dict,
    company_profile: dict,
    semantic_similarity: float,
) -> dict:
    """
    Hybrid scoring:
    - semantic similarity
    - weighted lexical matches
    - geography
    - value
    - deadline
    - exclusions penalty
    - notice type usefulness
    """
    matching_text = build_matching_text(tender)

    core_capabilities = company_profile.get("core_capabilities", []) or []
    secondary_capabilities = company_profile.get("secondary_capabilities", []) or []
    industry_focus = company_profile.get("industry_focus", []) or []
    technologies_vendors = company_profile.get("technologies_vendors", []) or []
    excluded_sectors = company_profile.get("excluded_sectors", []) or []
    preferred_regions = company_profile.get("preferred_regions", []) or []

    acceptable_min_tender_value = company_profile.get("acceptable_min_tender_value")
    closing_within_days = company_profile.get("closing_within_days")

    semantic = semantic_points(semantic_similarity)

    core_score, matched_core, core_match_types = weighted_phrase_score(
        phrases=core_capabilities,
        text=matching_text,
        exact_points_per_hit=6,
        soft_points_per_hit=3,
        partial_points_per_hit=1,
        max_score=16,
    )

    secondary_score, matched_secondary, secondary_match_types = weighted_phrase_score(
        phrases=secondary_capabilities,
        text=matching_text,
        exact_points_per_hit=3,
        soft_points_per_hit=1,
        partial_points_per_hit=0,
        max_score=8,
    )

    industry_score, matched_industry, industry_match_types = weighted_phrase_score(
        phrases=industry_focus,
        text=matching_text,
        exact_points_per_hit=3,
        soft_points_per_hit=1,
        partial_points_per_hit=0,
        max_score=6,
    )

    matched_industry_buckets = get_matching_industry_buckets(
        cpv_code=tender.get("cpv_code"),
        selected_industries=industry_focus,
    )
    if matched_industry_buckets:
        matched_industry = unique_preserve_order(matched_industry + matched_industry_buckets)
        industry_score = min(6, industry_score + (len(matched_industry_buckets) * 3))

    technology_score, matched_technologies, technology_match_types = weighted_phrase_score(
        phrases=technologies_vendors,
        text=matching_text,
        exact_points_per_hit=2,
        soft_points_per_hit=1,
        partial_points_per_hit=0,
        max_score=6,
    )

    geo_score, matched_regions = geography_score(
        region=tender.get("region"),
        preferred_regions=preferred_regions,
    )

    val_score, value_note = value_score(
        tender_value=tender.get("value_amount") or tender.get("value"),
        acceptable_min_tender_value=acceptable_min_tender_value,
    )

    dl_score, deadline_note = deadline_score(
        deadline_value=tender.get("deadline"),
        closing_within_days=closing_within_days,
    )

    penalty, triggered_exclusions = exclusion_penalty(
        excluded_sectors=excluded_sectors,
        text=matching_text,
    )

    nt_score, notice_note = notice_type_score(tender.get("latest_notice_type"))

    total = (
        semantic
        + core_score
        + secondary_score
        + industry_score
        + technology_score
        + geo_score
        + val_score
        + dl_score
        + nt_score
        - penalty
    )

    total = max(0, min(100, round(total)))

    if len(triggered_exclusions) >= 2:
        total = min(total, 25)

    notes: list[str] = []

    if semantic >= 50:
        notes.append("Strong semantic match to the company profile")
    elif semantic >= 30:
        notes.append("Moderate semantic match to the company profile")
    else:
        notes.append("Limited semantic match to the company profile")

    if matched_core:
        notes.append(
            "Matched core capabilities: "
            + ", ".join(format_match_notes(core_match_types))
        )

    if matched_secondary:
        notes.append(
            "Matched secondary capabilities: "
            + ", ".join(format_match_notes(secondary_match_types))
        )

    if matched_industry:
        notes.append(
            "Industry overlap found: "
            + ", ".join(matched_industry)
        )

    if matched_technologies:
        notes.append(
            "Technology/vendor overlap found: "
            + ", ".join(format_match_notes(technology_match_types))
        )

    if matched_regions:
        notes.append(f"Preferred geography matched: {', '.join(matched_regions)}")

    notes.append(value_note)
    notes.append(deadline_note)
    notes.append(notice_note)

    if triggered_exclusions:
        notes.append(
            f"Excluded sector signals found: {', '.join(triggered_exclusions)}"
        )

    return {
        "pre_score_v2": total,
        "fit_band": score_to_band(total),
        "semantic_similarity": round(float(semantic_similarity or 0), 4),
        "breakdown": {
            "semantic": semantic,
            "core_capabilities": core_score,
            "secondary_capabilities": secondary_score,
            "industry_focus": industry_score,
            "technologies_vendors": technology_score,
            "geography": geo_score,
            "value": val_score,
            "deadline": dl_score,
            "notice_type": nt_score,
            "exclusion_penalty": penalty,
        },
        "matched_core_capabilities": matched_core,
        "matched_secondary_capabilities": matched_secondary,
        "matched_industry_focus": matched_industry,
        "matched_technologies_vendors": matched_technologies,
        "matched_regions": matched_regions,
        "triggered_exclusions": triggered_exclusions,
        "notes": notes,
    }