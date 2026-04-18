from datetime import datetime, timezone


def detect_notice_type(release):
    """Basic notice type detection (MVP level)"""
    tag = release.get("tag", [])

    if "tender" in tag:
        return "TENDER"
    elif "award" in tag:
        return "AWARD"
    elif "contract" in tag:
        return "CONTRACT"
    else:
        return "OTHER"


def safe_get(d, path, default=None):
    """Safe nested access for dicts AND lists"""
    for key in path:
        if isinstance(key, int):
            if isinstance(d, list) and len(d) > key:
                d = d[key]
            else:
                return default
        else:
            if isinstance(d, dict):
                d = d.get(key)
            else:
                return default
    return d if d is not None else default


def extract_industry_sources(tender):
    """
    Minimal MVP extraction for industry:
    1. tender.classification.description
    2. tender.items[].additionalClassifications[].description
    """
    main_desc = safe_get(tender, ["classification", "description"])

    additional_descs = []
    items = tender.get("items", [])
    for item in items:
        for cls in item.get("additionalClassifications", []):
            desc = cls.get("description")
            if desc:
                additional_descs.append(desc)

    return main_desc, additional_descs


def extract_cpv_sources(tender):
    """
    Minimal MVP CPV code extraction:
    1. tender.classification.id
    2. tender.items[].additionalClassifications[].id
    """
    main_code = safe_get(tender, ["classification", "id"])

    additional_codes = []
    items = tender.get("items", [])
    for item in items:
        for cls in item.get("additionalClassifications", []):
            code = cls.get("id")
            if code:
                additional_codes.append(code)

    return main_code, additional_codes


def extract_cpv_description_fallback(tender):
    """
    Fallback CPV description from tender.items[].additionalClassifications[].description
    """
    items = tender.get("items", [])
    for item in items:
        for cls in item.get("additionalClassifications", []):
            desc = cls.get("description")
            if desc:
                return desc
    return None


def extract_cpv_code_fallback(tender):
    """
    Fallback CPV code from tender.items[].additionalClassifications[].id
    """
    items = tender.get("items", [])
    for item in items:
        for cls in item.get("additionalClassifications", []):
            code = cls.get("id")
            if code:
                return code
    return None


def extract_region_from_parties(release):
    """
    Fallback region extraction from release.parties[].address.region
    """
    parties = release.get("parties", [])
    for party in parties:
        address = party.get("address", {})
        region = address.get("region")
        if region:
            return region
    return None


def build_find_tender_url(notice_id):
    """
    Build canonical Find a Tender URL from release/notice id.
    """
    if not notice_id:
        return None
    return f"https://www.find-tender.service.gov.uk/Notice/{notice_id}"


def map_industry_bucket(main_desc, additional_descs, main_code=None, additional_codes=None):
    """
    Minimal MVP industry buckets:
    - IT / Software
    - Telecom / Connectivity
    - Construction / Works
    - Engineering / Technical

    Logic:
    1. Try descriptions first
    2. If no match -> fallback to CPV codes
    """
    texts = []

    if main_desc:
        texts.append(main_desc.lower())

    for d in additional_descs or []:
        if d:
            texts.append(d.lower())

    full_text = " | ".join(texts)

    telecom_keywords = [
        "telecom",
        "connectivity",
        "network",
        "fibre",
        "fiber",
        "broadband",
        "communications",
        "transmission",
        "signalling",
        "signaling",
    ]

    it_keywords = [
        "software",
        "digital",
        "data",
        "platform",
        "information technology",
        "internet",
        "cloud",
        "systems",
        "application",
        "it services",
    ]

    construction_keywords = [
        "construction",
        "building",
        "civil",
        "roof",
        "roofing",
        "works",
        "refurbishment",
        "fire-prevention",
        "installation works",
    ]

    engineering_keywords = [
        "engineering",
        "technical",
        "testing",
        "laboratory",
        "maintenance",
        "equipment",
        "architectural",
        "architecture",
        "technical analysis",
        "technical consultancy",
    ]

    # 1. Description-first logic (unchanged)
    if any(k in full_text for k in telecom_keywords):
        return "Telecom / Connectivity"
    if any(k in full_text for k in it_keywords):
        return "IT / Software"
    if any(k in full_text for k in construction_keywords):
        return "Construction / Works"
    if any(k in full_text for k in engineering_keywords):
        return "Engineering / Technical"

    # 2. CPV code fallback (unchanged)
    codes = []
    if main_code:
        codes.append(str(main_code))
    for c in additional_codes or []:
        if c:
            codes.append(str(c))

    for code in codes:
        if code.startswith(("32", "64")):
            return "Telecom / Connectivity"
        if code.startswith(("48", "72")) or code.startswith("724"):
            return "IT / Software"
        if code.startswith(("45", "452", "453", "454")):
            return "Construction / Works"
        if code.startswith(("71", "716", "719")):
            return "Engineering / Technical"

    return None


def extract_deadline(release):
    """
    Unified deadline mapping with fallback chain.

    Priority:
    1. tender.tenderPeriod.endDate
    2. tender.expressionOfInterestDeadline
    3. tender.communication.futureNoticeDate
    4. planning.communication.futureNoticeDate
    5. planning.milestones[].dueDate
    6. awards[0].standstillPeriod.endDate
    7. awards[0].milestones[].dueDate
    """
    tender = release.get("tender", {})
    planning = release.get("planning", {})
    awards = release.get("awards", [])

    deadline = (
        safe_get(tender, ["tenderPeriod", "endDate"])
        or tender.get("expressionOfInterestDeadline")
        or safe_get(tender, ["communication", "futureNoticeDate"])
        or safe_get(planning, ["communication", "futureNoticeDate"])
    )
    if deadline:
        return deadline

    for milestone in planning.get("milestones", []):
        due_date = milestone.get("dueDate")
        if due_date:
            return due_date

    award = awards[0] if awards else {}

    deadline = safe_get(award, ["standstillPeriod", "endDate"])
    if deadline:
        return deadline

    for milestone in award.get("milestones", []):
        due_date = milestone.get("dueDate")
        if due_date:
            return due_date

    return None


def map_release_to_notice(release):
    tender = release.get("tender", {})
    buyer = release.get("buyer", {})
    awards = release.get("awards", [])
    contracts = release.get("contracts", [])

    award = awards[0] if awards else {}
    items = award.get("items", [])
    item = items[0] if items else {}

    # CPV
    cpv_main = safe_get(tender, ["classification", "id"]) or extract_cpv_code_fallback(tender)
    cpv_desc = safe_get(tender, ["classification", "description"]) or extract_cpv_description_fallback(tender)

    cpv_codes = []
    additional = tender.get("additionalClassifications", [])
    for c in additional:
        if c.get("id"):
            cpv_codes.append(c.get("id"))

    # also collect cpv_codes from tender.items[].additionalClassifications[]
    _, item_additional_codes = extract_cpv_sources(tender)
    for code in item_additional_codes:
        if code not in cpv_codes:
            cpv_codes.append(code)

    # Industry sources
    industry_source_main, industry_source_additional = extract_industry_sources(tender)

    # CPV source codes for fallback mapping
    cpv_main_code, cpv_additional_codes = extract_cpv_sources(tender)

    industry_bucket = map_industry_bucket(
        industry_source_main,
        industry_source_additional,
        cpv_main_code,
        cpv_additional_codes,
    )

    # Region (fallback logic patched)
    region = (
        safe_get(tender, ["deliveryLocation", "description"])
        or safe_get(item, ["deliveryAddresses", 0, "region"])
        or extract_region_from_parties(release)
    )

    location = safe_get(item, ["deliveryAddresses", 0, "streetAddress"])

    # Value
    value_amount = safe_get(tender, ["value", "amount"])
    value_currency = safe_get(tender, ["value", "currency"])

    # Deadline (patched with multi-field fallback)
    deadline = extract_deadline(release)

    # URL (priority logic with Find a Tender fallback)
    url = None
    if tender.get("documents"):
        url = tender["documents"][0].get("url")
    elif award.get("documents"):
        url = award["documents"][0].get("url")
    elif contracts:
        docs = contracts[0].get("documents", [])
        if docs:
            url = docs[0].get("url")

    if not url:
        url = build_find_tender_url(release.get("id"))

    notice = {
        "id": release.get("id"),
        "ocid": release.get("ocid"),

        "notice_type": detect_notice_type(release),
        "stage": (release.get("tag") or [None])[0],

        "title": tender.get("title"),
        "description": tender.get("description"),
        "buyer_name": buyer.get("name"),

        "published_at": release.get("date"),
        "deadline": deadline,

        "value_amount": value_amount,
        "value_currency": value_currency,

        "cpv_code": cpv_main,
        "cpv_description": cpv_desc,
        "cpv_codes": cpv_codes,

        "industry_source_main": industry_source_main,
        "industry_source_additional": industry_source_additional,
        "industry_bucket": industry_bucket,

        "region": region,
        "location": location,

        "procurement_method": tender.get("procurementMethod"),
        "procurement_method_details": tender.get("procurementMethodDetails"),

        "is_framework": tender.get("hasEnquiries"),  # placeholder

        "submission_url": url,

        "is_suitable_for_sme": safe_get(tender, ["suitability", "sme"]),
        "is_suitable_for_vco": safe_get(tender, ["suitability", "vco"]),

        "economic_criteria": safe_get(tender, ["criteria", "economic"]),
        "economic_minimum": safe_get(tender, ["criteria", "economic", "minimum"]),

        "technical_criteria": safe_get(tender, ["criteria", "technical"]),
        "technical_minimum": safe_get(tender, ["criteria", "technical", "minimum"]),

        "raw_json": release
    }

    return notice


def map_release_to_procurement(release):
    tender = release.get("tender", {})
    buyer = release.get("buyer", {})

    awards = release.get("awards", [])
    award = awards[0] if awards else {}
    items = award.get("items", [])
    item = items[0] if items else {}

    deadline = extract_deadline(release)
    published = release.get("date")

    notice_type = detect_notice_type(release)

    # Industry sources
    industry_source_main, industry_source_additional = extract_industry_sources(tender)

    # CPV source codes for fallback mapping
    cpv_main_code, cpv_additional_codes = extract_cpv_sources(tender)

    industry_bucket = map_industry_bucket(
        industry_source_main,
        industry_source_additional,
        cpv_main_code,
        cpv_additional_codes,
    )

    # Procurement submission URL / original tender URL
    submission_url = None
    if tender.get("documents"):
        submission_url = tender["documents"][0].get("url")
    elif award.get("documents"):
        submission_url = award["documents"][0].get("url")
    else:
        contracts = release.get("contracts", [])
        if contracts and contracts[0].get("documents"):
            submission_url = contracts[0]["documents"][0].get("url")

    if not submission_url:
        submission_url = build_find_tender_url(release.get("id"))

    # LIVE logic (correct + timezone-safe)
    is_live = False

    if notice_type == "TENDER":
        if deadline:
            try:
                now = datetime.now(timezone.utc)
                dl = datetime.fromisoformat(deadline.replace("Z", "+00:00"))
                is_live = dl > now
            except Exception:
                is_live = True
        else:
            is_live = True

    procurement = {
        "ocid": release.get("ocid"),

        "title": tender.get("title"),
        "description": tender.get("description"),
        "buyer_name": buyer.get("name"),

        "latest_notice_id": release.get("id"),
        "latest_notice_type": notice_type,
        "latest_stage": (release.get("tag") or [None])[0],

        "published_at": published,
        "deadline": deadline,

        "value_amount": safe_get(tender, ["value", "amount"]),
        "value_currency": safe_get(tender, ["value", "currency"]),

        "cpv_code": safe_get(tender, ["classification", "id"]) or extract_cpv_code_fallback(tender),
        "cpv_description": safe_get(tender, ["classification", "description"]) or extract_cpv_description_fallback(tender),
        "cpv_codes": cpv_additional_codes,

        "industry_source_main": industry_source_main,
        "industry_source_additional": industry_source_additional,
        "industry_bucket": industry_bucket,

        "region": (
            safe_get(tender, ["deliveryLocation", "description"])
            or safe_get(item, ["deliveryAddresses", 0, "region"])
            or extract_region_from_parties(release)
        ),

        "is_live": is_live,
        "submission_url": submission_url,

        "updated_at": published,
    }

    return procurement


def normalize_release(release):
    """Main function → returns BOTH notice and procurement"""
    return {
        "notice": map_release_to_notice(release),
        "procurement": map_release_to_procurement(release)
    }


def normalize_releases(data):
    """Normalize all releases from API"""
    releases = data.get("releases", [])

    result = []
    for r in releases:
        try:
            result.append(normalize_release(r))
        except Exception as e:
            print(f"Error normalizing release: {e}")

    return result