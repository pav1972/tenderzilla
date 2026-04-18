def build_profile_text(profile: dict) -> str:
    parts = []

    if profile.get("core_capabilities"):
        parts.append("Core capabilities: " + ", ".join(profile["core_capabilities"]))

    if profile.get("secondary_capabilities"):
        parts.append("Secondary capabilities: " + ", ".join(profile["secondary_capabilities"]))

    if profile.get("industry_focus"):
        parts.append("Industry focus: " + ", ".join(profile["industry_focus"]))

    if profile.get("technologies_vendors"):
        parts.append("Technologies/vendors: " + ", ".join(profile["technologies_vendors"]))

    if profile.get("preferred_regions"):
        parts.append("Preferred regions: " + ", ".join(profile["preferred_regions"]))

    if profile.get("excluded_sectors"):
        parts.append("Excluded sectors: " + ", ".join(profile["excluded_sectors"]))

    if profile.get("acceptable_min_tender_value"):
        parts.append(f"Minimum value: {profile['acceptable_min_tender_value']} GBP")

    if profile.get("closing_within_days"):
        parts.append(f"Closing within: {profile['closing_within_days']} days")

    return "\n".join(parts)