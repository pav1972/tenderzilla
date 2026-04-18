def build_tender_text(row: dict) -> str:
    parts = []

    if row.get("title"):
        parts.append(f"Title: {row['title']}")

    if row.get("description"):
        parts.append(f"Description: {row['description']}")

    if row.get("cpv_description"):
        parts.append(f"Category: {row['cpv_description']}")

    if row.get("region"):
        parts.append(f"Region: {row['region']}")

    if row.get("buyer_name"):
        parts.append(f"Buyer: {row['buyer_name']}")

    value = row.get("value_amount")
    if value:
        parts.append(f"Contract value: {value} GBP")

    if row.get("latest_notice_type"):
        parts.append(f"Notice type: {row['latest_notice_type']}")

    return "\n".join(parts)