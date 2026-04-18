import hashlib
import psycopg
from psycopg.rows import dict_row
from sentence_transformers import SentenceTransformer

MODEL_NAME = "all-MiniLM-L6-v2"
model = SentenceTransformer(MODEL_NAME)


def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


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

    return "\n".join(parts).strip()


def embed(text: str):
    return model.encode(text, normalize_embeddings=True).tolist()


def main():
    conn = psycopg.connect("postgresql://localhost:5432/tenderzilla")

    # ---- 1. Берём только live procurements
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                ocid,
                title,
                description,
                cpv_description,
                buyer_name,
                region,
                latest_notice_type,
                value_amount,
                is_live
            FROM procurements
            WHERE is_live = TRUE
            """
        )
        rows = cur.fetchall()

    print(f"Found {len(rows)} live procurements")

    # ---- 2. Подтягиваем существующие hashes
    existing_hashes = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ocid, tender_text_hash
            FROM procurement_embeddings
            """
        )
        for ocid, tender_hash in cur.fetchall():
            existing_hashes[ocid] = tender_hash

    processed = 0
    updated = 0
    skipped = 0
    failed = 0

    with conn.cursor() as cur:
        for row in rows:
            try:
                ocid = row["ocid"]
                tender_text = build_tender_text(row)

                if not tender_text:
                    skipped += 1
                    continue

                tender_hash = text_hash(tender_text)

                # ---- 3. Если хеш не изменился — пропускаем
                if existing_hashes.get(ocid) == tender_hash:
                    skipped += 1
                    continue

                vector = embed(tender_text)

                cur.execute(
                    """
                    INSERT INTO procurement_embeddings (
                        ocid,
                        tender_text,
                        tender_text_hash,
                        embedding_model,
                        embedding
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (ocid)
                    DO UPDATE SET
                        tender_text = EXCLUDED.tender_text,
                        tender_text_hash = EXCLUDED.tender_text_hash,
                        embedding_model = EXCLUDED.embedding_model,
                        embedding = EXCLUDED.embedding,
                        updated_at = NOW()
                    """,
                    (
                        ocid,
                        tender_text,
                        tender_hash,
                        MODEL_NAME,
                        vector,
                    ),
                )

                updated += 1
                processed += 1

                if processed % 100 == 0:
                    conn.commit()
                    print(
                        f"Processed {processed} | updated={updated} | skipped={skipped} | failed={failed}"
                    )

            except Exception as e:
                failed += 1
                print(f"Failed on OCID {row.get('ocid')}: {e}")

        conn.commit()

    conn.close()

    print("\n✅ Done.")
    print(f"Updated embeddings: {updated}")
    print(f"Skipped unchanged/empty: {skipped}")
    print(f"Failed: {failed}")


if __name__ == "__main__":
    main()
