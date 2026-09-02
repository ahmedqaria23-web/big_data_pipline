"""
Incremental Loader — Path B (Change Data Capture / Delta Engine)

Architecture & Specifications:
1. High-Watermark Metric:
   - Primary metric: `updated_at` (ISO-8601 Timestamp).
   - Fallback metric: `order_date` for initial records lacking explicit `updated_at`.
   - Watermark state is persisted in MongoDB `meta_state` collection (`pipeline: path_b_incremental`).

2. Change Discovery & Delta Filtering:
   - Initial Load: Loads baseline dataset and initializes the Watermark.
   - Delta Load: Filters incoming stream for records where `updated_at > watermark`.

3. Version Conflict Resolution (Latest-Wins & Stale Rejection):
   - Compares incoming `version` against stored `version` in `orders_validated`.
   - If `incoming_version >= stored_version`: Record is accepted for upsert.
   - If `incoming_version < stored_version`: Stale update is rejected (`conflicts_rejected`).

4. Atomic Upsert & True Idempotency:
   - Integrates with `upsert_validated_batch` to execute atomic `ReplaceOne(..., upsert=True)`.
   - Accurately tracks:
     * `count_inserted`: Truly new records.
     * `count_updated`: Existing records with modified business state.
     * `count_unchanged`: Existing records whose business state is identical (on re-runs).
   - Re-running the same Delta batch is 100% idempotent.
"""

from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from pymongo.database import Database

from config.settings import COLLECTION_META_STATE, COLLECTION_VALIDATED
from src.mongodb.repositories import upsert_validated_batch


def get_watermark(db: Database, pipeline_name: str = "path_b_incremental") -> str:
    """Retrieves the last committed watermark timestamp from meta_state collection."""
    state = db[COLLECTION_META_STATE].find_one({"pipeline": pipeline_name})
    if state and "last_watermark" in state:
        return state["last_watermark"]
    return "1970-01-01T00:00:00Z"


def save_watermark(
    db: Database,
    new_watermark: str,
    pipeline_name: str = "path_b_incremental",
    additional_meta: Optional[Dict[str, Any]] = None
):
    """Persists the updated watermark timestamp into meta_state collection."""
    payload = {
        "last_watermark": new_watermark,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    if additional_meta:
        payload.update(additional_meta)

    db[COLLECTION_META_STATE].update_one(
        {"pipeline": pipeline_name},
        {"$set": payload},
        upsert=True
    )


def initial_load(
    db: Database,
    records: List[Dict[str, Any]],
    pipeline_name: str = "path_b_incremental"
) -> Dict[str, Any]:
    """
    Executes an Initial Baseline Load for Path B.
    Inserts initial records, resets watermark, and initializes tracking state.
    """
    if not records:
        return {
            "mode": "initial_load",
            "records_count": 0,
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
            "watermark": "1970-01-01T00:00:00Z"
        }

    max_ts = "1970-01-01T00:00:00Z"
    for r in records:
        ts = r.get("updated_at") or r.get("order_date") or "1970-01-01T00:00:00Z"
        if ts > max_ts:
            max_ts = ts

    inserted, updated, unchanged = upsert_validated_batch(db, records)
    save_watermark(db, max_ts, pipeline_name, {"mode": "initial_load", "baseline_records": len(records)})

    return {
        "mode": "initial_load",
        "records_count": len(records),
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "watermark": max_ts
    }


def process_delta_batch(
    db: Database,
    records: List[Dict[str, Any]],
    pipeline_name: str = "path_b_incremental",
    force_process: bool = False
) -> Dict[str, Any]:
    """
    Processes an Incremental Delta Batch containing new and modified records.
    Filters by Watermark (`updated_at > watermark`), enforces version conflict resolution,
    and performs idempotent upsert into `orders_validated`.
    """
    watermark = get_watermark(db, pipeline_name)

    # 1. Discover Delta Records
    if force_process:
        delta_records = records
    else:
        delta_records = [
            r for r in records
            if (r.get("updated_at") or r.get("order_date") or "9999-12-31T23:59:59Z") > watermark
        ]

    if not delta_records:
        return {
            "mode": "delta_load",
            "watermark_used": watermark,
            "delta_records_read": 0,
            "processed_count": 0,
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
            "conflicts_rejected": 0,
            "new_watermark": watermark
        }

    # 2. Conflict Resolution (Latest-Wins & Version Check)
    id_orders = [
        str(r.get("id_order") or r.get("order_id")).strip()
        for r in delta_records
        if (r.get("id_order") or r.get("order_id"))
    ]
    existing_docs = {
        doc["id_order"]: doc
        for doc in db[COLLECTION_VALIDATED].find({"id_order": {"$in": id_orders}})
    }

    eligible_records = []
    conflicts_rejected = 0
    max_watermark = watermark

    for rec in delta_records:
        raw_id = rec.get("id_order") or rec.get("order_id")
        if not raw_id:
            continue
        id_order = str(raw_id).strip()
        rec["id_order"] = id_order

        incoming_ver = rec.get("version", 1)
        rec_ts = rec.get("updated_at") or rec.get("order_date") or watermark

        if id_order in existing_docs:
            stored_doc = existing_docs[id_order]
            stored_ver = stored_doc.get("version", 0)
            stored_ts = stored_doc.get("updated_at") or stored_doc.get("order_date") or "1970-01-01T00:00:00Z"

            if incoming_ver > stored_ver or (incoming_ver == stored_ver and rec_ts >= stored_ts):
                eligible_records.append(rec)
            else:
                conflicts_rejected += 1
        else:
            eligible_records.append(rec)

        if rec_ts > max_watermark:
            max_watermark = rec_ts

    # 3. Perform atomic upsert for eligible delta records only
    inserted, updated, unchanged = upsert_validated_batch(db, eligible_records)
    save_watermark(db, max_watermark, pipeline_name, {
        "mode": "delta_load",
        "last_delta_size": len(eligible_records)
    })

    return {
        "mode": "delta_load",
        "watermark_used": watermark,
        "delta_records_read": len(delta_records),
        "processed_count": len(eligible_records),
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "conflicts_rejected": conflicts_rejected,
        "new_watermark": max_watermark
    }
