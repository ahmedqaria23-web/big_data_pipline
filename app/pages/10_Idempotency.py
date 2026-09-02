import sys
import json
from pathlib import Path

root_dir = Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

import streamlit as st
from src.mongodb.mongo_setup import get_mongo_db
from src.mongodb.repositories import find_raw_sample, count_validated, upsert_validated_batch
from src.quality.classifier import classify_record

st.set_page_config(page_title="Idempotency Demonstration", page_icon="🛡️", layout="wide")
st.title("🛡️ Idempotency & Upsert Verification Dashboard")

st.markdown("""
### Mandatory Requirement:
Rerunning the exact same dataset must be **Idempotent**:
1. Zero growth in duplicate business records (`Duplicate Difference = 0`).
2. Zero unintended mutation of existing valid states (`Final State Difference = 0`).
3. Updating an existing record modifies the document in-place without introducing a duplicate.

> **100% Real Production Data**: All test operations run on actual classified records from `orders_mixed_bad_good.jsonl` and `orders_raw`.
""")

db = get_mongo_db()

def get_real_dataset_sample(limit: int = 10) -> list:
    """Reads real classified records from the project dataset."""
    records = []
    dataset_path = root_dir / "data" / "orders_mixed_bad_good.jsonl"
    if dataset_path.exists():
        with open(dataset_path, "r", encoding="utf-8") as f:
            for line in f:
                if len(records) >= limit:
                    break
                line_str = line.strip()
                if not line_str:
                    continue
                try:
                    raw_doc = json.loads(line_str)
                    outcome, payload = classify_record(raw_doc)
                    if outcome in ["VALID", "CORRECTED"]:
                        records.append(payload)
                except Exception:
                    pass
    if not records:
        raw_samples = find_raw_sample(db, limit=limit)
        for doc in raw_samples:
            outcome, payload = classify_record(doc)
            if outcome in ["VALID", "CORRECTED"]:
                records.append(payload)
    return records


st.markdown("---")
st.markdown("### 🧪 Real Data Idempotency Verification Test")

real_sample_batch = get_real_dataset_sample(limit=10)

if not real_sample_batch:
    st.warning("No real data found in dataset file or orders_raw. Run pipeline via Upload & Run page first.")
else:
    c1, c2, c3 = st.columns(3)

    with c1:
        if st.button("1️⃣ Execute First Run (10 Real Orders)"):
            before_cnt = count_validated(db)
            ins, upd, unc = upsert_validated_batch(db, real_sample_batch)
            after_cnt = count_validated(db)

            st.success(f"First Run Executed on Real Orders:\n- Before Count: {before_cnt}\n- After Count: {after_cnt}\n- Inserted: {ins}\n- Unchanged: {unc}")
            st.json(real_sample_batch[:2])

    with c2:
        if st.button("2️⃣ Re-Run Identical Real Dataset"):
            before_cnt = count_validated(db)
            ins, upd, unc = upsert_validated_batch(db, real_sample_batch)
            after_cnt = count_validated(db)

            dup_diff = after_cnt - before_cnt
            st.info(f"Second Run Executed on Real Orders:\n- Before Count: {before_cnt}\n- After Count: {after_cnt}\n- Inserted: {ins}\n- Duplicate Diff: {dup_diff}")
            if dup_diff == 0:
                st.success("✅ **Idempotency Verified on Real Data! Duplicate Difference = 0**")

    with c3:
        if st.button("3️⃣ Modify Status & Update Real Order"):
            modified_batch = [dict(r) for r in real_sample_batch[:1]]
            modified_batch[0]["status"] = "تم التسليم"

            before_cnt = count_validated(db)
            ins, upd, unc = upsert_validated_batch(db, modified_batch)
            after_cnt = count_validated(db)

            st.warning(f"Update Run Executed on Real Order:\n- Before Count: {before_cnt}\n- After Count: {after_cnt}\n- Updated: {upd}")
            if after_cnt == before_cnt and upd >= 1:
                st.success("✅ **In-Place Update Verified on Real Data! No duplicate created.**")
            st.json(modified_batch)
