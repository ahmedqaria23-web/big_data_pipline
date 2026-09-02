import sys
import json
from pathlib import Path

root_dir = Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

import streamlit as st
from datetime import datetime, timezone
from src.mongodb.mongo_setup import get_mongo_db
from src.mongodb.repositories import find_raw_sample, count_validated, upsert_validated_batch
from src.quality.classifier import classify_record
from src.incremental.incremental_loader import get_watermark, process_delta_batch

st.set_page_config(page_title="Incremental Loading", page_icon="🔄", layout="wide")
st.title("🔄 Path B — Incremental & Watermark Delta Processing")

st.markdown("""
This module operates on **100% real dataset records** from `orders_mixed_bad_good.jsonl` and `orders_raw`. Zero dummy/fake data.
""")

db = get_mongo_db()

current_wm = get_watermark(db)
st.metric("Current Pipeline Watermark", current_wm)

st.markdown("---")
st.markdown("### 🧪 Path B Real Data Demonstration")


def get_real_classified_batch(offset: int = 0, limit: int = 5) -> list:
    """Reads real raw e-commerce order records from project dataset and classifies them."""
    records = []
    dataset_path = root_dir / "data" / "orders_mixed_bad_good.jsonl"
    
    if dataset_path.exists():
        with open(dataset_path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i < offset:
                    continue
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
    
    # Fallback to MongoDB orders_raw if dataset file is moved
    if not records:
        raw_samples = find_raw_sample(db, limit=limit)
        for doc in raw_samples:
            outcome, payload = classify_record(doc)
            if outcome in ["VALID", "CORRECTED"]:
                records.append(payload)
                
    return records


c1, c2, c3 = st.columns(3)

with c1:
    if st.button("1️⃣ Run Baseline Load (Real Dataset Records 1-5)"):
        real_batch1 = get_real_classified_batch(offset=0, limit=5)
        if real_batch1:
            res = process_delta_batch(db, real_batch1)
            st.success(f"Baseline Load Executed! Inserted: {res['inserted']}, Watermark: {res['new_watermark']}")
            st.markdown("#### 📋 Real Ingested Baseline Orders:")
            st.json(real_batch1)
        else:
            st.warning("No real data found in dataset file or orders_raw. Run pipeline via Upload & Run page first.")

with c2:
    if st.button("2️⃣ Run Delta Load (Real Dataset Records 6-10)"):
        real_batch2 = get_real_classified_batch(offset=5, limit=5)
        if real_batch2:
            res = process_delta_batch(db, real_batch2)
            st.success(f"Delta Load Executed! Inserted: {res['inserted']}, Updated: {res['updated']}")
            st.markdown("#### 📋 Real Ingested Delta Orders:")
            st.json(real_batch2)
        else:
            st.warning("No real data found in dataset file or orders_raw.")

with c3:
    if st.button("3️⃣ Re-Run Same Delta (Idempotency Test on Real Data)"):
        real_batch2 = get_real_classified_batch(offset=5, limit=5)
        if real_batch2:
            res = process_delta_batch(db, real_batch2)
            st.info(f"Re-Run Completed! Inserted: {res['inserted']}, Updated: {res['updated']}, Unchanged: {res['unchanged']}")
            if res['inserted'] == 0:
                st.success("✅ **Idempotency Verified on Real Data**: Re-running same real delta created 0 duplicate records.")
            st.json(res)
        else:
            st.warning("No real data found in dataset file or orders_raw.")
