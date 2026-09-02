import sys
from pathlib import Path

root_dir = Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

import streamlit as st
import pandas as pd
from src.mongodb.mongo_setup import get_mongo_db
from src.mongodb.repositories import count_quarantine, find_quarantine_sample
from src.quality.quarantine_manager import get_quarantine_summary, revalidate_quarantine_records

st.set_page_config(page_title="Quarantine Management", page_icon="🛡️", layout="wide")
st.title("🛡️ Quarantine Data Explorer (`quarantine_orders`)")

st.markdown("""
> Records that fail validation and cannot be safely corrected are preserved here with full raw evidence and explicit error codes. **Zero data loss.**
""")

db = get_mongo_db()

q_count = count_quarantine(db)
st.metric("Total Quarantined Documents", f"{q_count:,}")

st.markdown("---")
st.markdown("### 1. Error Code Distribution Summary")

summary = get_quarantine_summary(db)
if summary:
    df_sum = pd.DataFrame(summary)
    st.dataframe(df_sum, use_container_width=True)
else:
    st.info("No quarantine documents found.")

st.markdown("---")
st.markdown("### 2. Search & Filter Quarantine Records")

c1, c2 = st.columns(2)
with c1:
    filter_code = st.text_input("Filter by Error Code (e.g. ID_ORDER_MISSING)")
with c2:
    search_term = st.text_input("Search by id_order or customer detail")

query = {}
if filter_code.strip():
    query["codes_error"] = filter_code.strip()
if search_term.strip():
    query["$or"] = [
        {"id_order": {"$regex": search_term, "$options": "i"}},
        {"record_raw.customer.name": {"$regex": search_term, "$options": "i"}}
    ]

samples = find_quarantine_sample(db, limit=100, filter_query=query)

if samples:
    df_samples = pd.DataFrame(samples)
    st.dataframe(df_samples, use_container_width=True)

    csv_data = df_samples.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Export Filtered Quarantine Results as CSV",
        data=csv_data,
        file_name="quarantine_export.csv",
        mime="text/csv"
    )

    st.markdown("### 🔍 Quarantine Record Inspector")
    idx = st.number_input("Select row index to inspect payload", min_value=0, max_value=len(samples)-1, value=0)
    st.json(samples[idx])
else:
    st.info("No matching quarantine documents.")

st.markdown("---")
st.markdown("### 3. Re-validate & Reprocess Quarantine Items")
if st.button("🔄 Run Quarantine Re-Validation Batch"):
    res = revalidate_quarantine_records(db)
    st.success(f"Quarantine Re-validation Complete: Recovered `{res['recovered_count']}` records into orders_validated.")
    st.json(res)
