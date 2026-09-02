import streamlit as st
import sys
from pathlib import Path

root_dir = Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

import pandas as pd
from src.mongodb.mongo_setup import get_mongo_db
from src.mongodb.repositories import count_raw, find_raw_sample

st.set_page_config(page_title="Raw Data Explorer", page_icon="📦", layout="wide")
st.title("📦 Raw Data Explorer (`orders_raw`)")

db = get_mongo_db()

raw_cnt = count_raw(db)
st.metric("Total Raw Documents", f"{raw_cnt:,}")

st.markdown("---")
st.markdown("### Sample Records (First 50)")

samples = find_raw_sample(db, limit=50)
if samples:
    df = pd.DataFrame(samples)
    st.dataframe(df, use_container_width=True)

    st.markdown("### 🔍 Record Inspector")
    idx = st.number_input("Select row index to inspect raw payload", min_value=0, max_value=len(samples)-1, value=0)
    st.json(samples[idx])
else:
    st.info("No raw documents found. Run pipeline first.")
