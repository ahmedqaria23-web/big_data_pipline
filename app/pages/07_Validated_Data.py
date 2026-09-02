import sys
from pathlib import Path

root_dir = Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

import streamlit as st
import pandas as pd
from src.mongodb.mongo_setup import get_mongo_db
from src.mongodb.repositories import count_validated, find_validated_sample

st.set_page_config(page_title="Validated Data Explorer", page_icon="✅", layout="wide")
st.title("✅ Validated Data Explorer (`orders_validated`)")

db = get_mongo_db()

val_cnt = count_validated(db)
st.metric("Total Validated Documents", f"{val_cnt:,}")

st.markdown("---")
st.markdown("### Sample Validated Records (First 50)")

samples = find_validated_sample(db, limit=50)
if samples:
    df = pd.DataFrame(samples)
    st.dataframe(df, use_container_width=True)

    st.markdown("### 🔍 Record Inspector")
    idx = st.number_input("Select row index to inspect validated payload", min_value=0, max_value=len(samples)-1, value=0)
    st.json(samples[idx])
else:
    st.info("No validated documents found.")
