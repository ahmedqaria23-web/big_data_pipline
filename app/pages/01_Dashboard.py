import sys
from pathlib import Path

root_dir = Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

import streamlit as st
import pandas as pd
import plotly.express as px

from src.pipeline.pipeline_controller import check_system_status, get_latest_metrics
from app.components.metric_cards import render_metric_card

st.set_page_config(page_title="Dashboard", page_icon="📈", layout="wide")
st.title("📈 Executive Pipeline Dashboard")

status = check_system_status()
metrics = get_latest_metrics()

c1, c2, c3, c4 = st.columns(4)

raw_cnt = status.get("collections", {}).get("orders_raw", 0)
val_cnt = status.get("collections", {}).get("orders_validated", 0)
quar_cnt = status.get("collections", {}).get("quarantine_orders", 0)
engine = metrics.get("used_engine", "N/A")

with c1:
    render_metric_card("Total Raw Ingested", f"{raw_cnt:,}", "Raw collection count", "#457b9d")
with c2:
    render_metric_card("Validated Orders", f"{val_cnt:,}", "Safe business records", "#2a9d8f")
with c3:
    render_metric_card("Quarantine Orders", f"{quar_cnt:,}", "Uncorrectable errors", "#e63946")
with c4:
    render_metric_card("Selected Engine", engine.upper(), f"Last Run: {metrics.get('id_run', 'None')}", "#f4a261")

st.markdown("---")

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📊 Record Classification Breakdown")
    if raw_cnt > 0:
        df_pie = pd.DataFrame([
            {"Outcome": "Valid", "Count": metrics.get("count_valid", val_cnt)},
            {"Outcome": "Corrected", "Count": metrics.get("count_corrected", 0)},
            {"Outcome": "Quarantine", "Count": metrics.get("count_quarantine", quar_cnt)}
        ])
        fig = px.pie(df_pie, values="Count", names="Outcome", color="Outcome",
                     color_discrete_map={"Valid": "#2a9d8f", "Corrected": "#e9c46a", "Quarantine": "#e63946"},
                     hole=0.4)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data ingested yet. Run pipeline via Upload & Run page.")

with col_right:
    st.subheader("⚡ Performance & Consistency Equation")
    if metrics:
        st.markdown(f"**Execution Time:** `{metrics.get('seconds_elapsed', 0)} sec`")
        st.markdown(f"**Throughput:** `{metrics.get('throughput_records_per_sec', 0):,} records/sec`")
        st.markdown(f"**Upsert Statistics:**")
        st.markdown(f"- Inserted: `{metrics.get('count_inserted', 0):,}`")
        st.markdown(f"- Updated: `{metrics.get('count_updated', 0):,}`")
        st.markdown(f"- Unchanged: `{metrics.get('count_unchanged', 0):,}`")
        
        verified = metrics.get("consistency_equation_verified", False)
        if verified:
            st.success("✅ **Consistency Equation Verified**: `Raw = Valid + Corrected + Quarantine`")
        else:
            st.warning("⚠️ Run consistency pending verification.")
    else:
        st.info("No run metrics recorded yet.")
