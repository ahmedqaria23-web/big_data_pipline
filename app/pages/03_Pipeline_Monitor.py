import sys
from pathlib import Path

root_dir = Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

import streamlit as st
from app.components.pipeline_visualization import render_pipeline_flowchart
from src.pipeline.pipeline_controller import get_latest_metrics

st.set_page_config(page_title="Pipeline Monitor", page_icon="📡", layout="wide")
st.title("📡 Real-Time Pipeline Stage Monitor")

render_pipeline_flowchart()

st.markdown("---")
st.markdown("### 📊 Last Pipeline Execution Telemetry")

metrics = get_latest_metrics()

if metrics:
    st.subheader(f"Run ID: `{metrics.get('id_run')}`")
    
    stages = [
        ("1. File Discovery & Router", f"File: {metrics.get('file_name')} ({metrics.get('file_size_mb')} MB) -> Engine: {metrics.get('used_engine')}", "COMPLETED"),
        ("2. Raw Ingestion (ELT)", f"Ingested {metrics.get('loaded_raw', 0):,} records into orders_raw BEFORE cleaning", "COMPLETED"),
        ("3. Data Quality & Cleaning", "Executed 8+ cleaning rules & audit trail generation", "COMPLETED"),
        ("4. Classification", f"Valid: {metrics.get('count_valid', 0):,} | Corrected: {metrics.get('count_corrected', 0):,} | Quarantine: {metrics.get('count_quarantine', 0):,}", "COMPLETED"),
        ("5. Final Load & Upsert", f"Inserted: {metrics.get('count_inserted', 0):,} | Updated: {metrics.get('count_updated', 0):,} | Unchanged: {metrics.get('count_unchanged', 0):,}", "COMPLETED"),
        ("6. Consistency Verification", "run_raw_count = run_valid_count + run_corrected_count + run_quarantine_count", "VERIFIED" if metrics.get("consistency_equation_verified") else "PENDING")
    ]

    for name, detail, state in stages:
        with st.expander(f"🟢 Stage: {name} [{state}]", expanded=True):
            st.write(detail)
else:
    st.info("No execution telemetry recorded yet. Run a pipeline execution via Upload & Run page.")
