import sys
from pathlib import Path

root_dir = Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

import streamlit as st
from src.pipeline.pipeline_controller import get_latest_metrics

st.set_page_config(page_title="Spark Monitor", page_icon="⚡", layout="wide")
st.title("⚡ PySpark Cluster & Execution Telemetry")

metrics = get_latest_metrics()

if metrics and metrics.get("used_engine") == "pyspark":
    st.success("PySpark was used in the latest pipeline run.")
    st.markdown(f"**Run ID:** `{metrics.get('id_run')}`")
    st.markdown(f"**File:** `{metrics.get('file_name')}`")
    st.markdown(f"**File Size:** `{metrics.get('file_size_mb')} MB`")
    st.markdown(f"**Records Loaded:** `{metrics.get('loaded_raw', 0):,}`")
    st.markdown(f"**Execution Time:** `{metrics.get('seconds_elapsed', 0)} sec`")
    st.markdown(f"**Throughput:** `{metrics.get('throughput_records_per_sec', 0):,} records/sec`")
elif metrics:
    st.info(f"Latest run used **{metrics.get('used_engine', 'N/A').upper()}** engine. PySpark telemetry shown when file > threshold.")
    st.json(metrics)
else:
    st.info("No execution metrics recorded yet.")
