import streamlit as st
import sys
from pathlib import Path

root_dir = Path(__file__).parent.parent.resolve()
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from app.state.session_state import init_session_state
from src.pipeline.pipeline_controller import check_system_status

st.set_page_config(
    page_title="Data Engineering Pipeline Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

init_session_state()

st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        font-weight: 800;
        background: linear-gradient(90deg, #4361ee, #4cc9f0);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #8d99ae;
        margin-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-header">University Hybrid Data Engineering Pipeline</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Python Batch | PySpark | MongoDB ELT | Data Quality | Idempotency | Streamlit Dashboard</div>', unsafe_allow_html=True)

st.sidebar.image("https://img.icons8.com/color/96/data-configuration.png", width=64)
st.sidebar.markdown("### 🖥️ System Status")

status = check_system_status()

if status.get("mongodb_connected"):
    st.sidebar.success(f"Connected: {status['database_name']}")
    col_counts = status.get("collections", {})
    st.sidebar.markdown(f"📦 **Raw Orders:** `{col_counts.get('orders_raw', 0):,}`")
    st.sidebar.markdown(f"✅ **Validated Orders:** `{col_counts.get('orders_validated', 0):,}`")
    st.sidebar.markdown(f"⚠️ **Quarantine Orders:** `{col_counts.get('quarantine_orders', 0):,}`")
else:
    st.sidebar.error("MongoDB Disconnected! Start MongoDB service.")

st.sidebar.markdown("---")
st.sidebar.info("Use the navigation pages on the left to upload data, execute the pipeline, and monitor execution metrics.")

st.markdown("### Welcome to the Data Engineering Pipeline Control Center")
st.markdown("""
This application allows full control and evaluation of the university data engineering project.
You can upload dirty e-commerce order CSV/JSONL files, automatically route execution between Python Batch Loading and PySpark based on file size, enforce strict ELT processing, review data quality transformations with full audit trails, and inspect idempotent upserts into MongoDB.
""")

col1, col2, col3 = st.columns(3)
with col1:
    st.info("📂 **02 Upload & Run**\n\nUpload CSV datasets, validate router selection, and execute the full ELT pipeline.")
with col2:
    st.success("📊 **05 Data Quality**\n\nInspect the 8+ automatic cleaning rules, audit trail logs, and record classifications.")
with col3:
    st.warning("🔄 **10 Idempotency**\n\nDemonstrate zero duplicate creation on identical reruns and idempotent updates.")
