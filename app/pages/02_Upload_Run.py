import sys
import os
import streamlit as st
from pathlib import Path

root_dir = Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from src.routing.file_router import inspect_and_route
from src.pipeline.pipeline_controller import run_pipeline_for_file
from create_small_sample import generate_sample
from config.settings import DATA_DIR

st.set_page_config(page_title="Upload & Run Pipeline", page_icon="📂", layout="wide")

col_t, col_c = st.columns([3, 1])
with col_t:
    st.title("📂 Upload Dataset & Execute Pipeline")
with col_c:
    st.write("")
    if st.button("🧹 Clear Cache & RAM"):
        import gc
        st.cache_data.clear()
        st.cache_resource.clear()
        gc.collect()
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark:
                spark.stop()
        except Exception:
            pass
        st.success("✅ Cache & Memory Cleared!")

st.markdown("### 1. File Input & Sampling Options")

tab_local, tab_sample, tab_upload = st.tabs(["📁 Use Local File Path / Workspace", "🎲 Generate Sample", "📤 Upload Small File (<200MB)"])

if "target_file_path" not in st.session_state:
    st.session_state["target_file_path"] = None

with tab_local:
    st.markdown("#### ⚡ Option A: Enter Direct File Path on Disk (Recommended for 1GB+ files)")
    default_val = st.session_state.get("target_file_path") or ""
    custom_path = st.text_input("Enter full file path on disk (e.g. C:\\Users\\LEGOIN\\Downloads\\orders_huge_mixed_quality.csv):", value=default_val)
    if custom_path and os.path.exists(custom_path):
        st.session_state["target_file_path"] = custom_path
        st.success(f"Selected local file: `{custom_path}`")
    elif custom_path:
        st.error(f"File not found at path: `{custom_path}`")

    st.markdown("---")
    st.markdown("#### 📁 Option B: Select from Data / Workspace Folder")
    existing_files = list(Path(".").glob("*.jsonl")) + list(Path(".").glob("*.csv")) + list(DATA_DIR.glob("*"))
    existing_file_names = [str(f.resolve()) for f in existing_files]
    if existing_file_names:
        selected_file = st.selectbox("Select file from workspace directory", existing_file_names)
        if st.button("Set Selected Workspace File"):
            st.session_state["target_file_path"] = selected_file
            st.info(f"Set target file: `{selected_file}`")

with tab_sample:
    sample_rows = st.number_input("Number of rows for small sample", min_value=10, max_value=500000, value=10000, step=5000)
    if st.button("Generate Reproducible Sample"):
        input_source = "orders_mixed_bad_good.jsonl"
        try:
            out_p = generate_sample(input_source, sample_rows)
            st.session_state["target_file_path"] = out_p
            st.success(f"Generated sample file at `{out_p}`")
        except Exception as e:
            st.error(f"Error generating sample: {e}")

with tab_upload:
    st.warning("⚠️ **Note:** Browser upload buffers files directly in RAM. Do NOT upload files larger than 200MB here as Python will throw a `MemoryError`. Use the **'Use Local File Path'** tab for large files.")
    try:
        uploaded_file = st.file_uploader("Choose dirty e-commerce orders CSV or JSONL file (Small files only)", type=["csv", "jsonl"])
        if uploaded_file is not None:
            save_path = DATA_DIR / uploaded_file.name
            with open(save_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            st.session_state["target_file_path"] = str(save_path)
            st.success(f"File uploaded successfully to `{save_path}`")
    except MemoryError:
        st.error("MemoryError: File is too large to buffer in browser RAM. Please use Tab 1 to enter local file path directly!")

st.markdown("---")
st.markdown("### 2. Router Engine Decision")

target_file_path = st.session_state.get("target_file_path")

if target_file_path and os.path.exists(target_file_path):
    routing_info = inspect_and_route(target_file_path)
    
    st.markdown(f"**Target File:** `{routing_info['file_name']}`")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("File Size", f"{routing_info['file_size_mb']} MB")
    with c2:
        st.metric("Configured Threshold", f"{routing_info['threshold_mb']} MB")
    with c3:
        st.metric("Selected Engine", routing_info['selected_engine'].upper())

    st.info(f"**Router Decision Reason:** {routing_info['reason']}")

    st.markdown("---")
    st.markdown("### 3. Pipeline Execution")
    
    if st.button("🚀 Run Complete ELT Pipeline", type="primary"):
        progress_bar = st.progress(0.0)
        status_text = st.empty()

        def update_ui_progress(msg: str, pct: float):
            try:
                progress_bar.progress(pct)
                status_text.markdown(f"**Progress:** `{msg}`")
            except Exception:
                pass


        try:
            results = run_pipeline_for_file(target_file_path, progress_callback=update_ui_progress)
            st.balloons()
            st.success(f"✅ Pipeline executed successfully! Run ID: `{results['id_run']}`")
            st.json(results)
        except Exception as err:
            st.error(f"❌ Pipeline Execution Error: {err}")
else:
    st.warning("Please upload a dataset or select an existing file above to proceed.")
