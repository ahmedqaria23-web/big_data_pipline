import sys
from pathlib import Path

root_dir = Path(__file__).resolve().parents[2]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

import streamlit as st
from app.components.pipeline_visualization import render_pipeline_flowchart

st.set_page_config(page_title="Architecture", page_icon="🏗️", layout="wide")
st.title("🏗️ System Architecture & MongoDB Collections")

render_pipeline_flowchart()

st.markdown("---")
st.markdown("### 🗄️ MongoDB Collections Specification")

c1, c2, c3 = st.columns(3)

with c1:
    st.markdown("#### `orders_raw`")
    st.write("- **Pattern:** Append-only Historical Raw Ingestion")
    st.write("- **Validator:** None (Zero load drop policy)")
    st.write("- **Key Fields:** `id_run`, `file_source`, `number_row_source`, `at_ingested`, `engine_used`, `record_raw`")

with c2:
    st.markdown("#### `orders_validated`")
    st.write("- **Pattern:** Business-ready Target Store")
    st.write("- **Validator:** MongoDB JSON Schema ($jsonSchema)")
    st.write("- **Key Index:** Unique Index on `id_order`")
    st.write("- **Write Strategy:** Idempotent Upsert (`ReplaceOne` / `$set`)")

with c3:
    st.markdown("#### `quarantine_orders`")
    st.write("- **Pattern:** Ununcorrectable Isolation Store")
    st.write("- **Validator:** None")
    st.write("- **Key Fields:** `id_run`, `id_order`, `codes_error`, `details_error`, `quarantined_at`, `record_raw`")

st.markdown("---")
st.markdown("### 📜 System Documentation Links")
st.markdown("- [System Architecture Documentation](docs/architecture.md)")
st.markdown("- [Requirements Traceability Matrix](docs/requirements_traceability.md)")
st.markdown("- [Notebook to Requirement Mapping](docs/notebook_requirements_mapping.md)")
st.markdown("- [Data Quality & Quarantine Rules](docs/data_quality_rules.md)")
st.markdown("- [Idempotency & Upsert Strategy](docs/idempotency.md)")
st.markdown("- [Path B Incremental Delta Processing](docs/incremental_loading.md)")
