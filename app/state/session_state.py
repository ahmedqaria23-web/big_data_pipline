import streamlit as st
from typing import Dict, Any


def init_session_state():
    """Initializes global Streamlit session state variables."""
    if "current_run" not in st.session_state:
        st.session_state.current_run = None

    if "uploaded_file_path" not in st.session_state:
        st.session_state.uploaded_file_path = None

    if "pipeline_history" not in st.session_state:
        st.session_state.pipeline_history = []

    if "selected_engine" not in st.session_state:
        st.session_state.selected_engine = None

    if "last_metrics" not in st.session_state:
        st.session_state.last_metrics = None
