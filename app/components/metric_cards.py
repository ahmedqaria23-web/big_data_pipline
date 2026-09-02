import streamlit as st


def render_metric_card(title: str, value: str, subtitle: str = "", border_color: str = "#4361ee"):
    """Renders a styled metric card with custom border and typography."""
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(135deg, rgba(255,255,255,0.05), rgba(255,255,255,0.01));
            border-left: 4px solid {border_color};
            border-radius: 8px;
            padding: 16px 20px;
            margin-bottom: 12px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.05);
        ">
            <div style="font-size: 0.85rem; color: #8d99ae; font-weight: 600; text-transform: uppercase;">{title}</div>
            <div style="font-size: 1.8rem; font-weight: 700; margin: 4px 0;">{value}</div>
            <div style="font-size: 0.8rem; color: #b8c0c2;">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


def render_status_badge(status_text: str, is_success: bool = True):
    bg_color = "#2a9d8f" if is_success else "#e63946"
    st.markdown(
        f"""
        <span style="
            background-color: {bg_color};
            color: white;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.85rem;
            font-weight: 600;
        ">
            {status_text}
        </span>
        """,
        unsafe_allow_html=True
    )
