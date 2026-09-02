import streamlit as st


def render_pipeline_flowchart():
    """Renders an interactive visual flowchart of the logical pipeline architecture."""
    mermaid_code = """
    graph TD
        A[Dirty CSV / JSONL File] --> B[File Discovery]
        B --> C[Generate id_run]
        C --> D{File Router: Size <= 200MB?}
        D -- Yes --> E[Python Streaming Batch]
        D -- No --> F[PySpark Distributed Engine]
        E --> G[(MongoDB orders_raw)]
        F --> G
        G --> H[8+ Data Quality Rules & Business Validation]
        H --> I{Classifier}
        I -- Safe Correction / Valid --> J[(orders_validated)]
        I -- Uncorrectable --> K[(quarantine_orders)]
        J --> L[Unique Index on id_order + Idempotent Upsert]
        L --> M[Metrics & Consistency Equation Verification]
        M --> N[reports/results.json]
        N --> O[Streamlit Dashboard]

        style A fill:#457b9d,color:#fff,stroke:#333
        style G fill:#e76f51,color:#fff,stroke:#333
        style J fill:#2a9d8f,color:#fff,stroke:#333
        style K fill:#e63946,color:#fff,stroke:#333
        style O fill:#f4a261,color:#fff,stroke:#333
    """
    st.markdown("### 🔄 Mandatory Logical Pipeline Flowchart")
    st.components.v1.html(
        f"""
        <script type="module">
            import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
            mermaid.initialize({{ startOnLoad: true, theme: 'dark' }});
        </script>
        <div class="mermaid">
            {mermaid_code}
        </div>
        """,
        height=450,
        scrolling=True
    )
