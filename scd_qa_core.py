# scd_qa_core.py
# Core logic for SCD Type 1 & Type 2 test case generator using Databricks LLM + Streamlit

import streamlit as st
from databricks.sdk import WorkspaceClient

# =====================================================================
# CONFIG – UPDATE ONLY THE ENDPOINT NAME FROM DATABRICKS MODEL SERVING
# =====================================================================

# Example: If your endpoint is named "deepseek-v3", put only the name.
MODEL_ENDPOINT_NAME = "deepseek-scd-qa"


# =====================================================================
# DATABRICKS MODEL SERVING CALL (uses Databricks Token + Workspace URL)
# =====================================================================

def call_databricks_llm(prompt: str) -> str:
    """
    Calls a Databricks Model Serving endpoint using internal workspace token.
    No OpenAI, no external URLs.
    """

    # WorkspaceClient auto-detects:
    # - DATABRICKS_HOST
    # - DATABRICKS_TOKEN
    w = WorkspaceClient()

    response = w.serving_endpoints.query(
        name=MODEL_ENDPOINT_NAME,
        inputs=[{"role": "user", "content": prompt}]
    )

    # Standard for chat models
    try:
        return response.output_text
    except:
        return str(response)


# =====================================================================
# PROMPT BUILDERS
# =====================================================================

def build_scd_type1_prompt(source_table, target_table, business_keys, attribute_columns, additional_rules):
    return f"""
You are a senior Data QA Engineer.

Generate detailed QA test cases to validate a Slowly Changing Dimension Type 1 (SCD1)
load between the following tables.

Source table: {source_table}
Target table: {target_table}
Business key columns: {business_keys}
Attribute columns (overwritten on change): {attribute_columns}

Additional rules:
{additional_rules}

Requirements:
- Assume SCD1 implemented in PySpark + Delta on Databricks.
- Generate 10–15 test cases.
- Include positive + negative scenarios.
- Include:
  • Initial load
  • Incremental load
  • Updates
  • No-change rows
  • Duplicate keys
  • Nulls
  • Data type issues

Output format (markdown table):

| Test Case ID | Scenario | Preconditions | Input Data Setup | Steps | Expected Result |
"""


def build_scd_type2_prompt(
    source_table,
    target_table,
    business_keys,
    attribute_columns,
    eff_from_col,
    eff_to_col,
    current_flag_col,
    version_col,
    additional_rules
):
    return f"""
You are a senior Data QA Engineer.

Generate detailed QA test cases to validate an SCD Type 2 load.

Source: {source_table}
Target: {target_table}
Business Keys: {business_keys}
Tracked Columns: {attribute_columns}

SCD2 Metadata:
- Effective From: {eff_from_col}
- Effective To: {eff_to_col}
- Current Flag: {current_flag_col}
- Version Column: {version_col}

Additional Rules:
{additional_rules}

Requirements:
- Assume PySpark + Delta Lake on Databricks
- Generate 12–18 cases
- Cover:
  • Initial load
  • First insert
  • Attribute change -> close old + open new
  • Multiple historical changes
  • Late-arriving data
  • Overlapping ranges
  • Current flag issues
  • Duplicate keys
  • Invalid effective dates

Output format (markdown table):

| Test Case ID | Scenario | Preconditions | Input Data Setup | Steps | Expected Result |
"""


# =====================================================================
# STREAMLIT UI
# =====================================================================

def run_app():
    st.title("🧪 SCD Test Case Generator (Databricks LLM)")
    st.caption("Uses Databricks Model Serving + Workspace Token (no OpenAI).")

    with st.sidebar:
        st.header("⚙️ Settings")
        scd_type = st.radio("Select SCD Type", ["SCD Type 1", "SCD Type 2"])
        st.info(f"Using Databricks Serving Endpoint: `{MODEL_ENDPOINT_NAME}`")

    st.subheader("🔢 Table & Column Metadata")

    col1, col2 = st.columns(2)
    with col1:
        source_table = st.text_input("Source Table", "src_customer_dim")
    with col2:
        target_table = st.text_input("Target Table", "dim_customer")

    business_keys = st.text_input("Business Keys (comma separated)", "customer_id")
    attribute_columns = st.text_input("Tracked / Overwritten Columns", "name,status,type")

    additional_rules = st.text_area("Additional Rules (optional)", height=100)

    eff_from_col = eff_to_col = current_flag_col = version_col = ""

    if scd_type == "SCD Type 2":
        st.subheader("⏳ SCD2 Metadata Columns")
        c1, c2 = st.columns(2)
        with c1:
            eff_from_col = st.text_input("Effective From", "eff_from_dt")
            current_flag_col = st.text_input("Current Flag", "is_current")
        with c2:
            eff_to_col = st.text_input("Effective To", "eff_to_dt")
            version_col = st.text_input("Version Column", "version_num")

    st.markdown("---")
    generate = st.button("⚡ Generate Test Cases")

    if generate:
        with st.spinner("Calling Databricks LLM…"):

            if scd_type == "SCD Type 1":
                prompt = build_scd_type1_prompt(
                    source_table, target_table, business_keys,
                    attribute_columns, additional_rules
                )
            else:
                prompt = build_scd_type2_prompt(
                    source_table, target_table, business_keys,
                    attribute_columns, eff_from_col, eff_to_col,
                    current_flag_col, version_col, additional_rules
                )

            try:
                result = call_databricks_llm(prompt)
                st.success("Generated!")
                st.code(result, language="markdown")

            except Exception as e:
                st.error(f"Error: {e}")

    st.markdown(
        "<hr><p style='text-align:center;color:gray'>Built on Databricks LLM</p>",
        unsafe_allow_html=True
    )
