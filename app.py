from __future__ import annotations

import io
import logging
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from etl import (
    cooccurrence_matrix,
    cross_field_cooccurrence,
    data_quality_report,
    explode_field,
    load_json,
    normalise,
)
from orchestrator import run_pipeline

try:
    from db import get_all_project_details, get_pipeline_status, validate_db
    from orchestrator import DB_PATH, STAGE1_MAX_AGE_HOURS
except ImportError:
    from .db import get_all_project_details, get_pipeline_status, validate_db
    from .orchestrator import DB_PATH, STAGE1_MAX_AGE_HOURS


DEFAULT_DATA_PATH = Path("data/raw/epah_details_atlas_projects_20260321T201622Z.json")


st.set_page_config(
    page_title="Energy Poverty Atlas Dashboard",
    page_icon="⚡",
    layout="wide",
)


@st.cache_data(show_spinner="Loading database records...")
def load_db_dataset(db_path: str) -> pd.DataFrame:
    rows = get_all_project_details(Path(db_path))
    if not rows:
        return pd.DataFrame()
    return normalise(rows)


@st.cache_data(show_spinner="Running ETL pipeline...")
def run_etl(file_bytes: bytes) -> pd.DataFrame:
    import io

    return normalise(load_json(io.BytesIO(file_bytes)))


@st.cache_data(show_spinner=False)
def load_pipeline_status(db_path: str, stage1_hours: int) -> dict:
    return get_pipeline_status(Path(db_path), stage1_max_age_hours=stage1_hours)


def run_startup_pipeline_once() -> str:
    if st.session_state.get("_startup_pipeline_ran", False):
        return st.session_state.get("_startup_pipeline_logs", "")

    log_buffer = io.StringIO()
    log_handler = logging.StreamHandler(log_buffer)
    log_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s \n")
    )

    startup_loggers = [
        logging.getLogger("atlas.orchestrator"),
        logging.getLogger("atlas.parser"),
    ]

    for logger in startup_loggers:
        logger.addHandler(log_handler)

    try:
        run_pipeline()
    finally:
        for logger in startup_loggers:
            logger.removeHandler(log_handler)
        log_handler.close()

    logs = log_buffer.getvalue().strip()
    st.session_state["_startup_pipeline_ran"] = True
    st.session_state["_startup_pipeline_logs"] = logs
    return logs


def format_timestamp(value: str | None) -> str:
    if not value:
        return "Not available"
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return str(value)
    return parsed.strftime("%Y-%m-%d")


def format_next_check(value: str | None) -> str:
    if not value:
        return "Not scheduled"
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return str(value)
    now = pd.Timestamp.now(tz="UTC")
    if parsed <= now:
        return f"Due now ({parsed.strftime('%Y-%m-%d')})" #'%Y-%m-%d'
    return parsed.strftime("%Y-%m-%d")


def get_latest_local_snapshot() -> Path | None:
    raw_dir = DEFAULT_DATA_PATH.parent
    if not raw_dir.exists():
        return DEFAULT_DATA_PATH if DEFAULT_DATA_PATH.exists() else None

    matches = sorted(raw_dir.glob("epah_details_atlas_projects_*.json"))
    matches.extend(sorted(raw_dir.glob("epah_details_atlas_projects_*.jsonl")))
    if matches:
        return max(matches, key=lambda path: path.stat().st_mtime)

    return DEFAULT_DATA_PATH if DEFAULT_DATA_PATH.exists() else None


def get_source_dataframe(uploaded_file) -> tuple[pd.DataFrame, str]:
    if uploaded_file is not None:
        return run_etl(uploaded_file.read()), "Uploaded JSON override"

    if validate_db(DB_PATH):
        db_df = load_db_dataset(str(DB_PATH))
        if not db_df.empty:
            return db_df, f"Database: `{DB_PATH}`"

    local_snapshot = get_latest_local_snapshot()
    if local_snapshot is not None:
        with local_snapshot.open("rb") as file_handle:
            return run_etl(file_handle.read()), f"Local file fallback: `{local_snapshot}`"

    return pd.DataFrame(), ""


st.title("⚡ Energy Poverty Atlas Dashboard")
# add subtitle with smaller font and lighter color
st.caption("This is a local ethical scraper, persistence layer  (with a database backend and optional JSON upload override), and Streamlit dashboard for monitoring and descriptive analytics of projects from the European Energy Poverty Advisory Hub (EPAH) Atlas. **Used only in research and educational purposes, not affiliated with or endorsed by the EPAH or the European Commission.**")
startup_pipeline_logs = run_startup_pipeline_once()
if startup_pipeline_logs:
    with st.status(label = "Parser pipeline startup information", state ="complete"):#, icon="🔔", duration=7)
        st.text(startup_pipeline_logs)

with st.sidebar:
    st.header("Data Source")
    uploaded = st.file_uploader("Upload JSON file (optional)", type=["json", "jsonl"])

df_full, source_label = get_source_dataframe(uploaded)

if df_full.empty:
    st.error("No project data is available. Populate `project_details` or upload a JSON file.")
    st.stop()

pipeline_status = (
    load_pipeline_status(str(DB_PATH), STAGE1_MAX_AGE_HOURS)
    if validate_db(DB_PATH)
    else {
        "last_stage1_run": None,
        "last_stage2_run": None,
        "next_stage1_due": None,
        "projects_added_since_last_run": 0,
        "new_projects": [],
    }
)

last_stage1 = format_timestamp(pipeline_status.get("last_stage1_run"))
next_stage1 = format_next_check(pipeline_status.get("next_stage1_due"))
new_projects_since_last_update = pipeline_status.get("projects_added_since_last_run", 0)
recent_projects_df = pd.DataFrame(pipeline_status.get("new_projects", []))

## st.info(f"Current source: {source_label or 'Unknown'}")

status_col1, status_col2, status_col3 = st.columns(3)
status_col1.metric("Projects Loaded", len(df_full))
status_col2.metric("Last Stage 1 Update", last_stage1)
status_col3.metric("Next Stage 1 Check", next_stage1)

with st.sidebar:
    if source_label:
        st.caption(f"Using {source_label}")
    st.markdown("---")
    st.header("Filters")

# Populate sidebar filters dynamically
all_scales = sorted(df_full["geographical_scale"].dropna().unique())
all_phases = sorted({v for lst in df_full["energy_poverty_phase_list"] for v in lst})
all_interventions = sorted({v for lst in df_full["intervention_type_list"] for v in lst})
all_countries = sorted({v for lst in df_full["countries_impacted_list"] for v in lst})

with st.sidebar:
    filter_scale = st.multiselect("Geographical Scale", all_scales, key="scale")
    filter_phase = st.multiselect("Energy Poverty Phase", all_phases, key="phase")
    filter_intervention = st.multiselect("Intervention Type", all_interventions, key="interv")
    filter_country = st.multiselect("Country", all_countries, key="country")

# Apply filters
df = df_full.copy()
if filter_scale:
    df = df[df["geographical_scale"].isin(filter_scale)]
if filter_phase:
    df = df[df["energy_poverty_phase_list"].apply(lambda x: any(p in x for p in filter_phase))]
if filter_intervention:
    df = df[df["intervention_type_list"].apply(lambda x: any(i in x for i in filter_intervention))]
if filter_country:
    df = df[df["countries_impacted_list"].apply(lambda x: any(c in x for c in filter_country))]

st.sidebar.markdown(f"**{len(df)} / {len(df_full)} projects** shown")

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    [
        "📊 Overview",
        "📈 Descriptive Stats",
        "🔥 Overlap Heatmap",
        "🗂️ Project Breakdown",
        "🧹 Data Quality",
    ]
)

with tab1:
    st.subheader("Dataset Overview")
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Projects", len(df))
    c2.metric("Countries", len({v for lst in df["countries_impacted_list"] for v in lst}))
    c3.metric("Intervention Types", len({v for lst in df["intervention_type_list"] for v in lst}))
    c4.metric("Funding Types", df["type_of_funding"].nunique())
    c5.metric("Geo Scales", df["geographical_scale"].nunique())
    c6.metric("New Since Last Update", new_projects_since_last_update)

    st.caption("Recent projects can also be seen in Project Breakdown by sorting the table by `parsed_at`.")

    if recent_projects_df.empty:
        st.info("No new projects were added in the latest Stage 1 update.")
    else:
        #st.markdown("#### New Projects In Latest Update")
        if st.button("Show New Projects In Latest Update", key="show_recent"):
            recent_projects_df = recent_projects_df.rename(
                columns={
                    "project_title": "Title",
                    "project_url": "Link",
                }
            )
            if "last_stage1_seen_at" not in recent_projects_df.columns:
                recent_projects_df["last_stage1_seen_at"] = None
            recent_projects_df = recent_projects_df.reindex(
                columns=["Title", "Link", "last_stage1_seen_at"],
                fill_value=None,
            )
            st.dataframe(
                recent_projects_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Link": st.column_config.LinkColumn("Link", display_text="Open project"),
                    "last_stage1_seen_at": st.column_config.DatetimeColumn(
                        "last_stage1_seen_at",
                        format="YYYY-MM-DD HH:mm",
                    ),
                },
            )


    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        scale_counts = df["geographical_scale"].value_counts().reset_index()
        scale_counts.columns = ["Scale", "Projects"]
        fig = px.pie(
            scale_counts,
            names="Scale",
            values="Projects",
            title="Projects by Geographical Scale",
            hole=0.4,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        country_exp = explode_field(df, "countries_impacted")
        if not country_exp.empty:
            ctry_counts = country_exp["countries_impacted"].value_counts().reset_index()
            # substitute long country names with shorter versions for better display
            country_mapping = {
                "United States of America": "USA",
                "United Kingdom of Great Britain and Northern Ireland": "UK",
                "Russian Federation": "Russia",
                # Add more mappings as needed
            }
            ctry_counts["countries_impacted"] = ctry_counts["countries_impacted"].map(country_mapping).fillna(ctry_counts["countries_impacted"])

            ctry_counts.columns = ["Country", "Projects"]
            fig2 = px.bar(
                ctry_counts,
                x="Country",
                y="Projects",
                title="Projects per Country",
                color="Projects",
                color_continuous_scale="Blues",
            )
            st.plotly_chart(fig2, use_container_width=True)

    fund_counts = df["type_of_funding"].value_counts().reset_index()
    fund_counts.columns = ["Funding Type", "Projects"]
    #Graph cloud of words in the funding types, sized by count
    fig3 = px.treemap(
        fund_counts,
        path=["Funding Type"],
        values="Projects",
        title="Projects by Funding Type",
        color="Projects",
        color_continuous_scale="Greens",
    )
    st.plotly_chart(fig3, use_container_width=True)

with tab2:
    st.subheader("Descriptive Statistics")

    col1, col2 = st.columns(2)

    with col1:
        int_exp = explode_field(df, "intervention_type")
        if not int_exp.empty:
            int_counts = int_exp["intervention_type"].value_counts().reset_index()
            int_counts.columns = ["Intervention Type", "Count"]
            fig = px.bar(
                int_counts,
                x="Count",
                y="Intervention Type",
                orientation="h",
                title="Intervention Types Distribution",
                color="Count",
                color_continuous_scale="Oranges",
            )
            fig.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        phase_exp = explode_field(df, "energy_poverty_phase")
        if not phase_exp.empty:
            phase_counts = phase_exp["energy_poverty_phase"].value_counts().reset_index()
            phase_counts.columns = ["Phase", "Count"]
            fig2 = px.bar(
                phase_counts,
                x="Phase",
                y="Count",
                title="Energy Poverty Phases Distribution",
                color="Count",
                color_continuous_scale="Purples",
            )
            st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")
    col3, col4 = st.columns(2)

    with col3:
        fig3 = px.histogram(
            df,
            x="country_count",
            nbins=10,
            title="Distribution of Countries per Project",
            labels={"country_count": "Number of Countries"},
            color_discrete_sequence=["#2196F3"],
        )
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        fig4 = px.histogram(
            df,
            x="intervention_count",
            nbins=8,
            title="Distribution of Intervention Types per Project",
            labels={"intervention_count": "Number of Interventions"},
            color_discrete_sequence=["#FF9800"],
        )
        st.plotly_chart(fig4, use_container_width=True)

    prof_exp = explode_field(df, "professionals_involved")
    if not prof_exp.empty:
        prof_counts = prof_exp["professionals_involved"].value_counts().reset_index()
        prof_counts.columns = ["Professional Type", "Count"]
        fig5 = px.bar(
            prof_counts,
            x="Professional Type",
            y="Count",
            title="Professionals Involved Across Projects",
            color="Count",
            color_continuous_scale="Greens",
        )
        st.plotly_chart(fig5, use_container_width=True)

with tab3:
    st.subheader("Category Overlap & Co-occurrence")

    heatmap_mode = st.radio(
        "Select heatmap type",
        [
            "Intervention Types (self)",
            "Phases (self)",
            "Intervention × Phase",
            "Intervention × Country",
            "Phase × Country",
        ],
        horizontal=True,
    )

    def plot_heatmap(matrix: pd.DataFrame, title: str) -> None:
        if matrix.empty:
            st.warning("Not enough data to build this matrix.")
            return
        fig = px.imshow(
            matrix,
            text_auto=True,
            aspect="auto",
            color_continuous_scale="YlOrRd",
            title=title,
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

    if heatmap_mode == "Intervention Types (self)":
        matrix = cooccurrence_matrix(df, "intervention_type")
        plot_heatmap(matrix, "Co-occurrence of Intervention Types across Projects")

    elif heatmap_mode == "Phases (self)":
        matrix = cooccurrence_matrix(df, "energy_poverty_phase")
        plot_heatmap(matrix, "Co-occurrence of Energy Poverty Phases across Projects")

    elif heatmap_mode == "Intervention × Phase":
        matrix = cross_field_cooccurrence(df, "intervention_type", "energy_poverty_phase")
        plot_heatmap(matrix, "Intervention Types × Energy Poverty Phases")

    elif heatmap_mode == "Intervention × Country":
        matrix = cross_field_cooccurrence(df, "intervention_type", "countries_impacted")
        plot_heatmap(matrix, "Intervention Types × Countries")

    elif heatmap_mode == "Phase × Country":
        matrix = cross_field_cooccurrence(df, "energy_poverty_phase", "countries_impacted")
        plot_heatmap(matrix, "Energy Poverty Phases × Countries")

    st.markdown("---")
    st.markdown(
        "**ℹ️ How to read this:** Each cell shows how many projects share both row and column attributes. Higher values mean stronger co-occurrence."
    )

with tab4:
    st.subheader("Per-Project Attribute Breakdown")
    st.caption("Recent projects can also be found here by sorting the table by `parsed_at`.")

    display_cols = [
        "atlas_id",
        "project_title",
        "geographical_scale",
        'parsed_at',
        "project_scope",
        "project_url",
        "country_count",
        "intervention_count",
        "phase_count",
        "type_of_funding",
    ]
    available = [column for column in display_cols if column in df.columns]
    display_df = df[available].copy()
    column_config = None
    if "project_url" in display_df.columns and "parsed_at" in display_df.columns:
        column_config = {
            "project_url": st.column_config.LinkColumn(
                "Link",
                display_text="Open project",
            ),
        "parsed_at": st.column_config.DatetimeColumn(format="YYYY-MM-DD", help="When this project was parsed into the database"),
        }
    st.dataframe(
        display_df,
        use_container_width=True,
        height=300,
        column_config=column_config,
    )

    st.markdown("---")
    st.markdown("### Project Detail")
    titles = df["project_title"].dropna().tolist()
    selected = st.selectbox("Select a project to inspect", titles)

    if selected:
        matches = df["project_title"] == selected
        row = df[matches].iloc[0]
        col1, col2 = st.columns(2)
        with col1:
            project_url = row.get("project_url")
            if pd.notna(project_url):
                st.link_button("Open project", project_url)
            else:
                st.write("URL: -")
            st.markdown(f"**Scope:** {row.get('project_scope', '-')}")
            st.markdown(f"**Geographical Scale:** {row.get('geographical_scale', '-')}")
            st.markdown(f"**Funding:** {row.get('type_of_funding', '-')}")
        with col2:
            st.markdown(f"**Countries:** {', '.join(row['countries_impacted_list']) or '-'}")
            st.markdown(f"**Phases:** {', '.join(row['energy_poverty_phase_list']) or '-'}")
            st.markdown(f"**Interventions:** {', '.join(row['intervention_type_list']) or '-'}")
            st.markdown(f"**Professionals:** {', '.join(row['professionals_involved_list']) or '-'}")
        with st.expander("📄 Project Description"):
            st.write(row.get("project_body", "No description available."))

        with st.expander("🤝 Partners Involved"):
            partners = row.get("partners_involved_list", [])
            if partners:
                for partner in partners:
                    st.markdown(f"- {partner}")
            else:
                st.write("No partners listed.")

    st.markdown("---")

with tab5:
    st.subheader("🧹 Data Quality Report")
    dq = data_quality_report(df)
    # remove 'website' from the report as it's not a critical field and often legitimately missing
    dq = dq[dq["field"] != "website"]

    col1, col2 = st.columns([2, 1])
    with col1:
        fig = px.bar(
            dq,
            x="field",
            y="fill_rate_%",
            title="Field Fill Rate (%)",
            color="fill_rate_%",
            color_continuous_scale="RdYlGn",
            range_color=[0, 100],
        )
        fig.update_xaxes(tickangle=45)
        fig.add_hline(y=80, line_dash="dash", line_color="orange", annotation_text="80% threshold")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.dataframe(dq, use_container_width=True, height=400)

    st.markdown("---")
    st.markdown("### 📥 Export Cleaned Data")
    csv = df.drop(columns=[column for column in df.columns if column.endswith("_list")]).to_csv(index=False)
    st.download_button("⬇️ Download cleaned CSV", csv, "cleaned_projects.csv", "text/csv")
