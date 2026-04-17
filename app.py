from __future__ import annotations

import io
import logging
import os
import tempfile
from collections import Counter
from itertools import combinations
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components

try:
    import networkx as nx
    from pyvis.network import Network
except ImportError:
    nx = None
    Network = None

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
    return parsed.strftime("%B %d, %Y") ##"%Y-%m-%d"


def format_next_check(value: str | None) -> str:
    if not value:
        return "Not scheduled"
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return str(value)
    now = pd.Timestamp.now(tz="UTC")
    if parsed <= now:
        return f"Due now ({parsed.strftime('%Y-%m-%d')})" #'%Y-%m-%d'
    return parsed.strftime("%B %d, %Y") ##"%Y-%m-%d"


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


def build_country_collaboration_graph(
    dataframe: pd.DataFrame,
    countries_column: str = "countries_impacted_list",
):
    if nx is None or countries_column not in dataframe.columns:
        return None, pd.DataFrame()

    country_counts: Counter[str] = Counter()
    pair_counts: Counter[tuple[str, str]] = Counter()

    for raw_countries in dataframe[countries_column].dropna():
        if isinstance(raw_countries, list):
            countries = sorted({str(country).strip() for country in raw_countries if str(country).strip()})
        else:
            countries = sorted(
                {
                    country.strip()
                    for country in str(raw_countries).split(";")
                    if country.strip()
                }
            )

        for country in countries:
            country_counts[country] += 1

        if len(countries) >= 2:
            pair_counts.update(combinations(countries, 2))

    graph = nx.Graph()

    for country, count in country_counts.items():
        graph.add_node(country, project_count=count)

    for (country_a, country_b), weight in pair_counts.items():
        graph.add_edge(country_a, country_b, weight=weight)

    edge_summary = pd.DataFrame(
        [
            {
                "country_1": country_a,
                "country_2": country_b,
                "shared_projects": weight,
            }
            for (country_a, country_b), weight in pair_counts.items()
        ]
    )

    if not edge_summary.empty:
        edge_summary = edge_summary.sort_values(
            by=["shared_projects", "country_1", "country_2"],
            ascending=[False, True, True],
        ).reset_index(drop=True)

    return graph, edge_summary


def filter_country_collaboration_graph(graph, selected_countries: list[str], min_weight: int):
    if nx is None or graph is None:
        return None

    filtered_graph = nx.Graph()
    selected_country_set = set(selected_countries)

    for node, attrs in graph.nodes(data=True):
        if node in selected_country_set:
            filtered_graph.add_node(node, **attrs)

    for country_a, country_b, attrs in graph.edges(data=True):
        if (
            country_a in selected_country_set
            and country_b in selected_country_set
            and attrs.get("weight", 0) >= min_weight
        ):
            filtered_graph.add_edge(country_a, country_b, **attrs)

    return filtered_graph


def freeze_pyvis_physics_after_stabilization(html: str) -> str:
    network_init = "network = new vis.Network(container, data, options);"
    freeze_script = """
              network.once("stabilizationIterationsDone", function () {
                  network.setOptions({ physics: false });
              });
    """

    if network_init in html and freeze_script not in html:
        return html.replace(network_init, f"{network_init}\n{freeze_script}", 1)

    return html


def render_country_collaboration_network(graph, height: int = 750) -> None:
    if Network is None or graph is None:
        return

    network = Network(
        height=f"{height}px",
        width="100%",
        bgcolor="white",
        font_color="black",
    )

    # Use a light force layout so nodes separate without becoming chaotic.
    network.set_options("""
    {
      "nodes": {
        "shape": "dot",
        "margin": 10,
        "scaling": {
          "min": 12,
          "max": 44,
          "label": {
            "enabled": true,
            "min": 14,
            "max": 24
          }
        },
        "font": {
          "size": 16,
          "face": "arial",
          "strokeWidth": 3,
          "strokeColor": "#ffffff"
        }
      },
      "edges": {
        "smooth": {
          "enabled": true,
          "type": "dynamic",
          "roundness": 0.18
        },
        "color": {
          "inherit": false
        },
        "width": 1.2
      },
      "interaction": {
        "hover": true,
        "tooltipDelay": 150,
        "selectConnectedEdges": true,
        "navigationButtons": false
      },
      "physics": {
        "enabled": true,
        "solver": "forceAtlas2Based",
        "forceAtlas2Based": {
          "gravitationalConstant": -85,
          "centralGravity": 0.015,
          "springLength": 180,
          "springConstant": 0.045,
          "damping": 0.72,
          "avoidOverlap": 1
        },
        "minVelocity": 0.75,
        "stabilization": {
          "enabled": true,
          "iterations": 250
        }
      }
    }
    """)

    # Read project counts to build a relative scale
    project_counts = [
        attrs.get("project_count", 0)
        for _, attrs in graph.nodes(data=True)
    ]
    max_project_count = max(project_counts) if project_counts else 1

    for node, attrs in graph.nodes(data=True):
        project_count = attrs.get("project_count", 0)

        # Bubble size scaling
        node_size = 16 + (project_count / max_project_count) * 26 if max_project_count > 0 else 16

        # Simple color scale by importance
        if project_count >= 0.75 * max_project_count:
            background = "#4f81bd"
            border = "#2f5d99"
        elif project_count >= 0.40 * max_project_count:
            background = "#7ea6d8"
            border = "#4f81bd"
        else:
            background = "#c6dbef"
            border = "#7ea6d8"

        network.add_node(
            node,
            label=node,
            title=f"{node}\nProjects involved: {project_count}",
            size=node_size,
            value=project_count,  # supports vis scaling
            font={
                "size": 20 if project_count >= 0.5 * max_project_count else 15,
                "face": "arial",
                "strokeWidth": 3,
                "strokeColor": "#ffffff"
            },
            color={
                "background": background,
                "border": border,
                "highlight": {
                    "background": "#ffcc00",
                    "border": "#cc9900"
                },
                "hover": {
                    "background": "#ffd966",
                    "border": "#d6a600"
                }
            }
        )

    # Edge scale
    edge_weights = [
        attrs.get("weight", 0)
        for _, _, attrs in graph.edges(data=True)
    ]
    max_weight = max(edge_weights) if edge_weights else 1

    for country_a, country_b, attrs in graph.edges(data=True):
        shared_projects = attrs.get("weight", 0)

        edge_width = 1 + (shared_projects / max_weight) * 4 if max_weight > 0 else 1

        network.add_edge(
            country_a,
            country_b,
            value=shared_projects,
            width=edge_width,
            title=f"{country_a} - {country_b}\nShared projects: {shared_projects}",
            color={
                "color": "#b0b0b0",
                "highlight": "#ff9900",
                "hover": "#ff9900"
            }
        )

    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
        temp_html_path = tmp_file.name

    try:
        network.save_graph(temp_html_path)
        with open(temp_html_path, "r", encoding="utf-8") as html_file:
            html = freeze_pyvis_physics_after_stabilization(html_file.read())
            components.html(html, height=height, scrolling=True)
    finally:
        if os.path.exists(temp_html_path):
            os.remove(temp_html_path)


st.title("⚡ Energy Poverty Atlas Dashboard")
# add subtitle with smaller font and lighter color
st.caption("This is a local ethical scraper, persistence layer (with a database backend and optional JSON upload override), and Streamlit dashboard for monitoring and descriptive analytics of projects from the **[European Energy Poverty Advisory Hub (EPAH) Atlas](https://energy-poverty.ec.europa.eu/discover-community/epah-atlas)**. **Used only in research and educational purposes, not affiliated with or endorsed by the EPAH or the European Commission.**")
##startup_pipeline_logs = run_startup_pipeline_once()
##if startup_pipeline_logs:
##    with st.status(label = "Parser pipeline startup information", state ="complete"):#, icon="🔔", duration=7)
##        st.text(startup_pipeline_logs)

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
# This shows the last Stage 1 run time and how many new projects were added since then, and when the next Stage 1 check is due, if available.
##status_col1, status_col2, status_col3 = st.columns(3)
##status_col1.metric("Projects Loaded", len(df_full), new_projects_since_last_update if new_projects_since_last_update > 0 else None)
##status_col2.metric("Last Scraping Update", last_stage1)
#status_col3.metric("Next Scraping (Stage 1) Check", next_stage1)
st.caption(f"Projects Loaded: **{len(df_full)}** | Last Scraping Update: **{last_stage1}** ")##| Next Scraping Check: **{next_stage1}**")

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

tab1, tab2, tab3, tab5, tab6 = st.tabs( #tab4 was reserved for a country network graph visualization
    [
        "📊 Overview",
        "📈 Descriptive Stats",
        "🔥 Overlap Heatmap",
        #"🌐 Country Network",
        "🗂️ Project Breakdown",
        "🧹 Data Quality",
    ]
)

with tab1:
    st.subheader("Dataset Overview")
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Projects", len(df), new_projects_since_last_update if new_projects_since_last_update > 0 else None)
    c2.metric("Countries", len({v for lst in df["countries_impacted_list"] for v in lst}))
    c3.metric("Intervention Types", len({v for lst in df["intervention_type_list"] for v in lst}))
    c4.metric("Funding Types", df["type_of_funding"].nunique(), help = "Simple, unique (non-normalized) count, treats different ways a funding source's name might be mentioned as unique across projects.")
    c5.metric("Geo Scales", df["geographical_scale"].nunique())
    c6.metric("New Since Last Update", new_projects_since_last_update if new_projects_since_last_update > 0 else None)

    if recent_projects_df.empty:
        st.caption("ℹ️ No new projects were added in the latest scraping update.")
    else:
        #st.markdown("#### New Projects In Latest Update")
        st.caption("Recent projects can also be seen in Project Breakdown by sorting the table by `parsed_at`.")
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

    #st.markdown("---")
    st.subheader("Country Collaboration Network")

    if nx is None or Network is None:
        st.warning(
            "Install `networkx` and `pyvis` to render the interactive country collaboration network."
        )
    else:
        country_graph, edge_summary = build_country_collaboration_graph(df)

        if country_graph is None or country_graph.number_of_nodes() == 0:
            st.info("Not enough country data is available to build a collaboration network.")
        elif country_graph.number_of_edges() == 0:
            st.info("Projects currently do not share multiple-country collaborations in the filtered dataset.")
        else:
            graph_col, table_col = st.columns([3, 1])

            with table_col:
                available_countries = sorted(country_graph.nodes())
                selected_countries = st.multiselect(
                    "Countries to show",
                    options=available_countries,
                    default=available_countries,
                )

                edge_weights = [
                    attrs.get("weight", 0)
                    for _, _, attrs in country_graph.edges(data=True)
                ]
                min_weight = st.slider(
                    "Minimum shared-project count",
                    min_value=1,
                    max_value=max(edge_weights),
                    value=1,
                )

                filtered_graph = filter_country_collaboration_graph(
                    country_graph,
                    selected_countries,
                    min_weight,
                )
                c1, c2 = st.columns(2)
                c1.metric("Countries", filtered_graph.number_of_nodes())
                c2.metric("Collaborations", filtered_graph.number_of_edges(), help="Edges represent total count of shared projects between countries, filtered by the minimum shared-project count slider.")

                if not edge_summary.empty:
                    filtered_edge_summary = edge_summary[
                        edge_summary["country_1"].isin(selected_countries)
                        & edge_summary["country_2"].isin(selected_countries)
                        & (edge_summary["shared_projects"] >= min_weight)
                    ]
                    #rename columns for better display
                    st.dataframe(
                        filtered_edge_summary.rename(columns={
                        "country_1": "Country 1",
                        "country_2": "Country 2",
                        "shared_projects": "Shared Proj."
                    }),
                        use_container_width=True,
                        hide_index=True,
                        height=260,
                    )

            with graph_col:
                if filtered_graph.number_of_edges() == 0:
                    st.info("No collaborations match the current country selection and minimum weight.")
                else:
                    render_country_collaboration_network(filtered_graph)

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


    col3, col4 = st.columns(2)

    with col3:
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

    with col4:
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
    col5, col6 = st.columns(2)

    with col5:
        fig3 = px.histogram(
            df,
            x="country_count",
            nbins=10,
            title="Distribution of Countries per Project",
            labels={"country_count": "Number of Countries"},
            color_discrete_sequence=["#2196F3"],
        )
        st.plotly_chart(fig3, use_container_width=True)

    with col6:
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

with tab5:
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

with tab6:
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
