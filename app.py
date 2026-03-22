import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from etl import (
    load_json,
    normalise,
    explode_field,
    cooccurrence_matrix,
    cross_field_cooccurrence,
    data_quality_report,
    MULTI_VALUE_FIELDS,
)

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Energy Poverty Atlas Dashboard",
    page_icon="⚡",
    layout="wide",
)

st.title("⚡ Energy Poverty Atlas — Project Dashboard")
st.caption("Upload your parsed JSON dataset to explore projects, categories and attribute overlaps.")

# ── Sidebar: upload + filters ──────────────────────────────────────────────────
with st.sidebar:
    st.header("Data Source")
    uploaded = st.file_uploader("Upload JSON file", type=["json"])
    default_data_path = "data/raw/epah_details_atlas_projects_20260321T201622Z.json"
    default_raw_bytes = None
    if not uploaded:
        try:
            with open(default_data_path, "rb") as f:
                default_raw_bytes = f.read()
            st.caption(f"Using local default dataset: `{default_data_path}`")
        except FileNotFoundError:
            st.warning("No default file found for upload. Please upload a JSON file to proceed.")

    st.markdown("---")
    st.header("Filters")
    filter_scale = st.multiselect("Geographical Scale", [])
    filter_phase = st.multiselect("Energy Poverty Phase", [])
    filter_intervention = st.multiselect("Intervention Type", [])
    filter_country = st.multiselect("Country", [])

if not uploaded and default_raw_bytes is None:
    st.info("Upload a JSON file in the sidebar to get started.")
    st.stop()

# ── ETL ────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Running ETL pipeline…")
def run_etl(file_bytes: bytes) -> pd.DataFrame:
    import io
    return normalise(load_json(io.BytesIO(file_bytes)))

raw_bytes = uploaded.read() if uploaded else default_raw_bytes
df_full = run_etl(raw_bytes)

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

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Overview",
    "📈 Descriptive Stats",
    "🔥 Overlap Heatmap",
    "🗂️ Project Breakdown",
    "🧹 Data Quality",
])

# ─── Tab 1: Overview ───────────────────────────────────────────────────────────
with tab1:
    st.subheader("Dataset Overview")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Projects", len(df))
    c2.metric("Countries", len({v for lst in df["countries_impacted_list"] for v in lst}))
    c3.metric("Intervention Types", len({v for lst in df["intervention_type_list"] for v in lst}))
    c4.metric("Funding Types", df["type_of_funding"].nunique())
    c5.metric("Geo Scales", df["geographical_scale"].nunique())

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        scale_counts = df["geographical_scale"].value_counts().reset_index()
        scale_counts.columns = ["Scale", "Projects"]
        fig = px.pie(scale_counts, names="Scale", values="Projects",
                     title="Projects by Geographical Scale", hole=0.4)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        country_exp = explode_field(df, "countries_impacted")
        if not country_exp.empty:
            ctry_counts = country_exp["countries_impacted"].value_counts().reset_index()
            ctry_counts.columns = ["Country", "Projects"]
            fig2 = px.bar(ctry_counts, x="Country", y="Projects",
                          title="Projects per Country", color="Projects",
                          color_continuous_scale="Blues")
            st.plotly_chart(fig2, use_container_width=True)

    # Funding breakdown
    fund_counts = df["type_of_funding"].value_counts().reset_index()
    fund_counts.columns = ["Funding Type", "Projects"]
    fig3 = px.bar(fund_counts, x="Projects", y="Funding Type", orientation="h",
                  title="Projects by Funding Type", color="Projects",
                  color_continuous_scale="Teal")
    fig3.update_layout(yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig3, use_container_width=True)

# ─── Tab 2: Descriptive Stats ──────────────────────────────────────────────────
with tab2:
    st.subheader("Descriptive Statistics")

    col1, col2 = st.columns(2)

    with col1:
        # Intervention types distribution
        int_exp = explode_field(df, "intervention_type")
        if not int_exp.empty:
            int_counts = int_exp["intervention_type"].value_counts().reset_index()
            int_counts.columns = ["Intervention Type", "Count"]
            fig = px.bar(int_counts, x="Count", y="Intervention Type", orientation="h",
                         title="Intervention Types Distribution",
                         color="Count", color_continuous_scale="Oranges")
            fig.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Energy poverty phases distribution
        phase_exp = explode_field(df, "energy_poverty_phase")
        if not phase_exp.empty:
            phase_counts = phase_exp["energy_poverty_phase"].value_counts().reset_index()
            phase_counts.columns = ["Phase", "Count"]
            fig2 = px.bar(phase_counts, x="Phase", y="Count",
                          title="Energy Poverty Phases Distribution",
                          color="Count", color_continuous_scale="Purples")
            st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")
    col3, col4 = st.columns(2)

    with col3:
        fig3 = px.histogram(df, x="country_count", nbins=10,
                            title="Distribution of Countries per Project",
                            labels={"country_count": "Number of Countries"},
                            color_discrete_sequence=["#2196F3"])
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        fig4 = px.histogram(df, x="intervention_count", nbins=8,
                            title="Distribution of Intervention Types per Project",
                            labels={"intervention_count": "Number of Interventions"},
                            color_discrete_sequence=["#FF9800"])
        st.plotly_chart(fig4, use_container_width=True)

    # Professionals involved
    prof_exp = explode_field(df, "professionals_involved")
    if not prof_exp.empty:
        prof_counts = prof_exp["professionals_involved"].value_counts().reset_index()
        prof_counts.columns = ["Professional Type", "Count"]
        fig5 = px.bar(prof_counts, x="Professional Type", y="Count",
                      title="Professionals Involved Across Projects",
                      color="Count", color_continuous_scale="Greens")
        st.plotly_chart(fig5, use_container_width=True)

# ─── Tab 3: Overlap Heatmap ────────────────────────────────────────────────────
with tab3:
    st.subheader("Category Overlap & Co-occurrence")

    heatmap_mode = st.radio(
        "Select heatmap type",
        ["Intervention Types (self)", "Phases (self)", "Intervention × Phase",
         "Intervention × Country", "Phase × Country"],
        horizontal=True,
    )

    def plot_heatmap(matrix: pd.DataFrame, title: str):
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
        m = cooccurrence_matrix(df, "intervention_type")
        plot_heatmap(m, "Co-occurrence of Intervention Types across Projects")

    elif heatmap_mode == "Phases (self)":
        m = cooccurrence_matrix(df, "energy_poverty_phase")
        plot_heatmap(m, "Co-occurrence of Energy Poverty Phases across Projects")

    elif heatmap_mode == "Intervention × Phase":
        m = cross_field_cooccurrence(df, "intervention_type", "energy_poverty_phase")
        plot_heatmap(m, "Intervention Types × Energy Poverty Phases")

    elif heatmap_mode == "Intervention × Country":
        m = cross_field_cooccurrence(df, "intervention_type", "countries_impacted")
        plot_heatmap(m, "Intervention Types × Countries")

    elif heatmap_mode == "Phase × Country":
        m = cross_field_cooccurrence(df, "energy_poverty_phase", "countries_impacted")
        plot_heatmap(m, "Energy Poverty Phases × Countries")

    st.markdown("---")
    st.markdown("**ℹ️ How to read this:** Each cell shows how many projects share both row and column attributes. Higher values = stronger co-occurrence.")

# ─── Tab 4: Project Breakdown ──────────────────────────────────────────────────
with tab4:
    st.subheader("Per-Project Attribute Breakdown")

    display_cols = [
        "atlas_id", "project_title", "geographical_scale", "project_scope",
        "project_url", "country_count", "intervention_count", "phase_count", "type_of_funding"
    ]
    available = [c for c in display_cols if c in df.columns]
    display_df = df[available].copy()
    column_config = None
    if "project_url" in display_df.columns:
        column_config = {
            "project_url": st.column_config.LinkColumn(
                "Link",
                display_text="Open project",
            )
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
        row = df[df["project_title"] == selected].iloc[0]
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
                for p in partners:
                    st.markdown(f"- {p}")
            else:
                st.write("No partners listed.")

    st.markdown("---")
    st.markdown("### 📊 Attribute Counts per Project")
    melt_df = df[["project_title", "country_count", "intervention_count", "phase_count"]].copy()
    melt_df = melt_df.melt(id_vars="project_title", var_name="Attribute", value_name="Count")
    melt_df["Attribute"] = melt_df["Attribute"].map({
        "country_count": "Countries",
        "intervention_count": "Interventions",
        "phase_count": "Phases",
    })
    fig = px.bar(melt_df, x="project_title", y="Count", color="Attribute",
                 barmode="group", title="Attribute Counts per Project",
                 labels={"project_title": "Project"})
    fig.update_xaxes(tickangle=30)
    st.plotly_chart(fig, use_container_width=True)

# ─── Tab 5: Data Quality ───────────────────────────────────────────────────────
with tab5:
    st.subheader("🧹 Data Quality Report")
    dq = data_quality_report(df)

    col1, col2 = st.columns([2, 1])
    with col1:
        fig = px.bar(dq, x="field", y="fill_rate_%",
                     title="Field Fill Rate (%)",
                     color="fill_rate_%",
                     color_continuous_scale="RdYlGn",
                     range_color=[0, 100])
        fig.update_xaxes(tickangle=45)
        fig.add_hline(y=80, line_dash="dash", line_color="orange",
                      annotation_text="80% threshold")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.dataframe(dq, use_container_width=True, height=400)

    st.markdown("---")
    st.markdown("### 📥 Export Cleaned Data")
    csv = df.drop(columns=[c for c in df.columns if c.endswith("_list")]).to_csv(index=False)
    st.download_button("⬇️ Download cleaned CSV", csv, "cleaned_projects.csv", "text/csv")
