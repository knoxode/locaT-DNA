import streamlit as st
import pandas as pd
import json
from pathlib import Path

PUBLISHED_INDEX_PATH = Path(
    "/home/shaiikura/.cache/locaT-DNA/publish/index.json"
)  # adjust if needed


@st.cache_data(ttl=300)
def _load_index_df() -> pd.DataFrame:
    if not PUBLISHED_INDEX_PATH.exists():
        st.warning(f"Index file not found: {PUBLISHED_INDEX_PATH}")
        return pd.DataFrame()
    data = json.loads(PUBLISHED_INDEX_PATH.read_text())
    entries = data.get("entries", [])
    return pd.DataFrame(entries)


def checkAvailableGenomes() -> pd.DataFrame:
    df = _load_index_df()
    if df.empty:
        st.warning("No published genomes found. Has the cache timer run yet?")
    return df


def select_reference_genome():
    if "genome_confirmed" not in st.session_state:
        st.session_state.genome_confirmed = False

    df = checkAvailableGenomes()
    if df.empty:
        return None

    if not st.session_state.genome_confirmed:
        # Selection widgets
        species_options = sorted(df["species"].dropna().unique().tolist())
        species = st.selectbox("Species", species_options, key="species_select")

        df_s = df[df["species"] == species]
        provider_options = sorted(df_s["provider"].dropna().unique().tolist())
        provider = st.selectbox("Provider", provider_options, key="provider_select")

        df_sp = df_s[df_s["provider"] == provider]
        assembly_options = sorted(df_sp["assembly"].dropna().unique().tolist())
        assembly = st.selectbox("Assembly", assembly_options, key="assembly_select")

        df_sel = df_sp[df_sp["assembly"] == assembly]
        if df_sel.empty:
            st.error("No entries for the selected species/provider/assembly.")
            return None

        row = df_sel.iloc[0]
        anno_path = row.get("anno_gz_path") or row.get("anno_plain_path")

        result = {
            "provider": provider,
            "species": species,
            "assembly": assembly,
            "genome_path": row.get("genome_path"),
            "annotation_path": anno_path,
            "annotation_ext": row.get("anno_ext"),
            "genome_is_gz": bool(row.get("genome_is_gz")),
        }

        if st.button("Confirm genome selection"):
            st.session_state.genome_confirmed = True
            st.session_state.genome_selection = result

    if st.session_state.genome_confirmed:
        result = st.session_state.genome_selection
        st.write("### Confirmed Selections:")
        st.write(f""" Provider: {result["provider"]}""")
        st.write(f""" Genome: {result["species"]}""")
        st.write(f""" Assembly: {result["assembly"]}""")
        return result

    return None


def store_reference_path(workspace, selection):
    """
    Store the selected reference genome path in the workspace for later use.
    """
    ref_path = selection.get("genome_path")
    if not ref_path:
        st.error("No genome path found in selection.")
        return None

    ref_file = workspace.base_dir / "reference_path.txt"
    ref_file.write_text(ref_path)
    return ref_file