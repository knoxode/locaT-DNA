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
    df = checkAvailableGenomes()
    if df.empty:
        return None

    # 1) Species
    species_options = sorted(df["species"].dropna().unique().tolist())
    species = st.selectbox("Species", species_options, key="species_select")

    df_s = df[df["species"] == species]
    if df_s.empty:
        st.error("No entries for the selected species.")
        return None

    # 2) Provider
    provider_options = sorted(df_s["provider"].dropna().unique().tolist())
    provider = st.selectbox("Provider", provider_options, key="provider_select")

    df_sp = df_s[df_s["provider"] == provider]
    if df_sp.empty:
        st.error("No entries for the selected species/provider.")
        return None

    # 3) Assembly
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

    with st.expander("Selected reference summary", expanded=False):
        st.write(
            {
                "Genome": result["genome_path"],
                "Annotation": result["annotation_path"],
                "Type": result["annotation_ext"],
            }
        )

    return result
