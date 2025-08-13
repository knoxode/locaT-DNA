# src/app/ui/genome_selector.py
import streamlit as st
import pandas as pd

def _list_genomes_df(gc) -> pd.DataFrame:
    """
    Convert GenomeCache.list_genomes() output into a DataFrame for easy filtering.
    """
    rows = gc.list_genomes()  # [(provider, species, assembly, genome_path), ...]
    return pd.DataFrame(rows, columns=["provider", "species", "assembly", "genome_path"])

def select_reference_genome(gc, ws):
    """
    Display chained dropdowns (Species → Provider → Assembly) for genome selection.
    Returns a dict with resolved file paths after confirmation, or None if not confirmed.
    """
    if "genome_confirmed" not in st.session_state:
        st.session_state.genome_confirmed = False

    df = _list_genomes_df(gc)
    if df.empty:
        st.warning("No published genomes found yet.")
        return None

    if not st.session_state.genome_confirmed:
        # Species dropdown
        species = st.selectbox("Species", sorted(df["species"].unique()))
        df_species = df[df["species"] == species]

        # Provider dropdown
        provider = st.selectbox("Provider", sorted(df_species["provider"].unique()))
        df_provider = df_species[df_species["provider"] == provider]

        # Assembly dropdown
        assembly = st.selectbox("Assembly", sorted(df_provider["assembly"].unique()))

        # Resolve final published paths directly from GenomeCache
        try:
            paths = gc.get_paths(provider, species, assembly)
        except KeyError:
            st.error("No published genome found for this selection.")
            return None

        selection = {
            "provider": provider,
            "species": species,
            "assembly": assembly,
            "genome_path": paths.get("genome"),
            "genome_fai": paths.get("fai"),
            "annotation_path": paths.get("anno") or paths.get("anno_gz"),
            "annotation_ext": _anno_ext_from_paths(paths.get("anno"), paths.get("anno_gz")),
        }

        if st.button("Confirm genome selection"):
            st.session_state.genome_confirmed = True
            ws.store_selected_reference(selection)
            

    if st.session_state.genome_confirmed:
        stored_reference = ws.get_reference()

        st.success("Genome selection confirmed")
        st.write(f"**Provider:** {stored_reference['provider']}")
        st.write(f"**Species:** {stored_reference['species']}")
        st.write(f"**Assembly:** {stored_reference['assembly']}")

    return None

def _anno_ext_from_paths(anno_path: str | None, anno_gz: str | None) -> str | None:
    """
    Infer annotation file extension from given paths.
    """
    p = (anno_path or anno_gz or "").lower()
    for ext in (".gff3", ".gff", ".gtf"):
        if p.endswith(ext):
            return ext
    return None
