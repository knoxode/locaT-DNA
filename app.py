import streamlit as st
import tempfile, pathlib, zipfile
from plant_ref_cache import warm_cache_all_plants

# Warm the cache on startup (first user hit). Subsequent runs are instant.
if "refs" not in st.session_state:
    with st.status("Caching plant references (first run only)…", expanded=False) as s:
        rows = []

        def bump(i, total, label):
            s.update(label=f"[{i}/{total}] {label}")

        st.session_state.refs = warm_cache_all_plants(progress_cb=bump)
        s.update(label="Plant references cached.", state="complete")

"""
# 📍🧬 LocaT-DNA
Locate T-DNA insertion sites from nanopore amplicon sequencing
"""
"""
# Please provide your FASTQ files here:
For large volumes of files, you can archive them into a .zip file.
"""

uploaded = st.file_uploader(
    "Upload FASTQ, FASTA, or zipped folder",
    type=["fastq", "fq", "fastq.gz", "fa", "fasta", "fa.gz", "zip"],
)

if uploaded:
    filename = uploaded.name.lower()

    if filename.endswith(".zip"):
        tmpdir = tempfile.mkdtemp()
        with zipfile.ZipFile(uploaded, "r") as z:
            z.extractall(tmpdir)
        st.success(f"Extracted {filename} to {tmpdir}")
        st.write("Files found:", list(pathlib.Path(tmpdir).rglob("*")))

    else:
        st.info(f"Got single file: {uploaded.name}")
