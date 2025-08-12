import streamlit as st
import tempfile, pathlib, zipfile
from start_button import startAnalysisButton
from genome_selection import select_reference_genome
from fasta_upload_handling import sample_upload

"""
# 📍🧬 LocaT-DNA
Locate T-DNA insertion sites from nanopore amplicon sequencing
"""
"""
# Please provide your sample FASTQ files here:
For large volumes of files, you can archive them into a .zip file.
"""

sample_upload()

st.title("Reference Genome")
selection = select_reference_genome()
if selection:
    st.success(
        f"Selected: {selection['species']} / {selection['provider']} / {selection['assembly']}"
    )

# This section will be for users to start the analysis and then show the results
"""
# Analysis
"""

startAnalysisButton()
