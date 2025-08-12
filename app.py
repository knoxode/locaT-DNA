import streamlit as st
import tempfile, pathlib, zipfile
from data_analysis.orchestrator import analysis
from genome_selection import select_reference_genome, store_reference_path
from fasta_upload_handling import sample_upload
from tdna_fasta_upload import tdna_upload
from session_management.workspace import Workspace as ws

ws = ws()

"""
# 📍🧬 LocaT-DNA
Locate T-DNA insertion sites from nanopore amplicon sequencing
"""
"""
# Please provide your sample FASTQ files here:
For large volumes of files, you can archive them into a .zip file.
"""

sample_upload(ws)

"""
# Please Provide your T-DNA Sequence
"""

tdna_upload(ws)

"""
# Reference Genome Selection
"""

selection = select_reference_genome()
reference_path = None  # Ensure it's always defined
if selection:
    reference_path = store_reference_path(ws, selection)



# This section will be for users to start the analysis and then show the results
"""
# Analysis
"""

analysis(ws, reference_path)
