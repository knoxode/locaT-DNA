import streamlit as st
import tempfile, pathlib, zipfile
from data_analysis.orchestrator import analysis
from data_analysis.jbrowse import jbrowse_viewer
from genome_database.genome_cache import GenomeCache
from genome_selection import select_reference_genome
from fasta_upload_handling import sample_upload
from tdna_fasta_upload import tdna_upload
from session_management.workspace import Workspace as ws

ws = ws()
gc = GenomeCache()

"""
# 📍🧬 LocaT-DNA
Locate T-DNA insertion sites from nanopore amplicon sequencing
"""

"""
# Requirements
- **Nanopore Sequencing**: The app is designed to analyze nanopore sequencing data, specifically FASTQ files.
- **T-DNA Sequences**: Users must provide a T-DNA sequence in FASTA format.
- **Reference Genome**: Users need to select a reference genome from the available genomes.
- **Analysis**: The app performs alignment of the nanopore reads against the reference genome and T-DNA sequence to identify insertion sites.
- **JBrowse Viewer**: The results are visualized using JBrowse, allowing users to explore the alignment and insertion sites interactively.
"""

"""
# Sample FASTQ files
For large volumes of files, you can archive them into a .zip file.
"""

sample_upload(ws)

"""
# T-DNA Sequence
"""

tdna_upload(ws)

"""
# Reference Genome Selection
"""

select_reference_genome(gc, ws)

# This section will be for users to start the analysis and then show the results
"""
# Analysis
"""

analysis(ws)