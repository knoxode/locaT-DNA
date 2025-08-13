import streamlit as st
from data_analysis.alignment import run_batch_alignment
from data_analysis.jbrowse import jbrowse_viewer

#Helper functions

def obtain_reference_path(ws):
    """
    Retrieve the reference genome path from the workspace.
    """
    # If not set, try to get it from the workspace
    reference = ws.get_selected_reference()
    if reference:
        st.session_state.reference_path = reference["genome_path"]
        return st.session_state.reference_path
    
    st.error("No reference genome selected.")
    return None

def ensure_files_uploaded(ws):
    """
    Ensure that the necessary files are uploaded in the workspace.
    """
    if not ws.get_dir("samples").exists() or not any(ws.get_dir("samples").glob("*.fastq.gz")):
        st.error("Please upload sample FASTQ files in the 'samples' directory.")
        return False
    if not ws.get_dir("tdna").exists() or not any(ws.get_dir("tdna").glob("*.fasta")):
        st.error("Please upload T-DNA sequences in the 'tdna' directory.")
        return False
    return True

def check_reference_selected(ws):
    """
    Check if a reference genome is selected in the workspace.
    """
    if not ws.get_selected_reference():
        st.error("Please select a reference genome before starting the analysis.")
        return False
    return True

def analysis(ws):
    if st.button("Start Analysis"):
        if not ensure_files_uploaded(ws):
            return

        if not check_reference_selected(ws):
            return

        st.write("Analysis started...")

        run_batch_alignment(
            workspace=ws,
            reference_path=obtain_reference_path(ws),
            threads=4
    )

        st.success("Analysis completed successfully!")
        jbrowse_viewer()