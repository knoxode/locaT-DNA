import streamlit as st
from data_analysis.alignment import run_batch_alignment


def analysis(ws,reference_path):
    if st.button("Start Analysis"):
        st.write("Analysis started...")

        run_batch_alignment(
            workspace=ws,
            reference_path=reference_path,
            threads=4
    )
