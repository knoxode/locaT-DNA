import streamlit as st

def sample_upload(workspace):
    uploaded_samples = st.file_uploader(
        "Upload FASTQ, FASTA, or zipped folder",
        type=["fastq", "fq", "fastq.gz", "fa", "fasta", "fa.gz", "zip"],
        accept_multiple_files=True
    )

    if uploaded_samples:
        st.info(
            "You can upload multiple files. If you have many files, consider zipping them into a single archive."
        )
        for u in uploaded_samples:
            path = workspace.save_file(u, "samples")
        st.toast(
            "All sample FASTA files were successfully uploaded.", icon="✅"
        )