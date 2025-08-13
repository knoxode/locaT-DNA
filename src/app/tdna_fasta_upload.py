import streamlit as st

def tdna_upload(ws):
    """
    Uploads a FASTA file and returns the content as a string.
    """
    uploaded_tnda = st.file_uploader("Upload a FASTA file", type=["fasta", "fa"], accept_multiple_files=False)
    
    if uploaded_tnda:
        ws.save_file(uploaded_tnda, "tdna")
        st.toast(
            "The T-DNA sequence was successfully uploaded.", icon="✅"
        )

