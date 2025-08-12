import streamlit as st


def sample_upload():
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
