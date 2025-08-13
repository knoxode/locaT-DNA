import subprocess
import streamlit as st
from session_management.workspace import Workspace

def run_batch_alignment(workspace: Workspace, reference_path: str, threads: int = 4):
    sample_dir = workspace.get_dir("samples")
    results_dir = workspace.get_dir("results")
    sample_files = list(sample_dir.glob("*.fastq.gz"))

    if not sample_files:
        st.error("No FASTQ files found in your workspace.")
        return

    for fastq_file in sample_files:
        sample_name = fastq_file.stem.replace(".fastq", "")
        output_bam = results_dir / f"{sample_name}_sorted.bam"

        st.write(f"Processing {fastq_file.name}...")

        # Minimap2 + samtools pipeline
        cmd = (
            f"minimap2 -ax map-ont -A2 -B4 -O6,24 -E2,1 -k9 -w2 -t {threads} {reference_path} {fastq_file} | "
            f"samtools view -bS - | "
            f"samtools sort -@ {threads} -o {output_bam}"
        )
        result = subprocess.run(cmd, shell=True, capture_output=True)
        if result.returncode == 0:
            st.success(f"Alignment and sorting completed for {fastq_file.name}. Output: {output_bam.name}")

            # Index BAM
            idx_cmd = f"samtools index {output_bam}"
            idx_result = subprocess.run(idx_cmd, shell=True, capture_output=True)
            if idx_result.returncode == 0:
                st.info(f"Indexing completed for {output_bam.name}.")
            else:
                st.error(f"Indexing failed for {output_bam.name}.")
        else:
            st.error(f"Alignment or sorting failed for {fastq_file.name}.")
            st.text(result.stderr.decode())

    st.success(f"Batch alignment completed. Results are in {results_dir}")

# Usage in your Streamlit app:
# run_batch_alignment(ws, reference_path="/path/to/reference.fa", threads=16)
