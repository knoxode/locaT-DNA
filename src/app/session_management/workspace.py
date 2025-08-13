import streamlit as st
import secrets
from pathlib import Path
import tempfile
import shutil


class Workspace:
    def __init__(self, base_dir: Path | None = None):
        if "sid" not in st.session_state:
            st.session_state.sid = secrets.token_urlsafe(12)

        self.sid = st.session_state.sid
        self.base_dir = Path(base_dir or tempfile.gettempdir()) / "locaT-DNA" / self.sid

        # Initialize directory tree
        self.samples_dir = self.base_dir / "samples"
        self.tdna_dir = self.base_dir / "tdna"
        self.results_dir = self.base_dir / "results"

        #Storing the reference path that was chosen by the user
        self.reference = {}

        for d in (self.samples_dir, self.tdna_dir, self.results_dir):
            d.mkdir(parents=True, exist_ok=True)

    def get_dir(self, name: str) -> Path:
        """Return a subdirectory path by logical name."""
        mapping = {
            "samples": self.samples_dir,
            "tdna": self.tdna_dir,
            "results": self.results_dir,
        }
        if name not in mapping:
            raise ValueError(f"Unknown directory name: {name}")
        return mapping[name]

    def get_reference(self) -> dict:
        """Return the currently selected reference genome information."""
        return self.reference   

    def save_file(self, file, subdir: str, filename: str | None = None) -> Path:
        """
        Save an uploaded file-like object into one of the subdirs.
        file: StreamlitUploadedFile or bytes-like
        subdir: 'samples' | 'tdna' | 'results'
        filename: optional override; defaults to file.name if available
        """
        target_dir = self.get_dir(subdir)
        name = filename or getattr(file, "name", "unnamed")
        dest = target_dir / name
        tmp = dest.with_suffix(dest.suffix + ".part")
        data = file.getbuffer() if hasattr(file, "getbuffer") else file
        tmp.write_bytes(data)
        tmp.replace(dest)
        return dest
    
    def store_selected_reference(self, selection: dict):
        """
        Store the path of the selected reference genome.
        This will be used later in the analysis.
        """
        self.reference = selection

    def clear(self):
        """Delete the entire workspace for this session."""
        shutil.rmtree(self.base_dir, ignore_errors=True)

    def __repr__(self):
        return f"<Workspace sid={self.sid} base={self.base_dir}>"
