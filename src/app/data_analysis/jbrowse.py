import streamlit as st # type: ignore
import json
import urllib.parse
from typing import Optional, Dict, Any

def session_instantiater(ws):

    ref = ws.get_reference()
    
    fasta_url = ref.get("genome_path")
    fai_url = ref.get("genome_fai")
    annotation_url = ref.get("annotation_path")
    annotation_index_url = ref.get("annotation_index_path")

    #TODO: Add missing bam and bai URLs, chrom, pos, pad, alias_url
    return JBrowseSession(
        jbrowse_base="localhost:3000",
        assembly_name= ws.get_reference(),
        fasta_url= fasta_url,
        fai_url= fai_url,
        annotation_url=annotation_url,
        annotation_index_url=annotation_index_url,
        bam_url="",
        bai_url="",
        chrom="",
        pos=1000,
        pad=200,
        alias_url=None,
        highlight=True,
        marker_name="T-DNA"
    )

class JBrowseSession:
    def __init__(
        self,
        jbrowse_base: str,
        assembly_name: str,
        fasta_url: str,
        fai_url: str,
        annotation_url: str,
        annotation_index_url: Optional[str],
        bam_url: str,
        bai_url: str,
        chrom: str,
        pos: int,
        pad: int = 200,
        alias_url: Optional[str] = None,
        highlight: bool = True,
        marker_name: str = "T-DNA",
    ):
        """Initialize the JBrowse session with all needed file paths and parameters."""
        if not jbrowse_base.endswith("/"):
            jbrowse_base += "/"

        self.jbrowse_base = jbrowse_base
        self.assembly_name = assembly_name
        self.fasta_url = fasta_url
        self.fai_url = fai_url
        self.alias_url = alias_url
        self.annotation_url = annotation_url
        self.annotation_index_url = annotation_index_url
        self.bam_url = bam_url
        self.bai_url = bai_url
        self.chrom = chrom
        self.pos = pos
        self.pad = pad
        self.highlight = highlight
        self.marker_name = marker_name

    def _assembly_dict(self) -> Dict[str, Any]:
        assembly = {
            "name": self.assembly_name,
            "sequence": {
                "type": "ReferenceSequenceTrack",
                "trackId": f"{self.assembly_name}_refseq",
                "adapter": {
                    "type": "IndexedFastaAdapter",
                    "fastaLocation": {"uri": self.fasta_url},
                    "faiLocation": {"uri": self.fai_url},
                },
            },
        }
        if self.alias_url:
            assembly["refNameAliases"] = {
                "adapter": {
                    "type": "RefNameAliasAdapter",
                    "location": {"uri": self.alias_url},
                }
            }
        return assembly

    def _annotation_track(self) -> Dict[str, Any]:
        """Supports either GFF3+TBI or BigBed."""
        if self.annotation_url.endswith(".bb"):
            return {
                "type": "FeatureTrack",
                "trackId": "genes_bb",
                "name": "Genes",
                "assemblyNames": [self.assembly_name],
                "adapter": {"type": "BigBedAdapter", "bigBedLocation": {"uri": self.annotation_url}},
                "displays": [{"type": "LinearBasicDisplay", "displayId": "genes_bb_disp"}],
            }
        elif self.annotation_url.endswith(".gff3.gz") and self.annotation_index_url:
            return {
                "type": "FeatureTrack",
                "trackId": "genes_gff3",
                "name": "Genes",
                "assemblyNames": [self.assembly_name],
                "adapter": {
                    "type": "Gff3TabixAdapter",
                    "gffGzLocation": {"uri": self.annotation_url},
                    "index": {"location": {"uri": self.annotation_index_url}, "indexType": "TBI"},
                },
                "displays": [{"type": "LinearBasicDisplay", "displayId": "genes_gff3_disp"}],
            }
        else:
            raise ValueError("Unsupported annotation format or missing index file.")

    def _alignment_track(self) -> Dict[str, Any]:
        return {
            "type": "AlignmentsTrack",
            "trackId": "alignments_bam",
            "name": "Alignments",
            "assemblyNames": [self.assembly_name],
            "adapter": {
                "type": "BamAdapter",
                "bamLocation": {"uri": self.bam_url},
                "index": {"location": {"uri": self.bai_url}, "indexType": "BAI"},
            },
            "displays": [{"type": "LinearAlignmentsDisplay", "displayId": "aln_bam_disp"}],
        }

    def _marker_track(self) -> Dict[str, Any]:
        start = max(0, self.pos - 1)  # keep 1bp marker
        end = self.pos
        return {
            "type": "FeatureTrack",
            "trackId": "marker",
            "name": self.marker_name,
            "assemblyNames": [self.assembly_name],
            "adapter": {
                "type": "FromConfigAdapter",
                "features": [{
                    "uniqueId": f"marker_{self.chrom}_{start}_{end}",
                    "refName": self.chrom,
                    "start": start,
                    "end": end,
                    "name": self.marker_name,
                }],
            },
            "displays": [{"type": "LinearBasicDisplay", "displayId": "marker_disp"}],
        }

    def build_url(self) -> str:
        start = max(0, self.pos - self.pad)
        end = self.pos + self.pad
        locus = f"{self.chrom}:{start}-{end}"

        assembly = self._assembly_dict()
        tracks = [self._annotation_track(), self._alignment_track(), self._marker_track()]
        view_tracks = [t["trackId"] for t in tracks]

        session_dict = {
            "session": {
                "name": "Dynamic T-DNA session",
                "view": {
                    "id": "lgv",
                    "type": "LinearGenomeView",
                    "tracks": view_tracks,
                    "loc": locus,
                },
                "assemblies": [assembly],
                "sessionTracks": tracks,
            }
        }

        session_param = "json-" + json.dumps(session_dict, separators=(",", ":"))
        url = f"{self.jbrowse_base}?session={urllib.parse.quote(session_param)}"
        if self.highlight:
            url += f"&highlight={urllib.parse.quote(locus)}"
        return url


def generate_jbrowse_session():
    """
    Generate a JBrowse 2 session URL with a test data configuration.
    This is a placeholder function that can be replaced with actual session generation logic.
    """
    return "https://jbrowse.org/code/jb2/main/?config=test_data%2Fconfig.json"

def jbrowse_viewer(jbrowse_url="https://jbrowse.org/code/jb2/main/?config=test_data%2Fconfig.json"):
    """
    Embed a JBrowse 2 session in Streamlit using an iframe.
    :param jbrowse_url: URL to the JBrowse instance or config.
    """
    st.header("Your Alignment in JBrowse")
    st.components.v1.iframe(jbrowse_url, height=600, scrolling=True)