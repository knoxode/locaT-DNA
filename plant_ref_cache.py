# plant_ref_cache.py
import os, re, requests, zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd

MANIFEST_URL = "https://raw.githubusercontent.com/ewels/AWS-iGenomes/master/ngi-igenomes_file_manifest.txt"
HTTP_PREFIX = "https://ngi-igenomes.s3.amazonaws.com/igenomes"  # public mirror
CACHE_ROOT = Path(
    os.environ.get("LOCATDNA_CACHE", Path.home() / ".cache" / "locaT-DNA")
)

PLANTS = {
    "Arabidopsis_thaliana",
    "Oryza_sativa_japonica",
    "Zea_mays",
    "Sorghum_bicolor",
    "Glycine_max",
    "Solanum_lycopersicum",
    "Brachypodium_distachyon",
    "Populus_trichocarpa",
    "Physcomitrella_patens",
    "Setaria_italica",
}


def _s3_to_http(s3: str) -> str:
    assert s3.startswith("s3://ngi-igenomes/igenomes/")
    return s3.replace("s3://ngi-igenomes/igenomes", HTTP_PREFIX, 1)


def _download(url: str, dest: Path, chunk: int = 1024 * 1024) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + ".partial")
        with open(tmp, "wb") as f:
            for c in r.iter_content(chunk_size=chunk):
                if c:
                    f.write(c)
        tmp.replace(dest)


def _head_exists(url: str) -> bool:
    try:
        r = requests.head(url, timeout=20, allow_redirects=True)
        return r.ok
    except Exception:
        return False


def _load_manifest() -> pd.DataFrame:
    txt = requests.get(MANIFEST_URL, timeout=30).text
    rows = []
    for line in txt.splitlines():
        m = re.match(
            r"s3://ngi-igenomes/igenomes/([^/]+)/([^/]+)/([^/]+)/([^/]+)/(.+)$", line
        )
        if m:
            genome, source, build, topdir, rest = m.groups()
            rows.append((genome, source, build, topdir, rest))
    df = pd.DataFrame(rows, columns=["genome", "source", "build", "topdir", "path"])
    return df[df["genome"].isin(PLANTS)]


def _targets_for_build(genome: str, source: str, build: str) -> Dict[str, str]:
    base = f"s3://ngi-igenomes/igenomes/{genome}/{source}/{build}"
    return {
        "fasta": f"{base}/Sequence/WholeGenomeFasta/genome.fa",
        "gtf": f"{base}/Annotation/Genes/genes.gtf",
        "bed": f"{base}/Annotation/Genes/genes.bed",
    }


def warm_cache_all_plants(progress_cb=None) -> Dict[str, Dict[str, Path]]:
    """
    Download (if missing) FASTA + annotation for all plant builds.
    Returns mapping: label -> {'fasta','annotation','kind','dir'} (local Paths).
    """
    df = _load_manifest()
    builds = (
        df[["genome", "source", "build"]]
        .drop_duplicates()
        .sort_values(["genome", "source", "build"])
    )
    out: Dict[str, Dict[str, Path]] = {}

    total = len(builds)
    for i, row in enumerate(builds.itertuples(index=False), start=1):
        genome, source, build = row
        label = f"{genome.replace('_',' ')} ({build}, {source})"
        if progress_cb:
            progress_cb(i, total, label)

        # where we store this build
        d = CACHE_ROOT / f"{genome}__{source}__{build}"
        d.mkdir(parents=True, exist_ok=True)

        # fetch fasta
        s3s = _targets_for_build(genome, source, build)
        fasta_http = _s3_to_http(s3s["fasta"])
        fasta_path = d / "genome.fa"
        if not fasta_path.exists():
            _download(fasta_http, fasta_path)

        # pick annotation: prefer GTF, try gz, then BED, then BED.gz
        candidates = [s3s["gtf"], s3s["gtf"] + ".gz", s3s["bed"], s3s["bed"] + ".gz"]
        ann_path: Optional[Path] = None
        kind: Optional[str] = None
        for s3 in candidates:
            url = _s3_to_http(s3)
            if _head_exists(url):
                p = d / Path(s3).name
                if not p.exists():
                    _download(url, p)
                ann_path = p
                kind = (
                    "gtf" if p.suffix in (".gtf", ".gz") and "gtf" in p.name else "bed"
                )
                break

        out[label] = {
            "fasta": fasta_path,
            "annotation": ann_path,
            "kind": kind,
            "dir": d,
        }
    return out
