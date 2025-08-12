#!/usr/bin/env python3
"""
locaT-DNA reference cache (YAML-driven, multi-source)

- Reads a catalog of genomes from BASE/sources.yaml (or $LOCAT_DNA_SOURCES).
- Downloads FASTA (.fa.gz) and GTF (.gtf.gz) with ETag/Last-Modified caching.
- Decompresses FASTA to .fa and indexes (.fai) via pyfaidx or samtools.
- Publishes stable symlinks per source.
- Writes a single manifest.json aggregating all sources.

Layout (BASE=/home/shaiikura/.cache/locaT-DNA by default)
BASE/
  cache/{provider}/{species}/{assembly}/
    raw/
      genome.fa.gz         # downloaded (has .etag/.lastmod)
      genes.gtf.gz
    ready/
      genome.fa            # unzipped
      genome.fa.fai
      genes.gtf.gz         # symlink (or copy) to raw/genes.gtf.gz
  publish/{provider}/{species}/
    genome.fa     -> ../../../../cache/.../ready/genome.fa
    genes.gtf.gz  -> ../../../../cache/.../ready/genes.gtf.gz
  manifest.json
  sources.yaml    # <-- your catalog goes here (or set LOCAT_DNA_SOURCES)
"""

from __future__ import annotations

import contextlib
import gzip
import json
import os
import bz2
import lzma
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import requests

# Optional dependencies
try:
    import yaml  # PyYAML for sources.yaml
except Exception:
    yaml = None

try:
    from pyfaidx import Fasta  # for indexing (fallback to samtools)
except Exception:
    Fasta = None

# ---------------- Config & defaults ----------------

BASE = Path(os.getenv("LOCAT_DNA_BASE", "/home/shaiikura/.cache/locaT-DNA"))
CACHE_ROOT = Path(os.getenv("LOCAT_DNA_CACHE", str(BASE / "cache")))
PUBLISH_ROOT = Path(os.getenv("LOCAT_DNA_PUBLISH", str(BASE / "publish")))
PUBLISHED_INDEX_PATH = Path(
    os.getenv("LOCAT_DNA_PUBLISHED_INDEX", str(PUBLISH_ROOT / "index.json"))
)
MANIFEST_PATH = Path(os.getenv("LOCAT_DNA_MANIFEST", str(BASE / "manifest.json")))
SOURCES_PATH = Path(os.getenv("LOCAT_DNA_SOURCES", str(BASE / "sources.yaml")))

USER_AGENT = "locaT-DNA-cache/1.0"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


@dataclass(frozen=True)
class SourceSpec:
    provider: str
    species: str  # e.g., Arabidopsis_thaliana
    assembly: str  # e.g., TAIR10
    fasta_url: str
    anno_url: Optional[str] = None
    decompress_fasta: bool = True  # keep True: samtools can't index .fa.gz


@dataclass
class Paths:
    base: Path
    raw: Path
    ready: Path
    raw_fa_gz: Path
    ready_fa: Path
    ready_fai: Path
    anno_ext: Optional[str]  # '.gtf' | '.gff3' | '.gff' | None
    raw_anno_gz: Optional[Path]  # raw/genes{ext}.gz
    ready_anno_gz: Optional[Path]  # ready/genes{ext}.gz


# ---------------- Helpers ----------------


def safe_makedirs(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def etag_path(target: Path) -> Path:
    return target.with_suffix(target.suffix + ".etag")


def lm_path(target: Path) -> Path:
    return target.with_suffix(target.suffix + ".lastmod")


@contextlib.contextmanager
def file_lock(lockfile: Path):
    p = Path(lockfile)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(p), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    try:
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        yield
    finally:
        try:
            p.unlink(missing_ok=True)
        except FileNotFoundError:
            pass


def run_cmd(cmd: List[str]):
    subprocess.run(cmd, check=True)


def download_with_cache(url: str, target: Path) -> bool:
    safe_makedirs(target.parent)
    etag_file = etag_path(target)
    lm_file = lm_path(target)
    headers: Dict[str, str] = {}
    if etag_file.exists():
        headers["If-None-Match"] = etag_file.read_text().strip()
    if lm_file.exists():
        headers["If-Modified-Since"] = lm_file.read_text().strip()
    with SESSION.get(
        url, stream=True, headers=headers, timeout=600, allow_redirects=True
    ) as r:
        if r.status_code == 304:
            return False
        r.raise_for_status()
        tmp = target.with_suffix(target.suffix + ".part")
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    fh.write(chunk)
        shutil.move(tmp, target)
        if etag := r.headers.get("ETag"):
            etag_file.write_text(etag)
        if lastmod := r.headers.get("Last-Modified"):
            lm_file.write_text(lastmod)
        return True


def layout(cache_root: Path, s: SourceSpec) -> Paths:
    base = cache_root / s.provider / s.species / s.assembly
    raw = base / "raw"
    ready = base / "ready"

    anno_ext = guess_anno_ext(s.anno_url) if s.anno_url else None
    raw_anno_gz = (raw / f"genes{anno_ext}.gz") if s.anno_url else None
    ready_anno_gz = (ready / f"genes{anno_ext}.gz") if s.anno_url else None

    return Paths(
        base=base,
        raw=raw,
        ready=ready,
        raw_fa_gz=raw / "genome.fa.gz",
        ready_fa=ready / "genome.fa",
        ready_fai=ready / "genome.fa.fai",
        anno_ext=anno_ext,
        raw_anno_gz=raw_anno_gz,
        ready_anno_gz=ready_anno_gz,
    )


def guess_anno_ext(url: str) -> str:
    """
    Guess '.gff3', '.gff', or '.gtf' from the URL. Defaults to '.gtf'.
    """
    u = url.lower()
    for ext in (".gff3.gz", ".gff.gz", ".gtf.gz", ".gff3", ".gff", ".gtf"):
        if u.endswith(ext):
            return "." + ext.lstrip(".").split(".")[0]  # -> .gff3 / .gff / .gtf
    return ".gtf"


def sniff_compression(path: Path) -> str:
    """
    Return one of: 'gzip', 'bz2', 'xz', 'plain'.
    bgzip is detected as 'gzip' (same magic).
    """
    with open(path, "rb") as fh:
        head = fh.read(6)
    if head.startswith(b"\x1f\x8b"):
        return "gzip"
    if head.startswith(b"BZh"):
        return "bz2"
    if head.startswith(b"\xfd7zXZ\x00"):
        return "xz"
    return "plain"


def decompress_any(src_path: Path, dst_path: Path, lock: bool = True) -> None:
    """
    Decompress/copy src_path -> dst_path based on detected compression type.
    Uses an atomic .part file then moves into place.
    """

    def _open(src: Path):
        kind = sniff_compression(src)
        if kind == "gzip":
            return gzip.open(src, "rb")
        elif kind == "bz2":
            return bz2.open(src, "rb")
        elif kind == "xz":
            return lzma.open(src, "rb")
        else:
            return open(src, "rb")

    lockfile = dst_path.with_suffix(dst_path.suffix + ".lock")
    ctx = file_lock(lockfile) if lock else contextlib.nullcontext()
    with ctx:
        tmp = dst_path.with_suffix(dst_path.suffix + ".part")
        with _open(src_path) as fin, open(tmp, "wb") as fout:
            shutil.copyfileobj(fin, fout)
        shutil.move(tmp, dst_path)


def _build_published_record(
    s: SourceSpec, publish_root: Path, fasta_meta: Dict, gtf_meta: Optional[Dict]
) -> Dict:
    out_dir = publish_root / s.provider / s.species

    genome_candidates = [out_dir / "genome.fa", out_dir / "genome.fa.gz"]
    genome_path = next((p for p in genome_candidates if p.exists()), None)

    anno_ext = (gtf_meta or {}).get("ext")
    anno_gz_path = (out_dir / f"genes{anno_ext}.gz") if anno_ext else None
    anno_plain_path = (out_dir / f"genes{anno_ext}") if anno_ext else None

    def _stat(p: Optional[Path]):
        try:
            st = p.stat() if p else None
            return (st.st_size, int(st.st_mtime)) if st else (None, None)
        except FileNotFoundError:
            return None, None

    genome_size, genome_mtime = _stat(genome_path)
    anno_gz_size, anno_gz_mtime = _stat(anno_gz_path)
    anno_plain_size, anno_plain_mtime = _stat(anno_plain_path)

    return {
        "provider": s.provider,
        "species": s.species,
        "assembly": s.assembly,
        "display": f"{s.provider}/{s.species} ({s.assembly})",
        "genome_path": str(genome_path) if genome_path else None,
        "genome_is_gz": bool(genome_path and genome_path.suffix == ".gz"),
        "anno_ext": anno_ext,
        "anno_gz_path": (
            str(anno_gz_path) if anno_gz_path and anno_gz_path.exists() else None
        ),
        "anno_plain_path": (
            str(anno_plain_path)
            if anno_plain_path and anno_plain_path.exists()
            else None
        ),
        "genome_size": genome_size,
        "genome_mtime": genome_mtime,
        "anno_gz_size": anno_gz_size,
        "anno_gz_mtime": anno_gz_mtime,
        "anno_plain_size": anno_plain_size,
        "anno_plain_mtime": anno_plain_mtime,
    }


def write_published_index(
    publish_root: Path, entries: List[Dict], path: Path = PUBLISHED_INDEX_PATH
):
    safe_makedirs(path.parent)
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "publish_root": str(publish_root),
        "schema": {
            "publish": "{publish}/{provider}/{species}/genome.fa(.gz), genes{.gtf|.gff|.gff3}(.gz)"
        },
        "entries": entries,
        "version": 1,
    }
    tmp = path.with_suffix(path.suffix + ".part")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
    shutil.move(tmp, path)


# ---------------- Core ops ----------------


def grab_fasta(cache_root: Path, s: SourceSpec) -> Dict:
    p = layout(cache_root, s)
    safe_makedirs(p.raw)
    safe_makedirs(p.ready)

    with file_lock(p.raw_fa_gz.with_suffix(p.raw_fa_gz.suffix + ".lock")):
        changed = download_with_cache(s.fasta_url, p.raw_fa_gz)

    target_fa = p.ready_fa if s.decompress_fasta else p.raw_fa_gz
    if s.decompress_fasta and (changed or not p.ready_fa.exists()):
        decompress_any(p.raw_fa_gz, p.ready_fa)

    # Index
    if Fasta is not None:
        _ = Fasta(str(target_fa), rebuild=True)
    else:
        try:
            run_cmd(["samtools", "faidx", str(target_fa)])
        except FileNotFoundError:
            pass

    st = Path(target_fa).stat()
    return {
        "raw": str(p.raw_fa_gz),
        "file": str(target_fa),
        "fai": str(target_fa) + ".fai",
        "changed": changed,
        "size_bytes": st.st_size,
        "mtime": int(st.st_mtime),
    }


def grab_gtf(cache_root: Path, s: SourceSpec) -> Optional[Dict]:
    if not s.anno_url:
        return None
    p = layout(cache_root, s)
    safe_makedirs(p.raw)
    safe_makedirs(p.ready)

    with file_lock(p.raw_anno_gz.with_suffix(p.raw_anno_gz.suffix + ".lock")):
        changed = download_with_cache(s.anno_url, p.raw_anno_gz)

    # ready/ points to raw/
    if p.ready_anno_gz.exists() or p.ready_anno_gz.is_symlink():
        p.ready_anno_gz.unlink()
    try:
        p.ready_anno_gz.symlink_to(p.raw_anno_gz.resolve())
    except OSError:
        shutil.copy2(p.raw_anno_gz, p.ready_anno_gz)

    # Also provide an uncompressed file in ready/ with the right extension
    ready_plain = p.ready_anno_gz.with_suffix("")  # drop .gz -> genes{ext}
    if changed or not ready_plain.exists():
        decompress_any(p.raw_anno_gz, ready_plain)

    st_gz = p.ready_anno_gz.stat()
    st_plain = ready_plain.stat()
    return {
        "raw": str(p.raw_anno_gz),
        "file": str(p.ready_anno_gz),
        "file_uncompressed": str(ready_plain),
        "changed": changed,
        "size_bytes_gz": st_gz.st_size,
        "size_bytes_uncompressed": st_plain.st_size,
        "mtime_gz": int(st_gz.st_mtime),
        "mtime_uncompressed": int(st_plain.st_mtime),
        "ext": p.anno_ext,  # handy for callers
    }


def provide_symlinks(
    publish_root: Path, s: SourceSpec, fasta_meta: Dict, gtf_meta: Optional[Dict]
):
    out = publish_root / s.provider / s.species
    safe_makedirs(out)

    fa_src = Path(fasta_meta["file"]).resolve()
    fa_dst = out / ("genome.fa" if fa_src.suffix != ".gz" else "genome.fa.gz")
    if fa_dst.exists() or fa_dst.is_symlink():
        fa_dst.unlink()
    fa_dst.symlink_to(fa_src)

    if gtf_meta:
        # gz
        anno_gz_src = Path(gtf_meta["file"]).resolve()
        anno_gz_dst = out / f"genes{gtf_meta.get('ext', '.gtf')}.gz"
        if anno_gz_dst.exists() or anno_gz_dst.is_symlink():
            anno_gz_dst.unlink()
        anno_gz_dst.symlink_to(anno_gz_src)

        # plain (optional, but useful)
        anno_plain_src = Path(gtf_meta["file_uncompressed"]).resolve()
        anno_plain_dst = out / f"genes{gtf_meta.get('ext', '.gtf')}"
        if anno_plain_dst.exists() or anno_plain_dst.is_symlink():
            anno_plain_dst.unlink()
        anno_plain_dst.symlink_to(anno_plain_src)


# ---------------- Catalog loading ----------------


def load_catalog() -> List[SourceSpec]:
    if SOURCES_PATH.suffix.lower() == ".json" and SOURCES_PATH.exists():
        cfg = json.loads(SOURCES_PATH.read_text()) or {}
        items = cfg.get("sources") or []
        return [_spec_from_dict(item) for item in items]

    if SOURCES_PATH.exists():
        if yaml is None:
            raise SystemExit(
                f"ERROR: {SOURCES_PATH} exists but PyYAML is not installed. "
                "Install pyyaml (or set LOCAT_DNA_SOURCES to a .json file)."
            )
        cfg = yaml.safe_load(SOURCES_PATH.read_text()) or {}
        items = cfg.get("sources") or []
        return [_spec_from_dict(item) for item in items]

    # Fallback: Arabidopsis only
    return [
        SourceSpec(
            provider="ensemblplants",
            species="Arabidopsis_thaliana",
            assembly="TAIR10",
            fasta_url=(
                "https://ftp.ensemblgenomes.ebi.ac.uk/pub/plants/current/fasta/"
                "arabidopsis_thaliana/dna/Arabidopsis_thaliana.TAIR10.dna.toplevel.fa.gz"
            ),
            anno_url=(
                "https://ftp.ensemblgenomes.ebi.ac.uk/pub/plants/current/gtf/"
                "arabidopsis_thaliana/Arabidopsis_thaliana.TAIR10.gtf.gz"
            ),
            decompress_fasta=True,
        )
    ]


def _spec_from_dict(d: Dict) -> SourceSpec:
    required = ["provider", "species", "assembly", "fasta_url"]
    missing = [k for k in required if k not in d or not d[k]]
    if missing:
        raise ValueError(f"Invalid source entry; missing {missing}: {d}")
    return SourceSpec(
        provider=d["provider"],
        species=d["species"],
        assembly=d["assembly"],
        fasta_url=d["fasta_url"],
        anno_url=d.get("anno_url"),
        decompress_fasta=bool(d.get("decompress_fasta", True)),
    )


# ---------------- Orchestration ----------------


def run_once(
    cache_root: Path, publish_root: Path, manifest_path: Path, catalog: List[SourceSpec]
):
    results = []
    published_records = []

    for s in catalog:
        fasta_meta = grab_fasta(cache_root, s)
        gtf_meta = grab_gtf(cache_root, s)
        provide_symlinks(publish_root, s, fasta_meta, gtf_meta)

        # keep manifest as-is
        results.append(
            {
                "provider": s.provider,
                "species": s.species,
                "assembly": s.assembly,
                "fasta": fasta_meta,
                "gtf": gtf_meta,
            }
        )

        # add a published record for later retrieval
        published_records.append(
            _build_published_record(s, publish_root, fasta_meta, gtf_meta)
        )

    # write published index (for other processes)
    write_published_index(publish_root, published_records, PUBLISHED_INDEX_PATH)

    # write manifest (unchanged)
    manifest = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "base": str(BASE),
        "cache_root": str(cache_root),
        "publish_root": str(publish_root),
        "schema": {
            "cache": "{cache}/{provider}/{species}/{assembly}/{raw|ready}/...",
            "publish": "{publish}/{provider}/{species}/genome.fa, genes{.gtf|.gff3}.gz",
        },
        "sources": results,
    }
    safe_makedirs(manifest_path.parent)
    tmp = manifest_path.with_suffix(manifest_path.suffix + ".part")
    tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    shutil.move(tmp, manifest_path)


def main():
    CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    PUBLISH_ROOT.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)

    catalog = load_catalog()
    run_once(CACHE_ROOT, PUBLISH_ROOT, MANIFEST_PATH, catalog)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
