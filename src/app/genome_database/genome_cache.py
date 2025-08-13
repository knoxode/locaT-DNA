# src/app/genome_database/genome_cache.py
from __future__ import annotations

import contextlib
import gzip, bz2, lzma
import json
import os
import shutil
import sqlite3
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
try:
    import yaml  # optional; only needed for refresh_from_sources()
except Exception:
    yaml = None


# ---------- Data model ----------

@dataclass(frozen=True)
class SourceSpec:
    provider: str
    species: str
    assembly: str
    fasta_url: str
    anno_url: Optional[str] = None
    # For JBrowse we prefer to keep FASTA bgzip-compressed
    decompress_fasta: bool = False  # default false now (bgzip + index instead)


# ---------- GenomeCache ----------

class GenomeCache:
    """
    Cache manager that produces JBrowse2-ready artifacts.
    Filesystem is the source of truth; SQLite stores inventory/state.
    Publishing uses atomic replace so readers never see partial files.
    """

    def __init__(self, base: Path | str = "/data/genome_cache", user_agent: str = "locaT-DNA-cache/0.2-jb"):
        self.base = Path(base)
        self.cache_root = self.base / "cache"
        self.publish_root = self.base / "publish"
        self.meta_root = self.base / "meta"
        self.sources_path = self.base / "sources.yaml"  # can be overridden in calls
        self.index_json = self.base / "index.json"
        self.db_path = self.meta_root / "inventory.sqlite"

        # ensure dirs
        for p in (self.cache_root, self.publish_root, self.meta_root):
            p.mkdir(parents=True, exist_ok=True)

        # HTTP session
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})

        # DB
        self._init_db()

        self.refresh_from_sources(self.sources_path)  # load initial sources

    # ---------- Public API ----------

    def list_genomes(self) -> List[Tuple[str, str, str, str]]:
        """
        Return list of published genomes: (provider, species, assembly, genome_path)
        """
        with self._conn() as c:
            cur = c.execute("""
                SELECT provider, species, assembly, genome_fa_gz
                FROM genomes
                WHERE state='published'
                ORDER BY provider, species, assembly
            """)
            return [(r[0], r[1], r[2], r[3]) for r in cur.fetchall()]

    def get_paths(self, provider: str, species: str, assembly: Optional[str] = None) -> Dict[str, Optional[str]]:
        """
        Return published paths for a genome (latest if multiple assemblies and assembly not provided).
        """
        q = """
            SELECT genome_fa_gz, genome_fai, genome_gzi, anno_gz, anno_tbi, assembly,
                   jbrowse_assembly_json, jbrowse_tracks_json
            FROM genomes
            WHERE provider=? AND species=? AND state='published'
        """
        params = [provider, species]
        if assembly:
            q += " AND assembly=?"
            params.append(assembly)
        q += " ORDER BY updated_at DESC LIMIT 1"

        with self._conn() as c:
            row = c.execute(q, params).fetchone()
            if not row:
                raise KeyError(f"Not published: {provider}/{species}{('/'+assembly) if assembly else ''}")
            return {
                "genome_fa_gz": row[0],
                "genome_fai": row[1],
                "genome_gzi": row[2],
                "anno_gz": row[3],
                "anno_tbi": row[4],
                "assembly": row[5],
                "jbrowse_assembly_json": row[6],
                "jbrowse_tracks_json": row[7],
            }

    def ensure(self, spec: SourceSpec) -> Dict[str, Optional[str]]:
        """
        Ensure the given source is downloaded, processed, and published.
        Returns published paths.
        """
        g_id = self._upsert_genome(spec)
        self._process_and_publish(s=g_id, spec=spec)
        # return paths
        with self._conn() as c:
            row = c.execute("""
                SELECT genome_fa_gz, genome_fai, genome_gzi, anno_gz, anno_tbi,
                       jbrowse_assembly_json, jbrowse_tracks_json
                FROM genomes WHERE id=?
            """, (g_id,)).fetchone()
        return {
            "genome_fa_gz": row[0],
            "genome_fai": row[1],
            "genome_gzi": row[2],
            "anno_gz": row[3],
            "anno_tbi": row[4],
            "jbrowse_assembly_json": row[5],
            "jbrowse_tracks_json": row[6],
        }

    def refresh_from_sources(self, sources_file: Optional[Path | str] = None) -> None:
        """
        Load sources.yaml and ensure each entry. YAML structure:
        sources:
          - provider: ensemblplants
            species: Arabidopsis_thaliana
            assembly: TAIR10
            fasta_url: https://...
            anno_url: https://... (GFF3/GFF only; GTF is refused)
        """
        if sources_file is None:
            sources_file = self.sources_path
        sources_file = Path(sources_file)
        if not sources_file.exists():
            raise FileNotFoundError(f"sources.yaml not found: {sources_file}")
        if yaml is None:
            raise RuntimeError("PyYAML not installed; needed for refresh_from_sources()")

        cfg = yaml.safe_load(sources_file.read_text()) or {}
        items = cfg.get("sources") or []
        for d in items:
            spec = SourceSpec(
                provider=d["provider"],
                species=d["species"],
                assembly=d["assembly"],
                fasta_url=d["fasta_url"],
                anno_url=d.get("anno_url"),
                decompress_fasta=bool(d.get("decompress_fasta", False)),
            )
            self._assert_not_gtf_url(spec.anno_url)  # refuse GTF upfront
            self.ensure(spec)

    # ---------- Internals ----------

    # DB & schema
    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._conn() as c:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS genomes (
                id INTEGER PRIMARY KEY,
                provider TEXT NOT NULL,
                species TEXT NOT NULL,
                assembly TEXT NOT NULL,
                fasta_url TEXT NOT NULL,
                anno_url TEXT,
                state TEXT NOT NULL DEFAULT 'missing',  -- missing/downloading/ready/published/error
                last_error TEXT,
                -- paths (absolute, "ready" = in cache; "genome_*" = published)
                raw_fa TEXT,          -- as downloaded (could be .fa or .fa.gz)
                raw_anno TEXT,        -- as downloaded (GFF3/GFF, possibly compressed)
                ready_fa_gz TEXT,     -- bgzip FASTA
                ready_fai TEXT,       -- FASTA .fai (for .fa.gz)
                ready_gzi TEXT,       -- FASTA .gzi
                ready_anno_gz TEXT,   -- bgzip annotation (gff3.gz)
                ready_anno_tbi TEXT,  -- tabix index for annotation
                genome_fa_gz TEXT,
                genome_fai TEXT,
                genome_gzi TEXT,
                anno_gz TEXT,
                anno_tbi TEXT,
                jbrowse_assembly_json TEXT,
                jbrowse_tracks_json TEXT,
                -- caching headers
                etag_fa TEXT,
                lastmod_fa TEXT,
                etag_anno TEXT,
                lastmod_anno TEXT,
                updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                UNIQUE(provider, species, assembly)
            );
            """)

    def _upsert_genome(self, s: SourceSpec) -> int:
        with self._conn() as c:
            c.execute("""
                INSERT INTO genomes(provider, species, assembly, fasta_url, anno_url, state)
                VALUES(?,?,?,?,?,'missing')
                ON CONFLICT(provider, species, assembly) DO UPDATE SET
                  fasta_url=excluded.fasta_url,
                  anno_url=excluded.anno_url
            """, (s.provider, s.species, s.assembly, s.fasta_url, s.anno_url))
            row = c.execute("SELECT id FROM genomes WHERE provider=? AND species=? AND assembly=?",
                            (s.provider, s.species, s.assembly)).fetchone()
            return int(row[0])

    # layout
    def _url_basename(self, url: str) -> str:
        return Path(urlparse(url).path).name or "annotation.gff3"

    def _layout(self, s: SourceSpec) -> Dict[str, Path]:
        base = self.cache_root / s.provider / s.species / s.assembly
        raw = base / "raw"
        ready = base / "ready"
        raw.mkdir(parents=True, exist_ok=True)
        ready.mkdir(parents=True, exist_ok=True)

        # keep raw annotation filename from URL (preserves .gff/.gff3[.gz])
        raw_anno = (raw / self._url_basename(s.anno_url)) if s.anno_url else None
        # publish/ready annotation is always GFF3
        return {
            "base": base,
            "raw_fa": raw / ("genome.fa.gz" if str(s.fasta_url).lower().endswith(".gz") else "genome.fa"),
            "raw_anno": raw_anno,
            "ready_fa_gz": ready / "genome.fa.gz",
            "ready_fai": ready / "genome.fa.gz.fai",
            "ready_gzi": ready / "genome.fa.gz.gzi",
            "ready_anno_gz": (ready / "genes.gff3.gz") if s.anno_url else None,
            "ready_anno_tbi": (ready / "genes.gff3.gz.tbi") if s.anno_url else None,
            "pub_dir": self.publish_root / s.provider / s.species / s.assembly,
        }

    # processing pipeline
    def _process_and_publish(self, s: int, spec: SourceSpec) -> None:
        paths = self._layout(spec)

        with self._file_lock(paths["base"] / ".lock"):
            self._set_state(s, "downloading")

            # -------- FASTA --------
            etag_fa, lastmod_fa, changed_fa = self._download_with_cache(spec.fasta_url, paths["raw_fa"])
            # Ensure bgzip-compressed FASTA in ready path
            # Cases:
            #   - raw is uncompressed .fa  -> bgzip -> ready_fa_gz
            #   - raw is gzip (.fa.gz)     -> re-bgzip to ensure block gzip (safe), or copy then normalize
            if not paths["ready_fa_gz"].exists() or changed_fa:
                if str(paths["raw_fa"]).endswith(".gz"):
                    # normalize gzip to bgzip
                    self._bgzip_normalize(src=paths["raw_fa"], dst=paths["ready_fa_gz"])
                else:
                    self._bgzip_from_plain(src=paths["raw_fa"], dst=paths["ready_fa_gz"])

            # samtools faidx on bgzip FASTA -> creates .fai and .gzi alongside
            self._run(["samtools", "faidx", str(paths["ready_fa_gz"])])
            if not paths["ready_gzi"].exists():
                self._run(["bgzip", "-r", str(paths["ready_fa_gz"])])

            # -------- Annotation (optional, GFF/GFF3 only) --------
            etag_ann = lastmod_ann = None
            anno_gz = anno_tbi = None
            if spec.anno_url and paths["raw_anno"] is not None:
                self._assert_not_gtf_url(spec.anno_url)  # defense in depth
                etag_ann, lastmod_ann, changed_ann = self._download_with_cache(spec.anno_url, paths["raw_anno"])
                if (changed_ann or not paths["ready_anno_gz"].exists() or not paths["ready_anno_tbi"].exists()):
                    # produce bgzip+tabix with coordinate sort while preserving headers
                    self._prepare_annotation_for_tabix(
                        src=paths["raw_anno"],
                        dst_gz=paths["ready_anno_gz"],
                        dst_tbi=paths["ready_anno_tbi"]
                    )
                anno_gz  = paths["ready_anno_gz"]
                anno_tbi = paths["ready_anno_tbi"]

            # -------- Publish atomically --------
            pub = paths["pub_dir"]; pub.mkdir(parents=True, exist_ok=True)

            genome_fa_gz = pub / "genome.fa.gz"
            genome_fai   = pub / "genome.fa.gz.fai"
            genome_gzi   = pub / "genome.fa.gz.gzi"
            self._copy_atomic(paths["ready_fa_gz"], genome_fa_gz)
            self._copy_atomic(paths["ready_fai"],   genome_fai) if paths["ready_fai"].exists() else None
            self._copy_atomic(paths["ready_gzi"],   genome_gzi) if paths["ready_gzi"].exists() else None

            if anno_gz and anno_tbi:
                out_anno_gz  = pub / "genes.gff3.gz"
                out_anno_tbi = pub / "genes.gff3.gz.tbi"
                self._copy_atomic(anno_gz, out_anno_gz)
                self._copy_atomic(anno_tbi, out_anno_tbi)

            # -------- JBrowse helper configs --------
            jbrowse_assembly_json = pub / "jbrowse_assembly.json"
            jbrowse_tracks_json   = pub / "jbrowse_tracks.json"
            self._write_jbrowse_snippets(
                assembly_json_path=jbrowse_assembly_json,
                tracks_json_path=jbrowse_tracks_json,
                provider=spec.provider,
                species=spec.species,
                assembly=spec.assembly,
                genome_fa_gz=genome_fa_gz,
                genome_fai=genome_fai,
                genome_gzi=genome_gzi,
                anno_gz=(pub / "genes.gff3.gz") if (anno_gz and anno_tbi) else None,
                anno_tbi=(pub / "genes.gff3.gz.tbi") if (anno_gz and anno_tbi) else None,
            )

            # -------- DB update -> published --------
            with self._conn() as c:
                c.execute("""
                    UPDATE genomes SET
                        state='published', last_error=NULL,
                        raw_fa=?, raw_anno=?,
                        ready_fa_gz=?, ready_fai=?, ready_gzi=?,
                        ready_anno_gz=?, ready_anno_tbi=?,
                        genome_fa_gz=?, genome_fai=?, genome_gzi=?,
                        anno_gz=?, anno_tbi=?,
                        jbrowse_assembly_json=?, jbrowse_tracks_json=?,
                        etag_fa=?, lastmod_fa=?, etag_anno=?, lastmod_anno=?,
                        updated_at=strftime('%s','now')
                    WHERE id=?
                """, (
                    str(paths["raw_fa"]),
                    str(paths["raw_anno"]) if spec.anno_url else None,
                    str(paths["ready_fa_gz"]),
                    str(paths["ready_fai"]),
                    str(paths["ready_gzi"]),
                    str(paths["ready_anno_gz"]) if spec.anno_url else None,
                    str(paths["ready_anno_tbi"]) if spec.anno_url else None,
                    str(genome_fa_gz),
                    str(genome_fai) if paths["ready_fai"].exists() else None,
                    str(genome_gzi) if paths["ready_gzi"].exists() else None,
                    str(pub / "genes.gff3.gz") if (anno_gz and anno_tbi) else None,
                    str(pub / "genes.gff3.gz.tbi") if (anno_gz and anno_tbi) else None,
                    str(jbrowse_assembly_json),
                    str(jbrowse_tracks_json),
                    etag_fa, lastmod_fa, etag_ann, lastmod_ann,
                    s
                ))

            # refresh public index.json
            self._rewrite_public_index()

    # ---------- utilities ----------

    @staticmethod
    def _assert_not_gtf_url(url: Optional[str]) -> None:
        if not url:
            return
        u = url.lower()
        if u.endswith(".gtf") or u.endswith(".gtf.gz"):
            raise ValueError(
                f"GTF is not supported by GenomeCache. Please provide a GFF3/GFF URL instead: {url}"
            )

    def _set_state(self, g_id: int, state: str, err: Optional[str] = None):
        with self._conn() as c:
            c.execute("UPDATE genomes SET state=?, last_error=?, updated_at=strftime('%s','now') WHERE id=?",
                      (state, err, g_id))

    @staticmethod
    def _anno_ext(url: Optional[str]) -> str:
        """
        Normalized publish extension for annotations: always GFF3.
        Refuses GTF input.
        """
        if not url:
            return ".gff3"
        u = url.lower()
        if u.endswith(".gtf") or u.endswith(".gtf.gz"):
            raise ValueError(
                f"GTF is not supported by GenomeCache. Please provide a GFF3/GFF URL instead: {url}"
            )
        # Accept .gff and .gff3 inputs; we publish as .gff3
        return ".gff3"

    @contextlib.contextmanager
    def _file_lock(self, lock_path: Path):
        """
        Simple exclusive lock using O_EXCL file creation.
        Blocks (with retry) until acquired.
        """
        lockfile = lock_path.with_suffix(".lock")
        while True:
            try:
                fd = os.open(str(lockfile), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, str(os.getpid()).encode())
                os.close(fd)
                break
            except FileExistsError:
                time.sleep(0.2)
        try:
            yield
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(lockfile)

    def _download_with_cache(self, url: str, target: Path, timeout: int = 600) -> Tuple[Optional[str], Optional[str], bool]:
        target.parent.mkdir(parents=True, exist_ok=True)
        etag_file = target.with_suffix(target.suffix + ".etag")
        lm_file = target.with_suffix(target.suffix + ".lastmod")
        headers: Dict[str, str] = {}
        if etag_file.exists():
            headers["If-None-Match"] = etag_file.read_text().strip()
        if lm_file.exists():
            headers["If-Modified-Since"] = lm_file.read_text().strip()

        with self.session.get(url, stream=True, headers=headers, timeout=timeout, allow_redirects=True) as r:
            if r.status_code == 304 and target.exists():
                return etag_file.read_text().strip() if etag_file.exists() else None, \
                       lm_file.read_text().strip() if lm_file.exists() else None, False
            r.raise_for_status()
            tmp = target.with_suffix(target.suffix + ".part")
            with open(tmp, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        fh.write(chunk)
            os.replace(tmp, target)

            etag = r.headers.get("ETag"); lastmod = r.headers.get("Last-Modified")
            if etag: etag_file.write_text(etag)
            if lastmod: lm_file.write_text(lastmod)
            return etag, lastmod, True

    @staticmethod
    def _sniff_compression(path: Path) -> str:
        with open(path, "rb") as fh:
            head = fh.read(6)
        if head.startswith(b"\x1f\x8b"): return "gzip"
        if head.startswith(b"BZh"):      return "bz2"
        if head.startswith(b"\xfd7zXZ\x00"): return "xz"
        return "plain"

    def _bgzip_from_plain(self, src: Path, dst: Path) -> None:
        tmp = dst.with_suffix(".part")
        # stream through bgzip
        with open(src, "rb") as fin, open(tmp, "wb") as fout:
            p = subprocess.Popen(["bgzip", "-c"], stdin=fin, stdout=fout)
            p.wait()
            if p.returncode != 0:
                raise RuntimeError("bgzip failed on FASTA/annotation")
            fout.flush(); os.fsync(fout.fileno())
        os.replace(tmp, dst)

    def _bgzip_normalize(self, src: Path, dst: Path) -> None:
        """
        Recompress arbitrary gzip as bgzip so we can tabix/faidx properly.
        """
        if src.resolve() == dst.resolve():
            # in-place normalize
            self._run(["bgzip", "-f", str(src)])
            return
        tmp_plain = dst.with_suffix(".tmp.dec")
        self._decompress_any(src, tmp_plain)
        try:
            self._bgzip_from_plain(tmp_plain, dst)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.remove(tmp_plain)

    def _decompress_any(self, src: Path, dst: Path) -> None:
        def _open(p: Path):
            kind = self._sniff_compression(p)
            if kind == "gzip": return gzip.open(p, "rb")
            if kind == "bz2":  return bz2.open(p, "rb")
            if kind == "xz":   return lzma.open(p, "rb")
            return open(p, "rb")

        tmp = dst.with_suffix(dst.suffix + ".part")
        with _open(src) as fin, open(tmp, "wb") as fout:
            shutil.copyfileobj(fin, fout)
            fout.flush(); os.fsync(fout.fileno())
        os.replace(tmp, dst)

    @staticmethod
    def _run(cmd: List[str]) -> None:
        subprocess.run(cmd, check=True)

    @staticmethod
    def _copy_atomic(src: Path, dst: Path) -> None:
        tmp = dst.with_suffix(dst.suffix + ".part")
        shutil.copy2(src, tmp)
        with open(tmp, "rb") as f:
            os.fsync(f.fileno())
        os.replace(tmp, dst)

    def _prepare_annotation_for_tabix(self, src: Path, dst_gz: Path, dst_tbi: Path) -> None:
        """
        Produce bgzip+tabix for GFF/GFF3:
          - accept plain or compressed input (gz/bz2/xz)
          - preserve header lines (#...)
          - coordinate sort non-header lines by (seqid, start)
          - bgzip to dst_gz
          - tabix -p gff -> dst_tbi
        """
        tmp_plain  = dst_gz.with_suffix(".plain")
        tmp_sorted = dst_gz.with_suffix(".sorted")
        tmp_header = dst_gz.with_suffix(".hdr")
        tmp_body   = dst_gz.with_suffix(".body")

        # 1) ensure plain text input
        self._decompress_any(src, tmp_plain)

        # 2) split header/body
        with open(tmp_plain, "rt", encoding="utf-8", errors="ignore") as fin, \
             open(tmp_header, "wt", encoding="utf-8") as fh, \
             open(tmp_body,   "wt", encoding="utf-8") as fb:
            for line in fin:
                (fh if line.startswith("#") else fb).write(line)

        # 3) sort body; concat header + sorted body
        subprocess.run(
            ["bash", "-lc", f"cat {tmp_header} > {tmp_sorted} && "
                            f"LC_ALL=C sort -t $'\\t' -k1,1 -k4,4n {tmp_body} >> {tmp_sorted}"],
            check=True
        )

        # 4) bgzip + tabix
        self._bgzip_from_plain(tmp_sorted, dst_gz)
        self._run(["tabix", "-f", "-p", "gff", str(dst_gz)])

        # 5) cleanup
        for p in (tmp_plain, tmp_sorted, tmp_header, tmp_body):
            with contextlib.suppress(FileNotFoundError):
                os.remove(p)

    def _write_jbrowse_snippets(
        self,
        assembly_json_path: Path,
        tracks_json_path: Path,
        provider: str,
        species: str,
        assembly: str,
        genome_fa_gz: Path,
        genome_fai: Path,
        genome_gzi: Path,
        anno_gz: Optional[Path],
        anno_tbi: Optional[Path],
    ) -> None:
        """
        Emit small JSON files you can import into JBrowse 2 or into a React Linear Genome View.
        Paths are emitted as relative-to-their-location (same dir), which makes hosting easy.
        """
        assembly_conf = {
            "name": f"{species} {assembly}",
            "sequence": {
                "type": "ReferenceSequenceTrack",
                "trackId": "refseq",
                "adapter": {
                    "type": "BgzipFastaAdapter",
                    "fastaLocation": {"uri": "genome.fa.gz"},
                    "faiLocation": {"uri": "genome.fa.gz.fai"},
                    "gziLocation": {"uri": "genome.fa.gz.gzi"},
                },
            },
        }

        tracks = []
        if anno_gz and anno_tbi:
            tracks.append({
                "type": "FeatureTrack",
                "trackId": "genes",
                "name": "Genes",
                "assemblyNames": [f"{species} {assembly}"],
                "adapter": {
                    "type": "Gff3TabixAdapter",
                    "gffGzLocation": {"uri": anno_gz.name},
                    "index": {"location": {"uri": anno_tbi.name}},
                },
                "category": ["Annotations"],
                "renderer": {"type": "SvgFeatureRenderer"},
            })

        assembly_json_path.write_text(json.dumps(assembly_conf, indent=2))
        tracks_json_path.write_text(json.dumps(tracks, indent=2))

    def _rewrite_public_index(self) -> None:
        with self._conn() as c:
            rows = c.execute("""
                SELECT provider, species, assembly,
                       genome_fa_gz, genome_fai, genome_gzi,
                       anno_gz, anno_tbi, updated_at,
                       jbrowse_assembly_json, jbrowse_tracks_json
                FROM genomes WHERE state='published'
                ORDER BY provider, species, assembly
            """).fetchall()
        payload = {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "publish_root": str(self.publish_root),
            "entries": [{
                "provider": r["provider"],
                "species": r["species"],
                "assembly": r["assembly"],
                "genome_fa_gz": r["genome_fa_gz"],
                "genome_fai": r["genome_fai"],
                "genome_gzi": r["genome_gzi"],
                "anno_gz": r["anno_gz"],
                "anno_tbi": r["anno_tbi"],
                "jbrowse_assembly_json": r["jbrowse_assembly_json"],
                "jbrowse_tracks_json": r["jbrowse_tracks_json"],
                "mtime": int(r["updated_at"]),
            } for r in rows],
            "version": 2,
        }
        tmp = self.index_json.with_suffix(self.index_json.suffix + ".part")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
        os.replace(tmp, self.index_json)
