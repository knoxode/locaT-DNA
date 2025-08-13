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
    decompress_fasta: bool = True

# ---------- GenomeCache ----------

class GenomeCache:
    """
    Single-process or multi-process safe cache manager.
    Filesystem is the source of truth; SQLite stores inventory/state.
    Publish uses atomic replace so readers never see partial files.
    """

    def __init__(self, base: Path | str = "/data/genome_cache", user_agent: str = "locaT-DNA-cache/0.1"):
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
                SELECT provider, species, assembly, genome_path
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
            SELECT genome_path, genome_fai, anno_gz, anno_plain, assembly
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
            return {"genome": row[0], "fai": row[1], "anno_gz": row[2], "anno": row[3], "assembly": row[4]}

    def ensure(self, spec: SourceSpec) -> Dict[str, Optional[str]]:
        """
        Ensure the given source is downloaded, processed, and published.
        Returns published paths.
        """
        g_id = self._upsert_genome(spec)
        self._process_and_publish(g_id, spec)
        # return paths
        with self._conn() as c:
            row = c.execute("""
                SELECT genome_path, genome_fai, anno_gz, anno_plain
                FROM genomes WHERE id=?
            """, (g_id,)).fetchone()
        return {"genome": row[0], "fai": row[1], "anno_gz": row[2], "anno": row[3]}

    def refresh_from_sources(self, sources_file: Optional[Path | str] = None) -> None:
        """
        Load sources.yaml and ensure each entry. YAML structure:
        sources:
          - provider: ensemblplants
            species: Arabidopsis_thaliana
            assembly: TAIR10
            fasta_url: https://...
            anno_url: https://...
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
                decompress_fasta=bool(d.get("decompress_fasta", True)),
            )
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
                -- paths (absolute)
                raw_fa_gz TEXT,
                raw_anno_gz TEXT,
                ready_fa TEXT,
                ready_fai TEXT,
                ready_anno_gz TEXT,
                ready_anno_plain TEXT,
                genome_path TEXT,
                genome_fai TEXT,
                anno_gz TEXT,
                anno_plain TEXT,
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
    def _layout(self, s: SourceSpec) -> Dict[str, Path]:
        base = self.cache_root / s.provider / s.species / s.assembly
        raw = base / "raw"
        ready = base / "ready"
        raw.mkdir(parents=True, exist_ok=True)
        ready.mkdir(parents=True, exist_ok=True)

        ext = self._anno_ext(s.anno_url) if s.anno_url else None
        return {
            "base": base,
            "raw_fa_gz": raw / "genome.fa.gz",
            "raw_anno_gz": (raw / f"genes{ext}.gz") if ext else None,
            "ready_fa": ready / "genome.fa",
            "ready_fai": ready / "genome.fa.fai",
            "ready_anno_gz": (ready / f"genes{ext}.gz") if ext else None,
            "ready_anno_plain": (ready / f"genes{ext}") if ext else None,
            "pub_dir": self.publish_root / s.provider / s.species,
        }

    # processing pipeline
    def _process_and_publish(self, g_id: int, s: SourceSpec) -> None:
        paths = self._layout(s)

        with self._file_lock(paths["base"] / ".lock"):
            self._set_state(g_id, "downloading")

            # FASTA .fa.gz
            etag_fa, lastmod_fa, changed_fa = self._download_with_cache(s.fasta_url, paths["raw_fa_gz"])
            # Decompress if needed
            if s.decompress_fasta and (changed_fa or not paths["ready_fa"].exists()):
                self._decompress_any(paths["raw_fa_gz"], paths["ready_fa"])

            # Index
            self._run(["samtools", "faidx", str(paths["ready_fa"])])

            # Annotation (optional)
            etag_ann = lastmod_ann = None
            if s.anno_url and paths["raw_anno_gz"] is not None:
                etag_ann, lastmod_ann, changed_ann = self._download_with_cache(s.anno_url, paths["raw_anno_gz"])
                # ready gz = copy (no symlink)
                self._copy_atomic(paths["raw_anno_gz"], paths["ready_anno_gz"])
                # ready plain
                if changed_ann or not paths["ready_anno_plain"].exists():
                    self._decompress_any(paths["raw_anno_gz"], paths["ready_anno_plain"])

            # Publish atomically
            pub = paths["pub_dir"]; pub.mkdir(parents=True, exist_ok=True)
            genome_dst = pub / "genome.fa"
            self._copy_atomic(paths["ready_fa"], genome_dst)
            fai_dst = pub / "genome.fa.fai"
            self._copy_atomic(paths["ready_fai"], fai_dst) if paths["ready_fai"].exists() else None

            anno_gz_dst = anno_plain_dst = None
            if s.anno_url:
                ext = self._anno_ext(s.anno_url)
                anno_gz_dst = pub / f"genes{ext}.gz"
                anno_plain_dst = pub / f"genes{ext}"
                self._copy_atomic(paths["ready_anno_gz"], anno_gz_dst)
                self._copy_atomic(paths["ready_anno_plain"], anno_plain_dst)

            # DB update -> published
            with self._conn() as c:
                c.execute("""
                    UPDATE genomes SET
                        state='published', last_error=NULL,
                        raw_fa_gz=?, raw_anno_gz=?,
                        ready_fa=?, ready_fai=?,
                        ready_anno_gz=?, ready_anno_plain=?,
                        genome_path=?, genome_fai=?,
                        anno_gz=?, anno_plain=?,
                        etag_fa=?, lastmod_fa=?, etag_anno=?, lastmod_anno=?,
                        updated_at=strftime('%s','now')
                    WHERE id=?
                """, (
                    str(paths["raw_fa_gz"]),
                    str(paths["raw_anno_gz"]) if s.anno_url else None,
                    str(paths["ready_fa"]),
                    str(paths["ready_fai"]),
                    str(paths["ready_anno_gz"]) if s.anno_url else None,
                    str(paths["ready_anno_plain"]) if s.anno_url else None,
                    str(genome_dst),
                    str(fai_dst) if (paths["ready_fai"].exists()) else None,
                    str(anno_gz_dst) if s.anno_url else None,
                    str(anno_plain_dst) if s.anno_url else None,
                    etag_fa, lastmod_fa, etag_ann, lastmod_ann,
                    g_id
                ))

            # refresh public index.json
            self._rewrite_public_index()

    # utilities

    def _set_state(self, g_id: int, state: str, err: Optional[str] = None):
        with self._conn() as c:
            c.execute("UPDATE genomes SET state=?, last_error=?, updated_at=strftime('%s','now') WHERE id=?",
                      (state, err, g_id))

    @staticmethod
    def _anno_ext(url: Optional[str]) -> str:
        if not url:
            return ".gtf"
        u = url.lower()
        for ext in (".gff3.gz", ".gff.gz", ".gtf.gz", ".gff3", ".gff", ".gtf"):
            if u.endswith(ext):
                return "." + ext.lstrip(".").split(".")[0]
        return ".gtf"

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
            # atomic move
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
        # ensure written to disk before replace
        with open(tmp, "rb") as f:
            os.fsync(f.fileno())
        os.replace(tmp, dst)

    def _rewrite_public_index(self) -> None:
        with self._conn() as c:
            rows = c.execute("""
                SELECT provider, species, assembly, genome_path, genome_fai, anno_gz, anno_plain, updated_at
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
                "genome_path": r["genome_path"],
                "genome_fai": r["genome_fai"],
                "anno_gz_path": r["anno_gz"],
                "anno_plain_path": r["anno_plain"],
                "mtime": int(r["updated_at"]),
            } for r in rows],
            "version": 1,
        }
        tmp = self.index_json.with_suffix(self.index_json.suffix + ".part")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
        os.replace(tmp, self.index_json)
