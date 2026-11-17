"""Download the catalog's files and pin them in a lock.

`sources.lock.json` is committed. It records, per file, where it came from, its size and its
SHA-256. A second fetch checks every file against the lock: a publisher replacing a file under the
same URL is an error, not a silent change of the data a run is built on. `--update-lock` accepts
the new version deliberately.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import urllib.request
from dataclasses import asdict
from pathlib import Path

from loader.catalog import USER_AGENT, SourceFile

CHUNK = 1 << 20


class LockMismatch(RuntimeError):
    pass


def local_path(raw_dir: Path, f: SourceFile) -> Path:
    return raw_dir / f.system / f"{f.name}.csv"


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(CHUNK):
            h.update(chunk)
    return h.hexdigest()


def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_suffix(dest.suffix + ".part")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=600) as resp, part.open("wb") as out:
        shutil.copyfileobj(resp, out, CHUNK)
    part.replace(dest)  # never leave a truncated file under the final name


def fetch(
    files: list[SourceFile], raw_dir: Path, lock_path: Path, update_lock: bool = False
) -> dict:
    lock = json.loads(lock_path.read_text(encoding="utf8")) if lock_path.exists() else {}
    entries = lock.get("files", {})
    new_entries = {}
    for f in files:
        key = f"{f.system}/{f.name}"
        path = local_path(raw_dir, f)
        pinned = entries.get(key)
        if path.exists() and pinned and sha256_of(path) == pinned["sha256"]:
            new_entries[key] = pinned
            continue
        download(f.url, path)
        digest, size = sha256_of(path), path.stat().st_size
        if pinned and pinned["sha256"] != digest and not update_lock:
            raise LockMismatch(
                f"{key} changed at the source: locked {pinned['sha256'][:12]}, got {digest[:12]}. "
                "Rerun with --update-lock to accept it."
            )
        if size == 0:
            raise RuntimeError(f"{key} downloaded empty from {f.url}")
        new_entries[key] = {**asdict(f), "months": list(f.months), "bytes": size, "sha256": digest}
        print(f"fetched {key}: {size:,} bytes")
    lock = {"files": dict(sorted(new_entries.items()))}
    lock_path.write_text(json.dumps(lock, indent=2, ensure_ascii=False) + "\n", encoding="utf8")
    return lock
