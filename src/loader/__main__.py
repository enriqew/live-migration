"""python -m loader {catalog,fetch,load} [--config conf/load.toml] [--reset] [--update-lock]"""

from __future__ import annotations

import argparse
import json
import tomllib
from pathlib import Path

from loader.catalog import Month, SourceFile, dublin_catalog, helsinki_catalog
from loader.fetch import fetch
from loader.load import Endpoints, load_dublin, load_helsinki

ROOT = Path(__file__).resolve().parents[2]


def catalog(config: dict) -> list[SourceFile]:
    d, h = config["dublin"], config["helsinki"]
    return dublin_catalog(Month.parse(d["first"]), Month.parse(d["last"])) + helsinki_catalog(
        Month.parse(h["first"]), Month.parse(h["last"])
    )


def from_lock(lock_path: Path) -> list[SourceFile]:
    entries = json.loads(lock_path.read_text(encoding="utf8"))["files"].values()
    fields = SourceFile.__dataclass_fields__
    return [
        SourceFile(**{k: (tuple(v) if k == "months" else v) for k, v in e.items() if k in fields})
        for e in entries
    ]


def main() -> None:
    p = argparse.ArgumentParser(prog="loader")
    p.add_argument("command", choices=["catalog", "fetch", "load"])
    p.add_argument("--config", default="conf/load.toml")
    p.add_argument("--reset", action="store_true", help="empty the legacy systems before loading")
    p.add_argument("--update-lock", action="store_true", help="accept files changed at the source")
    args = p.parse_args()

    config_path = ROOT / args.config
    config = tomllib.loads(config_path.read_text(encoding="utf8"))
    raw_dir = ROOT / config.get("raw_dir", "data/raw")
    lock_path = ROOT / config["lock"]

    if args.command == "catalog":
        for f in catalog(config):
            print(f"{f.system:9} {f.era:9} {f.name:28} {','.join(f.months)}")
    elif args.command == "fetch":
        lock = fetch(catalog(config), raw_dir, lock_path, update_lock=args.update_lock)
        size = sum(e["bytes"] for e in lock["files"].values())
        print(f"{len(lock['files'])} files, {size / 1e9:.2f} GB, locked in {lock_path.name}")
    else:
        files = from_lock(lock_path)
        ep = Endpoints()
        a = load_dublin([f for f in files if f.system == "dublin"], raw_dir, ep, args.reset)
        b = load_helsinki([f for f in files if f.system == "helsinki"], raw_dir, ep, args.reset)
        print(f"loaded {a:,} Dublin readings and {b:,} Helsinki trips")


if __name__ == "__main__":
    main()
