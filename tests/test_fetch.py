import json

import pytest

from loader import fetch as fetch_mod
from loader.catalog import SourceFile

F = SourceFile(
    "helsinki", "trips", "hsl", "trips-2025-07", "https://example.invalid/x", ("2025-07",)
)


@pytest.fixture
def fake_download(monkeypatch):
    content = {"body": b"a,b\n1,2\n"}

    def _download(url, dest):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content["body"])

    monkeypatch.setattr(fetch_mod, "download", _download)
    return content


def test_lock_records_hash_and_size(tmp_path, fake_download):
    lock = fetch_mod.fetch([F], tmp_path / "raw", tmp_path / "lock.json")
    entry = lock["files"]["helsinki/trips-2025-07"]
    assert entry["bytes"] == 8
    assert entry["sha256"] == fetch_mod.sha256_of(tmp_path / "raw/helsinki/trips-2025-07.csv")
    assert json.loads((tmp_path / "lock.json").read_text())["files"] == lock["files"]


def test_a_file_replaced_at_the_source_is_refused(tmp_path, fake_download):
    fetch_mod.fetch([F], tmp_path / "raw", tmp_path / "lock.json")
    (tmp_path / "raw/helsinki/trips-2025-07.csv").unlink()
    fake_download["body"] = b"a,b\n1,3\n"
    with pytest.raises(fetch_mod.LockMismatch):
        fetch_mod.fetch([F], tmp_path / "raw", tmp_path / "lock.json")
    lock = fetch_mod.fetch([F], tmp_path / "raw", tmp_path / "lock.json", update_lock=True)
    assert lock["files"]["helsinki/trips-2025-07"]["sha256"] == fetch_mod.sha256_of(
        tmp_path / "raw/helsinki/trips-2025-07.csv"
    )


def test_a_file_matching_the_lock_is_not_downloaded_again(tmp_path, fake_download, monkeypatch):
    fetch_mod.fetch([F], tmp_path / "raw", tmp_path / "lock.json")
    monkeypatch.setattr(fetch_mod, "download", lambda *a: pytest.fail("downloaded again"))
    fetch_mod.fetch([F], tmp_path / "raw", tmp_path / "lock.json")


def test_an_interrupted_fetch_keeps_the_files_it_finished(tmp_path, monkeypatch):
    second = SourceFile(
        "helsinki", "trips", "hsl", "trips-2025-08", "https://example.invalid/y", ("2025-08",)
    )

    def _download(url, dest):
        if url.endswith("/y"):
            raise ConnectionError("cut")
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"a,b\n1,2\n")

    monkeypatch.setattr(fetch_mod, "download", _download)
    with pytest.raises(ConnectionError):
        fetch_mod.fetch([F, second], tmp_path / "raw", tmp_path / "lock.json")
    lock = json.loads((tmp_path / "lock.json").read_text())
    assert list(lock["files"]) == ["helsinki/trips-2025-07"]
