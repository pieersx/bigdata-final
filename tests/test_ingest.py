from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from tlc_pipeline.catalog import TLCFile
from tlc_pipeline.config import PipelineConfig, load_config
from tlc_pipeline.ingest import (
    IntegrityError,
    ManifestStore,
    download_file,
    download_zone_lookup,
    partial_path,
    sidecar_path,
)


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        body: bytes,
        headers: dict[str, str],
        *,
        url: str = "https://example.test/file",
    ) -> None:
        self.status_code = status_code
        self.body = body
        self.headers = headers
        self.url = url
        self.closed = False

    def iter_content(self, chunk_size: int) -> list[bytes]:
        return [
            self.body[index : index + chunk_size] for index in range(0, len(self.body), chunk_size)
        ]

    def close(self) -> None:
        self.closed = True


class DownloadSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.get_calls: list[tuple[str, dict[str, object]]] = []
        self.closed = False

    def get(self, url: str, **kwargs: object) -> FakeResponse:
        self.get_calls.append((url, kwargs))
        if not self.responses:
            raise AssertionError("Solicitud HTTP inesperada")
        return self.responses.pop(0)

    def close(self) -> None:
        self.closed = True


def parquet_entry(content: bytes) -> TLCFile:
    return TLCFile(
        service="yellow",
        year=2023,
        month=1,
        url="https://example.test/yellow_tripdata_2023-01.parquet",
        filename="yellow_tripdata_2023-01.parquet",
        size_bytes=len(content),
        available=True,
        etag='"v1"',
    )


def test_full_download_writes_checksum_sidecar_manifest_and_is_idempotent(
    tmp_path: Path,
) -> None:
    content = b"PAR1" + b"complete-payload" + b"PAR1"
    entry = parquet_entry(content)
    response = FakeResponse(200, content, {"Content-Length": str(len(content))})
    session = DownloadSession([response])
    manifest = ManifestStore(tmp_path / "_manifest.json")

    first = download_file(
        entry,
        tmp_path / "data",
        session=session,
        chunk_size_bytes=5,
        manifest=manifest,
    )
    second = download_file(
        entry,
        tmp_path / "data",
        session=DownloadSession([]),
        manifest=manifest,
    )

    expected_sha = hashlib.sha256(content).hexdigest()
    assert first.status == "VALIDATED"
    assert first.sha256 == expected_sha
    assert second.status == "SKIPPED"
    assert second.transferred_bytes == 0
    assert Path(first.path).read_bytes() == content
    metadata = json.loads(Path(first.sidecar_path).read_text(encoding="utf-8"))
    assert metadata["sha256"] == expected_sha
    assert metadata["content_length"] == len(content)
    assert manifest.entries()[0]["status"] == "SKIPPED"
    assert len(session.get_calls) == 1


def test_resume_uses_range_and_appends_only_remaining_bytes(tmp_path: Path) -> None:
    content = b"PAR1" + b"0123456789abcdef" + b"PAR1"
    entry = parquet_entry(content)
    destination_dir = tmp_path / "data"
    destination_dir.mkdir()
    destination = destination_dir / entry.filename
    offset = 9
    partial_path(destination).write_bytes(content[:offset])
    remaining = content[offset:]
    response = FakeResponse(
        206,
        remaining,
        {
            "Content-Length": str(len(remaining)),
            "Content-Range": f"bytes {offset}-{len(content) - 1}/{len(content)}",
        },
    )
    session = DownloadSession([response])

    result = download_file(entry, destination_dir, session=session, chunk_size_bytes=4)

    assert destination.read_bytes() == content
    assert result.resumed_from_bytes == offset
    assert result.transferred_bytes == len(remaining)
    assert session.get_calls[0][1]["headers"]["Range"] == f"bytes={offset}-"


def test_server_ignoring_range_restarts_instead_of_corrupting_file(tmp_path: Path) -> None:
    content = b"PAR1" + b"abcdefghijk" + b"PAR1"
    entry = parquet_entry(content)
    destination_dir = tmp_path / "data"
    destination_dir.mkdir()
    destination = destination_dir / entry.filename
    partial_path(destination).write_bytes(content[:6])
    response = FakeResponse(200, content, {"Content-Length": str(len(content))})

    result = download_file(entry, destination_dir, session=DownloadSession([response]))

    assert destination.read_bytes() == content
    assert result.resumed_from_bytes == 0
    assert result.transferred_bytes == len(content)


def test_cloudfront_throttle_is_retried_without_losing_partial_state(tmp_path: Path) -> None:
    content = b"PAR1" + b"retry-payload" + b"PAR1"
    entry = parquet_entry(content)
    blocked = FakeResponse(403, b"blocked", {"Content-Length": "7"})
    success = FakeResponse(200, content, {"Content-Length": str(len(content))})
    session = DownloadSession([blocked, success])

    result = download_file(
        entry,
        tmp_path / "data",
        session=session,
        throttle_retries=1,
        throttle_backoff_seconds=0,
    )

    assert result.status == "VALIDATED"
    assert len(session.get_calls) == 2
    assert blocked.closed is True


def test_content_length_and_parquet_magic_are_enforced(tmp_path: Path) -> None:
    content = b"PAR1" + b"payload" + b"PAR1"
    entry = parquet_entry(content)
    wrong_length = FakeResponse(200, content, {"Content-Length": str(len(content) + 1)})

    with pytest.raises(IntegrityError, match="Content-Length incorrecto"):
        download_file(entry, tmp_path / "length", session=DownloadSession([wrong_length]))

    bad_content = b"NOPE" + b"payload" + b"FAIL"
    bad_entry = parquet_entry(bad_content)
    bad_magic = FakeResponse(200, bad_content, {"Content-Length": str(len(bad_content))})
    with pytest.raises(IntegrityError, match="Magic bytes Parquet"):
        download_file(bad_entry, tmp_path / "magic", session=DownloadSession([bad_magic]))


def temporary_config(tmp_path: Path, zone_url: str) -> PipelineConfig:
    base = load_config(env={"TLC_DATA_ROOT": str(tmp_path / "data")})
    raw = base.as_dict()
    raw["source"]["zone_lookup_url"] = zone_url
    raw["audit"]["local_jsonl"] = str(tmp_path / "audit.jsonl")
    return PipelineConfig(raw=raw, config_file=base.config_file)


def test_zone_lookup_uses_probe_fallback_and_idempotent_bronze_reference(
    tmp_path: Path,
) -> None:
    url = "https://example.test/taxi_zone_lookup.csv"
    content = b"LocationID,Borough\n1,EWR\n"

    class ZoneSession(DownloadSession):
        def head(self, url: str, **_: object) -> FakeResponse:
            return FakeResponse(405, b"", {}, url=url)

    range_response = FakeResponse(
        206,
        content[:1],
        {"Content-Length": "1", "Content-Range": f"bytes 0-0/{len(content)}"},
        url=url,
    )
    full_response = FakeResponse(
        200,
        content,
        {"Content-Length": str(len(content))},
        url=url,
    )
    session = ZoneSession([range_response, full_response])
    config = temporary_config(tmp_path, url)

    first = download_zone_lookup(config, session=session)
    second = download_zone_lookup(config, session=ZoneSession([range_response]))

    assert Path(first.path) == tmp_path / "data" / "bronze" / "reference" / "taxi_zone_lookup.csv"
    assert Path(first.path).read_bytes() == content
    assert first.status == "VALIDATED"
    assert second.status == "SKIPPED"
    assert sidecar_path(Path(first.path)).is_file()
