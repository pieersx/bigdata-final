from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pytest

from tlc_pipeline.catalog import (
    CatalogCompletenessError,
    discover_catalog,
    expected_catalog,
    load_catalog,
    probe_url,
    save_catalog,
)
from tlc_pipeline.config import PipelineConfig, load_config


class FakeResponse:
    def __init__(self, status_code: int, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self.headers = headers or {}
        self.closed = False

    def close(self) -> None:
        self.closed = True


class CatalogSession:
    def __init__(self, available: set[str], *, size: int = 123) -> None:
        self.available = available
        self.size = size
        self.calls: defaultdict[str, int] = defaultdict(int)
        self.closed = False

    def head(self, url: str, **_: object) -> FakeResponse:
        self.calls[f"HEAD {url}"] += 1
        if url in self.available:
            return FakeResponse(
                200,
                {
                    "Content-Length": str(self.size),
                    "ETag": '"abc"',
                    "Last-Modified": "Wed, 01 Jul 2026 00:00:00 GMT",
                },
            )
        return FakeResponse(404, {"Content-Length": "0"})

    def get(self, url: str, **_: object) -> FakeResponse:
        self.calls[f"GET {url}"] += 1
        if url in self.available:
            return FakeResponse(
                206,
                {"Content-Length": "1", "Content-Range": f"bytes 0-0/{self.size}"},
            )
        return FakeResponse(404, {})

    def close(self) -> None:
        self.closed = True


def small_config(tmp_path: Path) -> PipelineConfig:
    base = load_config(env={"TLC_DATA_ROOT": str(tmp_path / "data")})
    raw = base.as_dict()
    raw["source"]["services"] = ["yellow"]
    raw["source"]["historical_years"] = [2023]
    raw["source"]["current_year"] = 2026
    raw["source"]["download_workers"] = 1
    raw["audit"]["local_jsonl"] = str(tmp_path / "audit.jsonl")
    return PipelineConfig(raw=raw, config_file=base.config_file)


def test_expected_catalog_contains_every_month_and_service() -> None:
    config = load_config(env={})
    entries = expected_catalog(config)

    assert len(entries) == 4 * 4 * 12
    assert entries[0].filename.endswith("2023-01.parquet")
    assert {item.year for item in entries} == {2023, 2024, 2025, 2026}
    assert {item.service for item in entries} == {"yellow", "green", "fhv", "fhvhv"}


def test_probe_falls_back_to_range_when_head_is_rejected() -> None:
    url = "https://example.test/yellow_tripdata_2023-01.parquet"

    class HeadRejected(CatalogSession):
        def head(self, url: str, **_: object) -> FakeResponse:
            return FakeResponse(405, {})

    session = HeadRejected({url}, size=999)
    result = probe_url(url, session=session)

    assert result.available is True
    assert result.method == "GET_RANGE"
    assert result.size_bytes == 999


def test_discovery_requires_all_historical_but_accepts_partial_current_year(
    tmp_path: Path,
) -> None:
    config = small_config(tmp_path)
    candidates = expected_catalog(config)
    available = {
        item.url
        for item in candidates
        if item.year == 2023 or (item.year == 2026 and item.month <= 2)
    }
    session = CatalogSession(available)

    catalog = discover_catalog(config, session=session, workers=1)

    assert len(catalog) == 14
    assert sum(item.year == 2023 for item in catalog) == 12
    assert [item.month for item in catalog if item.year == 2026] == [1, 2]
    assert all(item.available and item.size_bytes == 123 for item in catalog)


def test_discovery_raises_with_exact_missing_historical_file(tmp_path: Path) -> None:
    config = small_config(tmp_path)
    candidates = expected_catalog(config)
    available = {item.url for item in candidates if item.year == 2023 and item.month != 7}
    session = CatalogSession(available)

    with pytest.raises(CatalogCompletenessError) as captured:
        discover_catalog(config, session=session, workers=1)

    assert [item.filename for item in captured.value.missing] == ["yellow_tripdata_2023-07.parquet"]


def test_catalog_snapshot_roundtrip(tmp_path: Path) -> None:
    config = small_config(tmp_path)
    item = expected_catalog(config)[0]
    session = CatalogSession({item.url}, size=88)
    result = probe_url(item.url, session=session)
    discovered = [
        item.__class__(
            **{
                **item.to_dict(),
                "available": result.available,
                "size_bytes": result.size_bytes,
                "probe_method": result.method,
            }
        )
    ]

    path = save_catalog(discovered, tmp_path / "catalog.json")

    assert load_catalog(path) == discovered
