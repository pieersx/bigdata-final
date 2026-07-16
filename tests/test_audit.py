from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from tlc_pipeline.audit import AuditLogger, FileStatus
from tlc_pipeline.catalog import TLCFile


class FakeCollection:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[tuple[dict, dict, bool]] = []

    def update_one(self, selector: dict, update: dict, *, upsert: bool) -> None:
        if self.fail:
            raise RuntimeError("mongo unavailable")
        self.calls.append((selector, update, upsert))


class FakeDatabase:
    def __init__(self, collections: dict[str, FakeCollection]) -> None:
        self.collections = collections

    def __getitem__(self, name: str) -> FakeCollection:
        return self.collections.setdefault(name, FakeCollection())


class FakeMongoClient:
    def __init__(self, *, fail: bool = False) -> None:
        self.collections = {
            "pipeline_runs": FakeCollection(fail=fail),
            "file_manifest": FakeCollection(fail=fail),
            "quality_results": FakeCollection(fail=fail),
            "model_runs": FakeCollection(fail=fail),
        }
        self.database = FakeDatabase(self.collections)

    def __getitem__(self, _: str) -> FakeDatabase:
        return self.database


def read_events(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def file_entry() -> TLCFile:
    return TLCFile(
        service="yellow",
        year=2023,
        month=1,
        url="https://example.test/yellow_tripdata_2023-01.parquet",
        filename="yellow_tripdata_2023-01.parquet",
        size_bytes=100,
        available=True,
    )


def test_run_lifecycle_is_written_to_jsonl_and_upserted_in_mongo(tmp_path: Path) -> None:
    jsonl = tmp_path / "audit.jsonl"
    mongo = FakeMongoClient()
    audit = AuditLogger(jsonl_path=jsonl, mongo_client=mongo)

    with audit.run("ingest", parameters={"years": [2023, 2024, 2025]}) as run_id:
        audit.record_file(
            file_entry(),
            FileStatus.VALIDATED,
            run_id=run_id,
            path=tmp_path / "file.parquet",
            size_bytes=100,
            sha256="a" * 64,
        )

    events = read_events(jsonl)
    assert [event["status"] for event in events] == ["STARTED", "VALIDATED", "SUCCEEDED"]
    assert all(event["run_id"] == run_id for event in events)
    run_calls = mongo.collections["pipeline_runs"].calls
    assert len(run_calls) == 2
    assert run_calls[-1][1]["$set"]["status"] == "SUCCEEDED"
    manifest_update = mongo.collections["file_manifest"].calls[0][1]
    assert manifest_update["$set"]["sha256"] == "a" * 64
    assert manifest_update["$push"]["status_history"]["status"] == "VALIDATED"


def test_failed_run_records_exception_and_reraises(tmp_path: Path) -> None:
    jsonl = tmp_path / "audit.jsonl"
    audit = AuditLogger(jsonl_path=jsonl)

    with pytest.raises(ValueError, match="bad input"):
        with audit.run("silver"):
            raise ValueError("bad input")

    events = read_events(jsonl)
    assert events[-1]["status"] == "FAILED"
    assert "ValueError: bad input" in events[-1]["details"]["error"]


def test_mongo_failure_does_not_lose_local_audit(tmp_path: Path) -> None:
    jsonl = tmp_path / "audit.jsonl"
    audit = AuditLogger(jsonl_path=jsonl, mongo_client=FakeMongoClient(fail=True))

    run_id = audit.start_run("ingest")
    audit.record_file(file_entry(), FileStatus.FAILED, run_id=run_id, error="network")
    audit.finish_run(run_id, success=False, error="network")

    assert len(read_events(jsonl)) == 3
    assert audit.last_mongo_error == "RuntimeError: mongo unavailable"


def test_jsonl_writes_are_thread_safe(tmp_path: Path) -> None:
    jsonl = tmp_path / "audit.jsonl"
    audit = AuditLogger(jsonl_path=jsonl)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(
            pool.map(
                lambda index: audit.record_event("parallel", "OK", details={"index": index}),
                range(100),
            )
        )

    events = read_events(jsonl)
    assert len(events) == 100
    assert {event["details"]["index"] for event in events} == set(range(100))
