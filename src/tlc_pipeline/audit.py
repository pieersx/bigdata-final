"""Auditoria durable del pipeline en JSONL y, cuando esta disponible, MongoDB.

JSONL es el respaldo local obligatorio. MongoDB es un segundo destino: una
interrupcion del servidor no oculta ni interrumpe una descarga, salvo que el
llamador active ``strict=True``. Los documentos usan identificadores estables
para que reintentos no dupliquen ejecuciones ni archivos.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from .config import PipelineConfig


class RunStatus(StrEnum):
    STARTED = "STARTED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class FileStatus(StrEnum):
    DISCOVERED = "DISCOVERED"
    DOWNLOADING = "DOWNLOADING"
    DOWNLOADED = "DOWNLOADED"
    VALIDATED = "VALIDATED"
    SKIPPED = "SKIPPED"
    UNAVAILABLE = "UNAVAILABLE"
    FAILED = "FAILED"


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def _json_safe(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _json_safe(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple | set):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Path | os.PathLike):
        return str(value)
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, bytes):
        return value.hex()
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)


@dataclass(frozen=True, slots=True)
class AuditEvent:
    event_id: str
    event_type: str
    status: str
    timestamp_utc: str
    run_id: str | None
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return _json_safe(asdict(self))


class AuditLogger:
    """Escribe eventos de auditoria thread-safe en JSONL y MongoDB."""

    DEFAULT_COLLECTIONS = {
        "pipeline_runs": "pipeline_runs",
        "file_manifest": "file_manifest",
        "quality_results": "quality_results",
        "model_runs": "model_runs",
    }

    def __init__(
        self,
        *,
        jsonl_path: str | os.PathLike[str] | None,
        mongo_uri: str | None = None,
        mongo_database: str = "tlc_audit",
        collections: Mapping[str, str] | None = None,
        mongo_client: Any | None = None,
        strict: bool = False,
        server_selection_timeout_ms: int = 1_500,
    ) -> None:
        self.jsonl_path = Path(jsonl_path) if jsonl_path is not None else None
        self.mongo_database = mongo_database
        self.collections = {**self.DEFAULT_COLLECTIONS, **dict(collections or {})}
        self.strict = strict
        self._lock = threading.RLock()
        self._mongo_client = mongo_client
        self._owns_mongo_client = False
        self.last_mongo_error: str | None = None

        if self.jsonl_path is not None:
            self.jsonl_path.parent.mkdir(parents=True, exist_ok=True)

        if self._mongo_client is None and mongo_uri:
            try:
                from pymongo import MongoClient

                # connect=False evita que construir el logger bloquee si Mongo
                # aun esta arrancando dentro de Docker Compose.
                self._mongo_client = MongoClient(
                    mongo_uri,
                    connect=False,
                    serverSelectionTimeoutMS=server_selection_timeout_ms,
                )
                self._owns_mongo_client = True
            except Exception as exc:  # pragma: no cover - depende del entorno
                self._handle_mongo_error(exc)

    @classmethod
    def from_config(
        cls,
        config: PipelineConfig,
        *,
        mongo_uri: str | None = None,
        mongo_client: Any | None = None,
        strict: bool = False,
    ) -> AuditLogger:
        audit = config.section("audit")
        return cls(
            jsonl_path=audit.get("local_jsonl"),
            mongo_uri=mongo_uri or os.getenv("MONGO_URI") or os.getenv("TLC_MONGO_URI"),
            mongo_database=str(audit.get("mongo_database", "tlc_audit")),
            collections=audit.get("collections", {}),
            mongo_client=mongo_client,
            strict=strict,
        )

    @property
    def mongo_enabled(self) -> bool:
        return self._mongo_client is not None

    def _handle_mongo_error(self, exc: Exception) -> None:
        self.last_mongo_error = f"{type(exc).__name__}: {exc}"
        if self.strict:
            raise exc

    def _mongo_collection(self, logical_name: str) -> Any | None:
        if self._mongo_client is None:
            return None
        name = self.collections.get(logical_name, logical_name)
        return self._mongo_client[self.mongo_database][name]

    def _mongo(self, logical_name: str, operation: str, *args: Any, **kwargs: Any) -> Any:
        collection = self._mongo_collection(logical_name)
        if collection is None:
            return None
        try:
            return getattr(collection, operation)(*args, **kwargs)
        except Exception as exc:  # Mongo no debe invalidar el respaldo JSONL
            self._handle_mongo_error(exc)
            return None

    def _write_local(self, document: Mapping[str, Any]) -> None:
        if self.jsonl_path is None:
            return
        line = json.dumps(_json_safe(document), ensure_ascii=False, sort_keys=True)
        with self._lock:
            with self.jsonl_path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(line)
                handle.write("\n")
                handle.flush()

    def record_event(
        self,
        event_type: str,
        status: str | StrEnum,
        *,
        run_id: str | None = None,
        details: Mapping[str, Any] | None = None,
        event_id: str | None = None,
    ) -> AuditEvent:
        event = AuditEvent(
            event_id=event_id or str(uuid.uuid4()),
            event_type=event_type,
            status=str(status),
            timestamp_utc=utc_now(),
            run_id=run_id,
            details=_json_safe(dict(details or {})),
        )
        self._write_local(event.to_dict())
        return event

    def start_run(
        self,
        pipeline_name: str,
        *,
        run_id: str | None = None,
        parameters: Mapping[str, Any] | None = None,
    ) -> str:
        run_id = run_id or str(uuid.uuid4())
        timestamp = utc_now()
        document = {
            "_id": run_id,
            "run_id": run_id,
            "pipeline_name": pipeline_name,
            "status": RunStatus.STARTED.value,
            "started_at_utc": timestamp,
            "finished_at_utc": None,
            "parameters": _json_safe(dict(parameters or {})),
        }
        self._mongo(
            "pipeline_runs",
            "update_one",
            {"_id": run_id},
            {"$set": document},
            upsert=True,
        )
        self.record_event(
            "pipeline_run",
            RunStatus.STARTED,
            run_id=run_id,
            details=document,
            event_id=f"{run_id}:started",
        )
        return run_id

    def finish_run(
        self,
        run_id: str,
        *,
        success: bool,
        metrics: Mapping[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        status = RunStatus.SUCCEEDED if success else RunStatus.FAILED
        updates = {
            "status": status.value,
            "finished_at_utc": utc_now(),
            "metrics": _json_safe(dict(metrics or {})),
            "error": error,
        }
        self._mongo(
            "pipeline_runs",
            "update_one",
            {"_id": run_id},
            {"$set": updates},
            upsert=True,
        )
        self.record_event(
            "pipeline_run",
            status,
            run_id=run_id,
            details=updates,
            event_id=f"{run_id}:finished",
        )

    @contextmanager
    def run(
        self,
        pipeline_name: str,
        *,
        run_id: str | None = None,
        parameters: Mapping[str, Any] | None = None,
    ) -> Iterator[str]:
        current_run_id = self.start_run(
            pipeline_name,
            run_id=run_id,
            parameters=parameters,
        )
        try:
            yield current_run_id
        except BaseException as exc:
            self.finish_run(current_run_id, success=False, error=f"{type(exc).__name__}: {exc}")
            raise
        else:
            self.finish_run(current_run_id, success=True)

    def record_file(
        self,
        file: Any,
        status: str | FileStatus,
        *,
        run_id: str | None = None,
        path: str | os.PathLike[str] | None = None,
        size_bytes: int | None = None,
        sha256: str | None = None,
        error: str | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> str:
        if hasattr(file, "to_dict"):
            metadata = file.to_dict()
        elif is_dataclass(file):
            metadata = asdict(file)
        elif isinstance(file, Mapping):
            metadata = dict(file)
        else:
            metadata = {"url": str(file)}
        metadata = _json_safe(metadata)
        url = str(metadata.get("url", ""))
        filename = str(metadata.get("filename", Path(url).name))
        file_id = hashlib.sha256((url or filename).encode("utf-8")).hexdigest()
        timestamp = utc_now()
        status_value = status.value if isinstance(status, StrEnum) else str(status)
        document = {
            "_id": file_id,
            "file_id": file_id,
            "run_id": run_id,
            "status": status_value,
            "updated_at_utc": timestamp,
            "url": url,
            "filename": filename,
            "local_path": str(path) if path is not None else None,
            "size_bytes": size_bytes,
            "sha256": sha256,
            "error": error,
            "source": metadata,
            **_json_safe(dict(extra or {})),
        }
        history = {
            "status": status_value,
            "timestamp_utc": timestamp,
            "run_id": run_id,
            "error": error,
        }
        # $setOnInsert evita el conflicto de modificar _id con $set en Mongo.
        mongo_set = {key: value for key, value in document.items() if key != "_id"}
        self._mongo(
            "file_manifest",
            "update_one",
            {"_id": file_id},
            {
                "$set": mongo_set,
                "$setOnInsert": {"_id": file_id, "created_at_utc": timestamp},
                "$push": {"status_history": history},
            },
            upsert=True,
        )
        self.record_event(
            "file_status",
            status_value,
            run_id=run_id,
            details=document,
        )
        return file_id

    def record_quality(
        self,
        result: Mapping[str, Any],
        *,
        run_id: str | None = None,
    ) -> str:
        result_id = str(result.get("result_id") or uuid.uuid4())
        document = {
            "_id": result_id,
            "run_id": run_id,
            "recorded_at_utc": utc_now(),
            **_json_safe(dict(result)),
        }
        self._mongo(
            "quality_results", "update_one", {"_id": result_id}, {"$set": document}, upsert=True
        )
        self.record_event(
            "quality_result",
            str(result.get("status", "RECORDED")),
            run_id=run_id,
            details=document,
        )
        return result_id

    def record_model(
        self,
        result: Mapping[str, Any],
        *,
        run_id: str | None = None,
    ) -> str:
        model_run_id = str(result.get("model_run_id") or uuid.uuid4())
        document = {
            "_id": model_run_id,
            "run_id": run_id,
            "recorded_at_utc": utc_now(),
            **_json_safe(dict(result)),
        }
        self._mongo(
            "model_runs", "update_one", {"_id": model_run_id}, {"$set": document}, upsert=True
        )
        self.record_event(
            "model_run",
            str(result.get("status", "RECORDED")),
            run_id=run_id,
            details=document,
        )
        return model_run_id

    def close(self) -> None:
        if self._owns_mongo_client and self._mongo_client is not None:
            try:
                self._mongo_client.close()
            finally:
                self._mongo_client = None

    def __enter__(self) -> AuditLogger:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
