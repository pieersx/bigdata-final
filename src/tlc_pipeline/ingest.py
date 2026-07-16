"""Descarga completa, resumible e idempotente de NYC TLC a Bronze."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import threading
import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .audit import AuditLogger, FileStatus
from .catalog import (
    CatalogCompletenessError,
    CatalogError,
    TLCFile,
    build_http_session,
    expected_catalog,
    probe_url,
    session_from_config,
)
from .config import PipelineConfig

PARQUET_MAGIC = b"PAR1"
_CONTENT_RANGE_RE = re.compile(r"^bytes\s+(\d+)-(\d+)/(\d+)$", re.IGNORECASE)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def _header(headers: Mapping[str, Any], name: str) -> str | None:
    expected = name.casefold()
    for key, value in headers.items():
        if str(key).casefold() == expected:
            return str(value)
    return None


def _required_content_length(headers: Mapping[str, Any]) -> int:
    raw = _header(headers, "Content-Length")
    try:
        length = int(raw) if raw is not None else 0
    except ValueError as exc:
        raise IntegrityError(f"Content-Length invalido: {raw!r}") from exc
    if length <= 0:
        raise IntegrityError("La respuesta no contiene un Content-Length positivo")
    return length


class DownloadError(RuntimeError):
    """La transferencia no pudo completarse."""


class IntegrityError(DownloadError):
    """El archivo no coincide con los metadatos remotos o no es Parquet."""


class BatchDownloadError(DownloadError):
    """Una o mas descargas fallaron; las demas se dejaron completar."""

    def __init__(
        self,
        failures: Mapping[str, BaseException],
        completed: Sequence[DownloadResult],
    ) -> None:
        self.failures = dict(failures)
        self.completed = tuple(completed)
        sample = "; ".join(
            f"{name}: {type(error).__name__}: {error}"
            for name, error in list(self.failures.items())[:5]
        )
        super().__init__(f"Fallaron {len(self.failures)} descargas. {sample}")


@dataclass(frozen=True, slots=True)
class DownloadResult:
    url: str
    filename: str
    path: str
    status: str
    size_bytes: int
    sha256: str
    resumed_from_bytes: int
    transferred_bytes: int
    started_at_utc: str
    finished_at_utc: str
    sidecar_path: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ManifestStore:
    """Manifest JSON atomico con una entrada vigente por URL."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    def _read_unlocked(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"schema_version": 1, "updated_at_utc": None, "files": {}}
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise IntegrityError(f"Manifest corrupto {self.path}: {exc}") from exc
        if not isinstance(value, dict) or not isinstance(value.get("files"), dict):
            raise IntegrityError(f"Formato de manifest invalido: {self.path}")
        return value

    def record(self, entry: Mapping[str, Any]) -> None:
        url = str(entry.get("url", ""))
        if not url:
            raise ValueError("Una entrada de manifest requiere url")
        with self._lock:
            document = self._read_unlocked()
            document["updated_at_utc"] = _utc_now()
            document["files"][url] = dict(entry)
            temporary = self.path.with_name(f".{self.path.name}.{uuid.uuid4().hex}.tmp")
            temporary.write_text(
                json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            os.replace(temporary, self.path)

    def entries(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._read_unlocked()["files"].values())


def sidecar_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.metadata.json")


def partial_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.part")


def partial_metadata_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.part.metadata.json")


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(dict(value), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def sha256_file(path: str | os.PathLike[str], *, block_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while block := handle.read(block_size):
            digest.update(block)
    return digest.hexdigest()


def validate_file(
    path: str | os.PathLike[str],
    *,
    expected_size: int,
    require_parquet_magic: bool = True,
) -> str:
    """Valida tamano, cabecera/footer Parquet y devuelve SHA-256."""

    source = Path(path)
    if not source.is_file():
        raise IntegrityError(f"No existe el archivo esperado: {source}")
    actual_size = source.stat().st_size
    if actual_size != expected_size:
        raise IntegrityError(
            f"Tamano incorrecto para {source.name}: {actual_size} != {expected_size}"
        )
    if require_parquet_magic:
        if actual_size < len(PARQUET_MAGIC) * 2:
            raise IntegrityError(f"{source.name} es demasiado pequeno para ser Parquet")
        with source.open("rb") as handle:
            prefix = handle.read(4)
            handle.seek(-4, os.SEEK_END)
            suffix = handle.read(4)
        if prefix != PARQUET_MAGIC or suffix != PARQUET_MAGIC:
            raise IntegrityError(
                f"Magic bytes Parquet invalidos en {source.name}: {prefix!r}/{suffix!r}"
            )
    return sha256_file(source)


def _metadata_matches(entry: TLCFile, metadata: Mapping[str, Any], checksum: str) -> bool:
    if metadata.get("url") != entry.url:
        return False
    if metadata.get("size_bytes") != entry.size_bytes:
        return False
    if metadata.get("sha256") != checksum:
        return False
    saved_etag = metadata.get("etag")
    if saved_etag and entry.etag and saved_etag != entry.etag:
        return False
    return True


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _final_metadata(
    entry: TLCFile,
    destination: Path,
    checksum: str,
    *,
    status: str,
    resumed_from: int,
    transferred: int,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "url": entry.url,
        "filename": entry.filename,
        "service": entry.service,
        "year": entry.year,
        "month": entry.month,
        "file_format": entry.file_format,
        "local_path": str(destination.resolve()),
        "size_bytes": destination.stat().st_size,
        "content_length": entry.size_bytes,
        "sha256": checksum,
        "etag": entry.etag,
        "last_modified": entry.last_modified,
        "status": status,
        "resumed_from_bytes": resumed_from,
        "transferred_bytes": transferred,
        "validated_at_utc": _utc_now(),
    }


def _quarantine_or_remove(path: Path, quarantine_dir: Path | None) -> None:
    if not path.exists():
        return
    if quarantine_dir is None:
        path.unlink()
        return
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    target = quarantine_dir / f"{path.name}.{uuid.uuid4().hex}.invalid"
    shutil.move(str(path), str(target))


def _prepare_partial(entry: TLCFile, destination: Path) -> tuple[Path, Path, int]:
    part = partial_path(destination)
    part_meta = partial_metadata_path(destination)
    metadata = _load_json(part_meta)
    if part.exists():
        incompatible = part.stat().st_size > int(entry.size_bytes or 0)
        if metadata is not None:
            incompatible = incompatible or metadata.get("url") != entry.url
            incompatible = incompatible or metadata.get("size_bytes") != entry.size_bytes
            if metadata.get("etag") and entry.etag:
                incompatible = incompatible or metadata["etag"] != entry.etag
        if incompatible:
            part.unlink(missing_ok=True)
            part_meta.unlink(missing_ok=True)
    elif part_meta.exists():
        part_meta.unlink(missing_ok=True)

    _atomic_json(
        part_meta,
        {
            "url": entry.url,
            "size_bytes": entry.size_bytes,
            "etag": entry.etag,
            "last_modified": entry.last_modified,
            "updated_at_utc": _utc_now(),
        },
    )
    return part, part_meta, part.stat().st_size if part.exists() else 0


def _verify_response(
    response: Any,
    *,
    expected_size: int,
    requested_start: int,
) -> tuple[str, int]:
    """Devuelve modo de apertura y offset efectivo, tras validar HTTP."""

    status = int(response.status_code)
    content_length = _required_content_length(response.headers)
    if requested_start > 0 and status == 206:
        raw_range = _header(response.headers, "Content-Range")
        match = _CONTENT_RANGE_RE.match(raw_range.strip()) if raw_range else None
        if not match:
            raise IntegrityError("Respuesta 206 sin Content-Range valido")
        start, end, total = (int(group) for group in match.groups())
        if start != requested_start or total != expected_size or end < start:
            raise IntegrityError(
                f"Content-Range inesperado: {raw_range!r}; "
                f"esperado inicio={requested_start}, total={expected_size}"
            )
        if content_length != end - start + 1 or content_length != expected_size - requested_start:
            raise IntegrityError(
                f"Content-Length parcial incorrecto: {content_length} != "
                f"{expected_size - requested_start}"
            )
        return "ab", requested_start

    if status == 200:
        if content_length != expected_size:
            raise IntegrityError(f"Content-Length incorrecto: {content_length} != {expected_size}")
        # El servidor puede ignorar Range. En tal caso se reinicia de manera
        # segura y nunca se concatena una respuesta completa al .part.
        return "wb", 0

    raise DownloadError(f"HTTP {status} descargando {getattr(response, 'url', '')}")


def download_file(
    entry: TLCFile,
    destination_dir: str | os.PathLike[str],
    *,
    session: Any | None = None,
    chunk_size_bytes: int = 8 * 1024 * 1024,
    timeout: float | tuple[float, float] = (20, 180),
    audit: AuditLogger | None = None,
    run_id: str | None = None,
    manifest: ManifestStore | None = None,
    quarantine_dir: str | os.PathLike[str] | None = None,
    require_parquet_magic: bool | None = None,
    throttle_retries: int = 0,
    throttle_backoff_seconds: float = 30,
) -> DownloadResult:
    """Descarga un archivo con resume, checksum, sidecar y reemplazo atomico."""

    if not entry.available or entry.size_bytes is None or entry.size_bytes <= 0:
        raise DownloadError(f"El archivo no fue descubierto como disponible: {entry.url}")
    if chunk_size_bytes <= 0:
        raise ValueError("chunk_size_bytes debe ser positivo")
    if throttle_retries < 0 or throttle_backoff_seconds < 0:
        raise ValueError("Los parámetros de reintento por throttling no pueden ser negativos")
    check_parquet = (
        entry.file_format.casefold() == "parquet"
        if require_parquet_magic is None
        else require_parquet_magic
    )
    destination_directory = Path(destination_dir)
    destination_directory.mkdir(parents=True, exist_ok=True)
    destination = destination_directory / entry.filename
    sidecar = sidecar_path(destination)
    quarantine = Path(quarantine_dir) if quarantine_dir is not None else None
    started = _utc_now()

    # Camino idempotente: no realiza ninguna solicitud si archivo + sidecar son
    # coherentes. Si falta el sidecar, se reconstruye despues de validar todo.
    if destination.exists():
        try:
            checksum = validate_file(
                destination,
                expected_size=entry.size_bytes,
                require_parquet_magic=check_parquet,
            )
            prior_metadata = _load_json(sidecar)
            if prior_metadata is None or _metadata_matches(entry, prior_metadata, checksum):
                metadata = _final_metadata(
                    entry,
                    destination,
                    checksum,
                    status=FileStatus.VALIDATED.value,
                    resumed_from=entry.size_bytes,
                    transferred=0,
                )
                if prior_metadata is None:
                    _atomic_json(sidecar, metadata)
                result = DownloadResult(
                    url=entry.url,
                    filename=entry.filename,
                    path=str(destination.resolve()),
                    status=FileStatus.SKIPPED.value,
                    size_bytes=entry.size_bytes,
                    sha256=checksum,
                    resumed_from_bytes=entry.size_bytes,
                    transferred_bytes=0,
                    started_at_utc=started,
                    finished_at_utc=_utc_now(),
                    sidecar_path=str(sidecar.resolve()),
                )
                if manifest is not None:
                    manifest.record({**metadata, "status": result.status})
                if audit is not None:
                    audit.record_file(
                        entry,
                        FileStatus.SKIPPED,
                        run_id=run_id,
                        path=destination,
                        size_bytes=entry.size_bytes,
                        sha256=checksum,
                    )
                return result
        except IntegrityError:
            pass
        _quarantine_or_remove(destination, quarantine)
        sidecar.unlink(missing_ok=True)

    owns_session = session is None
    current_session = session or build_http_session()
    part, part_meta, requested_start = _prepare_partial(entry, destination)
    if requested_start == entry.size_bytes:
        try:
            validate_file(
                part,
                expected_size=entry.size_bytes,
                require_parquet_magic=check_parquet,
            )
        except IntegrityError:
            # Un proceso anterior pudo completar el numero de bytes pero dejar
            # un archivo corrupto. Se descarta para que el reintento no quede
            # atrapado validando eternamente el mismo .part.
            part.unlink(missing_ok=True)
            part_meta.unlink(missing_ok=True)
            part, part_meta, requested_start = _prepare_partial(entry, destination)
    effective_start = requested_start
    response = None
    try:
        if audit is not None:
            audit.record_file(
                entry,
                FileStatus.DOWNLOADING,
                run_id=run_id,
                path=destination,
                size_bytes=requested_start,
                extra={"expected_size_bytes": entry.size_bytes},
            )

        # Un .part que ya tiene todos los bytes puede proceder directamente a
        # validacion (por ejemplo, si el proceso murio antes de os.replace).
        if requested_start < entry.size_bytes:
            headers = {"Accept-Encoding": "identity"}
            if requested_start:
                headers["Range"] = f"bytes={requested_start}-"
            for attempt in range(throttle_retries + 1):
                response = current_session.get(
                    entry.url,
                    headers=headers,
                    stream=True,
                    timeout=timeout,
                    allow_redirects=True,
                )
                if int(response.status_code) not in {403, 429} or attempt >= throttle_retries:
                    break
                response.close()
                response = None
                # CloudFront puede bloquear temporalmente una IP después de un
                # lote de solicitudes. Se espera sin perder el .part y luego se
                # reintenta el mismo byte inicial.
                time.sleep(throttle_backoff_seconds)
            mode, effective_start = _verify_response(
                response,
                expected_size=entry.size_bytes,
                requested_start=requested_start,
            )
            bytes_on_disk = effective_start
            with part.open(mode) as handle:
                for chunk in response.iter_content(chunk_size=chunk_size_bytes):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    bytes_on_disk += len(chunk)
                    if bytes_on_disk > entry.size_bytes:
                        raise IntegrityError(
                            f"El servidor envio mas bytes de los anunciados para {entry.filename}"
                        )
                handle.flush()
                os.fsync(handle.fileno())

        if audit is not None:
            audit.record_file(
                entry,
                FileStatus.DOWNLOADED,
                run_id=run_id,
                path=part,
                size_bytes=part.stat().st_size if part.exists() else 0,
            )

        checksum = validate_file(
            part,
            expected_size=entry.size_bytes,
            require_parquet_magic=check_parquet,
        )
        os.replace(part, destination)
        part_meta.unlink(missing_ok=True)
        transferred = entry.size_bytes - effective_start
        metadata = _final_metadata(
            entry,
            destination,
            checksum,
            status=FileStatus.VALIDATED.value,
            resumed_from=effective_start,
            transferred=transferred,
        )
        _atomic_json(sidecar, metadata)
        if manifest is not None:
            manifest.record(metadata)
        if audit is not None:
            audit.record_file(
                entry,
                FileStatus.VALIDATED,
                run_id=run_id,
                path=destination,
                size_bytes=entry.size_bytes,
                sha256=checksum,
            )
        return DownloadResult(
            url=entry.url,
            filename=entry.filename,
            path=str(destination.resolve()),
            status=FileStatus.VALIDATED.value,
            size_bytes=entry.size_bytes,
            sha256=checksum,
            resumed_from_bytes=effective_start,
            transferred_bytes=transferred,
            started_at_utc=started,
            finished_at_utc=_utc_now(),
            sidecar_path=str(sidecar.resolve()),
        )
    except BaseException as exc:
        if audit is not None:
            audit.record_file(
                entry,
                FileStatus.FAILED,
                run_id=run_id,
                path=part,
                size_bytes=part.stat().st_size if part.exists() else 0,
                error=f"{type(exc).__name__}: {exc}",
            )
        raise
    finally:
        if response is not None:
            response.close()
        if owns_session:
            current_session.close()


def bronze_destination(bronze_dir: str | os.PathLike[str], entry: TLCFile) -> Path:
    """Particionado fisico estable de la capa Bronze."""

    return (
        Path(bronze_dir)
        / "trip_records"
        / f"service={entry.service}"
        / f"year={entry.year}"
        / f"month={entry.month:02d}"
    )


def _missing_historical(entries: Sequence[TLCFile], config: PipelineConfig) -> list[TLCFile]:
    historical_years = set(int(year) for year in config.get("source.historical_years", []))
    actual = {
        (entry.service, entry.year, entry.month)
        for entry in entries
        if entry.available and entry.year in historical_years
    }
    return [
        candidate
        for candidate in expected_catalog(config, include_current_year=False)
        if (candidate.service, candidate.year, candidate.month) not in actual
    ]


def download_catalog(
    entries: Iterable[TLCFile],
    config: PipelineConfig,
    *,
    bronze_dir: str | os.PathLike[str] | None = None,
    session: Any | None = None,
    workers: int | None = None,
    audit: AuditLogger | None = None,
    run_id: str | None = None,
    enforce_historical_completeness: bool | None = None,
) -> list[DownloadResult]:
    """Descarga el catalogo en paralelo y reporta todos los fallos juntos."""

    catalog = list(entries)
    enforce = (
        bool(config.get("source.require_complete_historical", True))
        if enforce_historical_completeness is None
        else enforce_historical_completeness
    )
    if enforce:
        missing = _missing_historical(catalog, config)
        if missing:
            raise CatalogCompletenessError(missing)

    target_bronze = Path(bronze_dir) if bronze_dir is not None else config.path("bronze")
    target_bronze.mkdir(parents=True, exist_ok=True)
    manifest = ManifestStore(target_bronze / "_manifest.json")
    source = config.section("source")
    worker_count = workers if workers is not None else int(source.get("download_workers", 4))
    if worker_count <= 0:
        raise ValueError("workers debe ser positivo")
    chunk_size = int(source.get("chunk_size_bytes", 8 * 1024 * 1024))
    timeout = (
        float(source.get("connect_timeout_seconds", 20)),
        float(source.get("read_timeout_seconds", 180)),
    )
    throttle_retries = int(source.get("throttle_retries", 0))
    throttle_backoff = float(source.get("throttle_backoff_seconds", 30))
    quarantine = config.path("quarantine")

    owns_session = session is None
    current_session = session or session_from_config(config)

    def transfer(entry: TLCFile) -> DownloadResult:
        return download_file(
            entry,
            bronze_destination(target_bronze, entry),
            session=current_session,
            chunk_size_bytes=chunk_size,
            timeout=timeout,
            audit=audit,
            run_id=run_id,
            manifest=manifest,
            quarantine_dir=quarantine,
            throttle_retries=throttle_retries,
            throttle_backoff_seconds=throttle_backoff,
        )

    completed: list[DownloadResult] = []
    failures: dict[str, BaseException] = {}
    try:
        if worker_count == 1:
            for entry in catalog:
                try:
                    completed.append(transfer(entry))
                except Exception as exc:
                    failures[entry.filename] = exc
        else:
            with ThreadPoolExecutor(
                max_workers=worker_count,
                thread_name_prefix="tlc-download",
            ) as pool:
                futures = {pool.submit(transfer, entry): entry for entry in catalog}
                for future in as_completed(futures):
                    entry = futures[future]
                    try:
                        completed.append(future.result())
                    except Exception as exc:
                        failures[entry.filename] = exc
    finally:
        if owns_session:
            current_session.close()

    completed.sort(key=lambda result: result.filename)
    if failures:
        raise BatchDownloadError(failures, completed)
    return completed


def download_zone_lookup(
    config: PipelineConfig,
    *,
    session: Any | None = None,
    audit: AuditLogger | None = None,
    run_id: str | None = None,
    bronze_dir: str | os.PathLike[str] | None = None,
) -> DownloadResult:
    """Descarga idempotente del lookup oficial de zonas a Bronze/reference."""

    source = config.section("source")
    url = str(source["zone_lookup_url"])
    owns_session = session is None
    current_session = session or session_from_config(config)
    try:
        remote = probe_url(
            url,
            session=current_session,
            timeout=float(source.get("discovery_timeout_seconds", 30)),
        )
        if not remote.available or remote.size_bytes is None:
            raise CatalogError(f"Zone lookup no disponible: {url}. {remote.error or ''}")
        filename = Path(urlparse(url).path).name or "taxi_zone_lookup.csv"
        entry = TLCFile(
            service="reference",
            year=0,
            month=0,
            url=url,
            filename=filename,
            size_bytes=remote.size_bytes,
            available=True,
            etag=remote.etag,
            last_modified=remote.last_modified,
            discovered_at_utc=_utc_now(),
            probe_method=remote.method,
            http_status=remote.status_code,
            file_format="csv",
        )
        target_bronze = Path(bronze_dir) if bronze_dir is not None else config.path("bronze")
        manifest = ManifestStore(target_bronze / "_manifest.json")
        if audit is not None:
            audit.record_file(
                entry,
                FileStatus.DISCOVERED,
                run_id=run_id,
                size_bytes=entry.size_bytes,
            )
        return download_file(
            entry,
            target_bronze / "reference",
            session=current_session,
            chunk_size_bytes=int(source.get("chunk_size_bytes", 8 * 1024 * 1024)),
            timeout=(
                float(source.get("connect_timeout_seconds", 20)),
                float(source.get("read_timeout_seconds", 180)),
            ),
            audit=audit,
            run_id=run_id,
            manifest=manifest,
            quarantine_dir=config.path("quarantine"),
            require_parquet_magic=False,
            throttle_retries=int(source.get("throttle_retries", 0)),
            throttle_backoff_seconds=float(source.get("throttle_backoff_seconds", 30)),
        )
    finally:
        if owns_session:
            current_session.close()
