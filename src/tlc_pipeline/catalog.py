"""Descubrimiento verificable del catalogo oficial NYC TLC.

Los nombres de los archivos siguen el contrato publicado por TLC, pero un
archivo solo se considera disponible despues de consultar el servidor. Primero
se intenta HEAD y, si el CDN/proxy lo rechaza o no informa el tamano, se usa un
GET ``Range: bytes=0-0`` sin descargar el cuerpo completo.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .audit import AuditLogger, FileStatus
from .config import PipelineConfig

PARQUET_SERVICES = ("yellow", "green", "fhv", "fhvhv")
_CONTENT_RANGE_RE = re.compile(r"^bytes\s+\d+-\d+/(\d+)$", re.IGNORECASE)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


@dataclass(frozen=True, slots=True)
class ProbeResult:
    url: str
    available: bool
    size_bytes: int | None
    etag: str | None
    last_modified: str | None
    status_code: int | None
    method: str
    error: str | None = None


@dataclass(frozen=True, slots=True)
class TLCFile:
    service: str
    year: int
    month: int
    url: str
    filename: str
    size_bytes: int | None
    available: bool
    etag: str | None = None
    last_modified: str | None = None
    discovered_at_utc: str | None = None
    probe_method: str | None = None
    http_status: int | None = None
    error: str | None = None
    file_format: str = "parquet"

    @property
    def content_length(self) -> int | None:
        """Alias explicito para el encabezado HTTP validado."""

        return self.size_bytes

    @property
    def is_historical(self) -> bool:
        return self.year > 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> TLCFile:
        return cls(**dict(value))


class CatalogError(RuntimeError):
    """No fue posible construir un catalogo confiable."""


class CatalogCompletenessError(CatalogError):
    """Falta al menos un archivo mensual historico obligatorio."""

    def __init__(self, missing: Sequence[TLCFile]) -> None:
        self.missing = tuple(missing)
        names = ", ".join(item.filename for item in self.missing[:8])
        suffix = " ..." if len(self.missing) > 8 else ""
        super().__init__(
            f"Faltan {len(self.missing)} archivos historicos obligatorios: {names}{suffix}"
        )


def build_http_session(
    *,
    retries: int = 5,
    backoff_seconds: float = 2,
    user_agent: str = "tlc-bigdata-final/1.0",
) -> requests.Session:
    """Crea una sesion con reintentos solo para operaciones HTTP idempotentes."""

    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=backoff_seconds,
        status_forcelist=(408, 425, 429, 500, 502, 503, 504),
        allowed_methods=frozenset({"HEAD", "GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": user_agent, "Accept-Encoding": "identity"})
    return session


def session_from_config(config: PipelineConfig) -> requests.Session:
    source = config.section("source")
    return build_http_session(
        retries=int(source.get("retries", 5)),
        backoff_seconds=float(source.get("retry_backoff_seconds", 2)),
        user_agent=str(source.get("user_agent", "tlc-bigdata-final/1.0")),
    )


def _header(headers: Mapping[str, Any], name: str) -> str | None:
    expected = name.casefold()
    for key, value in headers.items():
        if str(key).casefold() == expected:
            return str(value)
    return None


def _positive_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _metadata_from_headers(
    url: str,
    response: Any,
    *,
    method: str,
) -> ProbeResult | None:
    status = int(response.status_code)
    if status < 200 or status >= 300:
        return None
    headers = response.headers
    size = _positive_int(_header(headers, "Content-Length"))
    if method == "GET_RANGE":
        content_range = _header(headers, "Content-Range")
        match = _CONTENT_RANGE_RE.match(content_range.strip()) if content_range else None
        if match:
            size = _positive_int(match.group(1))
        elif status == 206:
            # Una respuesta parcial sin total no permite validar la descarga.
            size = None
    if size is None:
        return None
    return ProbeResult(
        url=url,
        available=True,
        size_bytes=size,
        etag=_header(headers, "ETag"),
        last_modified=_header(headers, "Last-Modified"),
        status_code=status,
        method=method,
    )


def probe_url(
    url: str,
    *,
    session: Any | None = None,
    timeout: float = 30,
) -> ProbeResult:
    """Verifica existencia y tamano remoto con fallback HEAD -> GET Range."""

    owns_session = session is None
    current_session = session or build_http_session()
    head_error: str | None = None
    head_status: int | None = None
    try:
        response = None
        try:
            response = current_session.head(url, timeout=timeout, allow_redirects=True)
            head_status = int(response.status_code)
            result = _metadata_from_headers(url, response, method="HEAD")
            if result is not None:
                return result
            head_error = f"HEAD HTTP {response.status_code} o Content-Length ausente/invalido"
        except Exception as exc:
            head_error = f"HEAD {type(exc).__name__}: {exc}"
        finally:
            if response is not None:
                response.close()

        response = None
        try:
            response = current_session.get(
                url,
                headers={"Range": "bytes=0-0", "Accept-Encoding": "identity"},
                stream=True,
                timeout=timeout,
                allow_redirects=True,
            )
            result = _metadata_from_headers(url, response, method="GET_RANGE")
            if result is not None:
                return result
            get_error = (
                f"GET Range HTTP {response.status_code} o Content-Length/Content-Range "
                "ausente/invalido"
            )
            return ProbeResult(
                url=url,
                available=False,
                size_bytes=None,
                etag=None,
                last_modified=None,
                status_code=int(response.status_code),
                method="GET_RANGE",
                error=f"{head_error}; {get_error}",
            )
        except Exception as exc:
            return ProbeResult(
                url=url,
                available=False,
                size_bytes=None,
                etag=None,
                last_modified=None,
                status_code=head_status,
                method="GET_RANGE",
                error=f"{head_error}; GET Range {type(exc).__name__}: {exc}",
            )
        finally:
            if response is not None:
                response.close()
    finally:
        if owns_session:
            current_session.close()


def expected_catalog(
    config: PipelineConfig,
    *,
    include_current_year: bool = True,
) -> list[TLCFile]:
    """Construye todos los candidatos mensuales que deben comprobarse."""

    source = config.section("source")
    services = tuple(str(service) for service in source["services"])
    years = list(int(year) for year in source["historical_years"])
    if include_current_year:
        years.append(int(source["current_year"]))
    base_url = str(source["base_url"]).rstrip("/")
    candidates: list[TLCFile] = []
    for year in years:
        for service in services:
            for month in range(1, 13):
                filename = f"{service}_tripdata_{year}-{month:02d}.parquet"
                candidates.append(
                    TLCFile(
                        service=service,
                        year=year,
                        month=month,
                        url=f"{base_url}/{filename}",
                        filename=filename,
                        size_bytes=None,
                        available=False,
                    )
                )
    return candidates


def _apply_probe(candidate: TLCFile, result: ProbeResult) -> TLCFile:
    return TLCFile(
        service=candidate.service,
        year=candidate.year,
        month=candidate.month,
        url=candidate.url,
        filename=candidate.filename,
        size_bytes=result.size_bytes,
        available=result.available,
        etag=result.etag,
        last_modified=result.last_modified,
        discovered_at_utc=_utc_now(),
        probe_method=result.method,
        http_status=result.status_code,
        error=result.error,
        file_format=candidate.file_format,
    )


def discover_catalog(
    config: PipelineConfig,
    *,
    session: Any | None = None,
    audit: AuditLogger | None = None,
    run_id: str | None = None,
    workers: int | None = None,
    include_unavailable: bool = False,
    include_current_year: bool = True,
) -> list[TLCFile]:
    """Descubre el corpus completo historico y los meses publicados de 2026.

    Se comprueban siempre los doce meses del anio corriente. Los 404 de ese
    anio son esperados y no ingresan al catalogo descargable; cualquier ausencia
    en 2023--2025 provoca ``CatalogCompletenessError`` si asi lo exige el YAML.
    """

    source = config.section("source")
    candidates = expected_catalog(config, include_current_year=include_current_year)
    timeout = float(source.get("discovery_timeout_seconds", 30))
    worker_count = workers if workers is not None else int(source.get("discovery_workers", 8))
    if worker_count <= 0:
        raise ValueError("workers debe ser positivo")

    owns_session = session is None
    current_session = session or session_from_config(config)
    discovered: list[TLCFile] = []

    def inspect(candidate: TLCFile) -> TLCFile:
        result = probe_url(candidate.url, session=current_session, timeout=timeout)
        entry = _apply_probe(candidate, result)
        if audit is not None:
            audit.record_file(
                entry,
                FileStatus.DISCOVERED if entry.available else FileStatus.UNAVAILABLE,
                run_id=run_id,
                size_bytes=entry.size_bytes,
                error=entry.error,
            )
        return entry

    try:
        if worker_count == 1:
            discovered = [inspect(candidate) for candidate in candidates]
        else:
            with ThreadPoolExecutor(
                max_workers=worker_count,
                thread_name_prefix="tlc-catalog",
            ) as pool:
                futures = {pool.submit(inspect, candidate): candidate for candidate in candidates}
                for future in as_completed(futures):
                    try:
                        discovered.append(future.result())
                    except Exception as exc:
                        candidate = futures[future]
                        entry = TLCFile(
                            service=candidate.service,
                            year=candidate.year,
                            month=candidate.month,
                            url=candidate.url,
                            filename=candidate.filename,
                            size_bytes=None,
                            available=False,
                            discovered_at_utc=_utc_now(),
                            probe_method="ERROR",
                            error=f"{type(exc).__name__}: {exc}",
                        )
                        discovered.append(entry)
                        if audit is not None:
                            audit.record_file(
                                entry,
                                FileStatus.UNAVAILABLE,
                                run_id=run_id,
                                error=entry.error,
                            )
    finally:
        if owns_session:
            current_session.close()

    discovered.sort(key=lambda item: (item.year, item.service, item.month))
    historical_years = set(int(year) for year in source["historical_years"])
    missing_historical = [
        item for item in discovered if item.year in historical_years and not item.available
    ]
    if missing_historical and bool(source.get("require_complete_historical", True)):
        raise CatalogCompletenessError(missing_historical)

    if include_unavailable:
        return discovered
    return [item for item in discovered if item.available]


def save_catalog(entries: Iterable[TLCFile], path: str | os.PathLike[str]) -> Path:
    """Guarda el snapshot del catalogo de forma atomica."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    document = {
        "schema_version": 1,
        "generated_at_utc": _utc_now(),
        "files": [entry.to_dict() for entry in entries],
    }
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, destination)
    return destination


def load_catalog(path: str | os.PathLike[str]) -> list[TLCFile]:
    source = Path(path)
    try:
        document = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CatalogError(f"No se pudo leer el catalogo {source}: {exc}") from exc
    if not isinstance(document, dict) or not isinstance(document.get("files"), list):
        raise CatalogError(f"Formato de catalogo invalido: {source}")
    try:
        entries = [TLCFile.from_dict(item) for item in document["files"]]
    except (TypeError, ValueError) as exc:
        raise CatalogError(f"Entrada invalida en {source}: {exc}") from exc
    return entries
