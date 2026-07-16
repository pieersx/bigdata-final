"""Verificacion integral y sin resultados simulados del proyecto TLC.

Las comprobaciones trabajan sobre los artefactos realmente persistidos. En
particular, los 144 Parquet historicos se leen de extremo a extremo para
recalcular SHA-256; no basta con que exista un nombre de archivo o una fila de
manifest.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .catalog import expected_catalog
from .config import PipelineConfig
from .ingest import PARQUET_MAGIC, bronze_destination, sidecar_path

REQUIRED_HISTORICAL_YEARS = (2023, 2024, 2025)
REQUIRED_SERVICES = ("yellow", "green", "fhv", "fhvhv")
EXPECTED_HISTORICAL_FILES = 144

REQUIRED_GOLD_TABLES = (
    "descriptive_daily_demand",
    "descriptive_hourly_profile",
    "descriptive_service_financials",
    "diagnostic_route_performance",
    "diagnostic_tip_factors",
    "diagnostic_daily_anomalies",
    "model_timeseries_daily",
    "model_segmentation_zones",
    "model_classification_demand",
)

REQUIRED_METRICS_TABLES = (
    "model_timeseries_metrics",
    "model_segmentation_metrics",
    "model_classification_metrics",
    "model_metrics",
)

REQUIRED_MODEL_ARTIFACTS = {
    "time_series_gbt": "time_series_forecast",
    "kmeans_zones": "zone_segmentation",
    "random_forest_high_demand": "high_demand_classifier",
}

REQUIRED_MODEL_METRICS = {
    "time_series_gbt": ("rmse", "mae", "r2", "train_rows", "test_rows"),
    "kmeans_zones": ("silhouette", "k", "zones", "clusters_produced"),
    "random_forest_high_demand": (
        "auc_roc",
        "accuracy",
        "f1",
        "train_rows",
        "test_rows",
    ),
}

REQUIRED_PBIP_TITLES = (
    "01 Resumen ejecutivo",
    "02 Demanda temporal",
    "03 Ingresos y tarifas",
    "04 Causas del cambio",
    "05 Rutas y congestión",
    "06 Propinas y anomalías",
    "07 Pronóstico de demanda",
    "08 Segmentación de zonas",
    "09 Clasificación de alta demanda",
    "10 Control y auditoría",
)

_TRIP_FILENAME = re.compile(
    r"^(yellow|green|fhv|fhvhv)_tripdata_(\d{4})-(0[1-9]|1[0-2])\.parquet$",
    re.IGNORECASE,
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class VerificationError(RuntimeError):
    """El proyecto no satisface uno o mas requisitos del examen."""


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    passed: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class VerificationReport:
    checks: tuple[CheckResult, ...]
    generated_at_utc: str

    @property
    def passed(self) -> bool:
        return bool(self.checks) and all(check.passed for check in self.checks)

    @property
    def failures(self) -> tuple[CheckResult, ...]:
        return tuple(check for check in self.checks if not check.passed)

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "generated_at_utc": self.generated_at_utc,
            "checks": [check.to_dict() for check in self.checks],
            "failure_count": len(self.failures),
        }

    def raise_for_failures(self) -> None:
        if self.passed:
            return
        summary = "; ".join(f"{check.name}: {check.message}" for check in self.failures)
        raise VerificationError(
            f"Verificacion incompleta ({len(self.failures)} controles fallidos): {summary}"
        )


def _normalise_text(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    without_marks = "".join(
        character for character in decomposed if not unicodedata.combining(character)
    )
    return re.sub(r"\s+", " ", without_marks).strip().casefold()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise VerificationError(f"No existe {path}") from exc
    except json.JSONDecodeError as exc:
        raise VerificationError(f"JSON invalido en {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise VerificationError(f"Se esperaba un objeto JSON en {path}")
    return value


def read_audit_events(config: PipelineConfig) -> list[dict[str, Any]]:
    """Lee y valida todas las lineas JSONL de auditoria."""

    path = Path(str(config.require("audit.local_jsonl"))).expanduser()
    if not path.is_file():
        raise VerificationError(f"No existe el respaldo de auditoria JSONL: {path}")
    events: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            if not raw_line.strip():
                continue
            try:
                event = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                raise VerificationError(
                    f"JSONL de auditoria invalido en linea {line_number}: {exc}"
                ) from exc
            if not isinstance(event, dict):
                raise VerificationError(
                    f"Evento de auditoria no es un objeto en linea {line_number}"
                )
            events.append(event)
    if not events:
        raise VerificationError(f"El respaldo de auditoria esta vacio: {path}")
    return events


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_magic(path: Path, *, parquet: bool) -> None:
    if not path.is_file() or path.stat().st_size <= 0:
        raise VerificationError(f"Archivo ausente o vacio: {path}")
    if not parquet:
        return
    if path.stat().st_size < 8:
        raise VerificationError(f"Parquet demasiado pequeno: {path}")
    with path.open("rb") as handle:
        prefix = handle.read(4)
        handle.seek(-4, 2)
        suffix = handle.read(4)
    if prefix != PARQUET_MAGIC or suffix != PARQUET_MAGIC:
        raise VerificationError(f"Magic bytes Parquet invalidos: {path}")


def _manifest_files(bronze_root: Path) -> dict[str, dict[str, Any]]:
    manifest_path = bronze_root / "_manifest.json"
    document = _read_json(manifest_path)
    files = document.get("files")
    if not isinstance(files, dict):
        raise VerificationError(f"Manifest sin mapping files: {manifest_path}")
    invalid = [url for url, value in files.items() if not isinstance(value, dict)]
    if invalid:
        raise VerificationError(f"Entradas invalidas en manifest: {invalid[:3]}")
    return files


def _verify_download_artifact(
    path: Path,
    *,
    expected_url: str,
    manifest: Mapping[str, Mapping[str, Any]],
    parquet: bool,
    recompute_checksums: bool,
) -> tuple[int, str]:
    _validate_magic(path, parquet=parquet)
    metadata_path = sidecar_path(path)
    metadata = _read_json(metadata_path)
    content_length = metadata.get("content_length", metadata.get("size_bytes"))
    if not isinstance(content_length, int) or content_length <= 0:
        raise VerificationError(f"Sidecar sin Content-Length valido: {metadata_path}")
    if path.stat().st_size != content_length or metadata.get("size_bytes") != content_length:
        raise VerificationError(
            f"Tamano/Content-Length inconsistente para {path.name}: "
            f"disco={path.stat().st_size}, sidecar={content_length}"
        )
    if metadata.get("url") != expected_url:
        raise VerificationError(f"URL incorrecta en sidecar de {path.name}")
    sidecar_checksum = str(metadata.get("sha256", "")).casefold()
    if not _SHA256.fullmatch(sidecar_checksum):
        raise VerificationError(f"SHA-256 invalido en sidecar de {path.name}")
    checksum = _sha256(path) if recompute_checksums else sidecar_checksum
    if checksum != sidecar_checksum:
        raise VerificationError(f"Checksum no coincide para {path.name}")

    manifest_entry = manifest.get(expected_url)
    if not isinstance(manifest_entry, Mapping):
        raise VerificationError(f"El manifest no contiene {path.name}")
    if manifest_entry.get("sha256") != checksum:
        raise VerificationError(f"Checksum del manifest no coincide para {path.name}")
    if manifest_entry.get("size_bytes") != content_length:
        raise VerificationError(f"Tamano del manifest no coincide para {path.name}")
    if str(manifest_entry.get("status")) not in {"VALIDATED", "SKIPPED"}:
        raise VerificationError(f"Estado no validado en manifest para {path.name}")
    return content_length, checksum


def _check_configuration(config: PipelineConfig) -> dict[str, Any]:
    years = tuple(int(year) for year in config.get("source.historical_years", []))
    services = tuple(str(service) for service in config.get("source.services", []))
    if set(years) != set(REQUIRED_HISTORICAL_YEARS) or len(years) != 3:
        raise VerificationError(f"Anios historicos requeridos: {REQUIRED_HISTORICAL_YEARS}")
    if set(services) != set(REQUIRED_SERVICES) or len(services) != 4:
        raise VerificationError(f"Servicios requeridos: {REQUIRED_SERVICES}")
    if int(config.get("source.current_year", 0)) != 2026:
        raise VerificationError("source.current_year debe ser 2026")
    expected = expected_catalog(config, include_current_year=False)
    if len(expected) != EXPECTED_HISTORICAL_FILES:
        raise VerificationError(
            f"El contrato debe producir {EXPECTED_HISTORICAL_FILES} archivos, "
            f"produce {len(expected)}"
        )
    return {"years": list(years), "services": list(services), "expected_files": len(expected)}


def _check_bronze(config: PipelineConfig, *, recompute_checksums: bool) -> dict[str, Any]:
    bronze_root = config.path("bronze")
    manifest = _manifest_files(bronze_root)
    expected = expected_catalog(config, include_current_year=False)
    observed_paths: set[Path] = set()
    bytes_total = 0
    for entry in expected:
        path = bronze_destination(bronze_root, entry) / entry.filename
        size, _ = _verify_download_artifact(
            path,
            expected_url=entry.url,
            manifest=manifest,
            parquet=True,
            recompute_checksums=recompute_checksums,
        )
        observed_paths.add(path.resolve())
        bytes_total += size

    if len(observed_paths) != EXPECTED_HISTORICAL_FILES:
        raise VerificationError(
            f"Se validaron {len(observed_paths)} historicos; "
            f"se requieren {EXPECTED_HISTORICAL_FILES}"
        )

    current_year = int(config.get("source.current_year"))
    base_url = str(config.get("source.base_url")).rstrip("/")
    current_files = 0
    trip_root = bronze_root / "trip_records"
    if trip_root.is_dir():
        for path in trip_root.rglob("*.parquet"):
            match = _TRIP_FILENAME.fullmatch(path.name)
            if match is None:
                raise VerificationError(f"Parquet TLC con nombre no reconocido: {path}")
            year = int(match.group(2))
            if year in REQUIRED_HISTORICAL_YEARS:
                continue
            if year != current_year:
                raise VerificationError(f"Anio no autorizado en Bronze: {path.name}")
            expected_url = f"{base_url}/{path.name}"
            _verify_download_artifact(
                path,
                expected_url=expected_url,
                manifest=manifest,
                parquet=True,
                recompute_checksums=recompute_checksums,
            )
            current_files += 1

    return {
        "historical_files": len(observed_paths),
        "current_files": current_files,
        "historical_bytes": bytes_total,
        "manifest_entries": len(manifest),
        "checksums_recomputed": recompute_checksums,
    }


def _check_zone_lookup(config: PipelineConfig, *, recompute_checksums: bool) -> dict[str, Any]:
    bronze_root = config.path("bronze")
    manifest = _manifest_files(bronze_root)
    path = bronze_root / "reference" / "taxi_zone_lookup.csv"
    size, checksum = _verify_download_artifact(
        path,
        expected_url=str(config.get("source.zone_lookup_url")),
        manifest=manifest,
        parquet=False,
        recompute_checksums=recompute_checksums,
    )
    header = path.open("r", encoding="utf-8-sig").readline().casefold()
    if "locationid" not in header.replace("_", "") or "borough" not in header:
        raise VerificationError("taxi_zone_lookup.csv no contiene LocationID y Borough")
    return {"path": str(path), "size_bytes": size, "sha256": checksum}


def _latest_event_details(
    events: Sequence[Mapping[str, Any]],
    *,
    event_type: str,
    predicate: Callable[[Mapping[str, Any]], bool],
) -> dict[str, Any]:
    matching: list[Mapping[str, Any]] = []
    for event in events:
        details = event.get("details")
        if (
            event.get("event_type") == event_type
            and isinstance(details, Mapping)
            and predicate(details)
        ):
            matching.append(event)
    if not matching:
        raise VerificationError(f"No hay evento {event_type} que cumpla el contrato")
    latest = max(matching, key=lambda event: str(event.get("timestamp_utc", "")))
    return dict(latest["details"])


def _integer(details: Mapping[str, Any], name: str) -> int:
    value = details.get(name)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise VerificationError(f"Metrica {name} ausente o no numerica")
    return int(value)


def _check_reconciliation(
    config: PipelineConfig,
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    summary = _latest_event_details(
        events,
        event_type="quality_result",
        predicate=lambda details: details.get("layer") == "silver"
        and isinstance(details.get("files_processed"), int | float),
    )
    source_rows = _integer(summary, "source_rows")
    silver_rows = _integer(summary, "silver_rows")
    valid_rows = _integer(summary, "valid_rows")
    quarantine_rows = _integer(summary, "quarantine_rows")
    files_processed = _integer(summary, "files_processed")
    if source_rows <= 0:
        raise VerificationError("La reconciliacion Silver reporta cero filas")
    if not bool(summary.get("reconciled")):
        raise VerificationError("La reconciliacion Silver esta marcada como fallida")
    if source_rows != silver_rows or source_rows != valid_rows + quarantine_rows:
        raise VerificationError(
            "Conteos no reconciliados: "
            f"source={source_rows}, silver={silver_rows}, valid={valid_rows}, "
            f"quarantine={quarantine_rows}"
        )
    if files_processed < EXPECTED_HISTORICAL_FILES:
        raise VerificationError(
            f"Silver proceso {files_processed} archivos; "
            f"requiere al menos {EXPECTED_HISTORICAL_FILES}"
        )

    files = summary.get("files")
    if not isinstance(files, list):
        raise VerificationError("La auditoria de reconciliacion no contiene el detalle por archivo")
    historical: dict[tuple[str, int, int], Mapping[str, Any]] = {}
    historical_years = set(REQUIRED_HISTORICAL_YEARS)
    for item in files:
        if not isinstance(item, Mapping) or int(item.get("year", 0)) not in historical_years:
            continue
        key = (str(item.get("service")), int(item["year"]), int(item.get("month", 0)))
        if key in historical:
            raise VerificationError(f"Detalle Silver duplicado para {key}")
        if not bool(item.get("reconciled")):
            raise VerificationError(f"Archivo Silver no reconciliado: {key}")
        item_source = _integer(item, "source_rows")
        item_silver = _integer(item, "silver_rows")
        item_valid = _integer(item, "valid_rows")
        item_quarantine = _integer(item, "quarantine_rows")
        if (
            item_source <= 0
            or item_source != item_silver
            or item_source != item_valid + item_quarantine
        ):
            raise VerificationError(f"Conteos Silver invalidos para {key}")
        historical[key] = item
    if len(historical) != EXPECTED_HISTORICAL_FILES:
        raise VerificationError(
            f"Auditoria Silver contiene {len(historical)} historicos; requiere 144"
        )
    return {
        "files_processed": files_processed,
        "historical_files_reconciled": len(historical),
        "source_rows": source_rows,
        "silver_rows": silver_rows,
        "valid_rows": valid_rows,
        "quarantine_rows": quarantine_rows,
    }


def _parquet_parts(path: Path) -> list[Path]:
    if path.is_file() and path.suffix.casefold() == ".parquet":
        candidates = [path]
    elif path.is_dir():
        candidates = sorted(
            candidate for candidate in path.rglob("*.parquet") if candidate.is_file()
        )
    else:
        candidates = []
    if not candidates:
        raise VerificationError(f"Dataset Parquet ausente o vacio: {path}")
    for candidate in candidates:
        _validate_magic(candidate, parquet=True)
    return candidates


def _gold_audit_rows(
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    latest: dict[str, Mapping[str, Any]] = {}
    for event in events:
        details = event.get("details")
        if event.get("event_type") != "quality_result" or not isinstance(details, Mapping):
            continue
        if details.get("layer") != "gold" or not isinstance(details.get("name"), str):
            continue
        latest[str(details["name"])] = details
    return latest


def _check_gold(
    config: PipelineConfig,
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    gold_root = config.path("gold")
    exports_root = config.path("exports")
    require_full_exports = bool(config.get("gold.export_full_tables", True))
    audited = _gold_audit_rows(events)
    rows_total = 0
    for table in REQUIRED_GOLD_TABLES:
        _parquet_parts(gold_root / table)
        if require_full_exports:
            _parquet_parts(exports_root / f"{table}.parquet")
            csv_path = exports_root / f"{table}.csv"
            if not csv_path.is_file() or csv_path.stat().st_size <= 1:
                raise VerificationError(f"Export CSV ausente o vacio: {csv_path}")
        audit_result = audited.get(table)
        if not isinstance(audit_result, Mapping):
            raise VerificationError(f"No hay auditoria Gold para {table}")
        rows = _integer(audit_result, "rows")
        if rows <= 0 or audit_result.get("status") != "PASSED":
            raise VerificationError(f"Tabla Gold {table} no tiene filas validadas")
        rows_total += rows
    return {
        "tables": len(REQUIRED_GOLD_TABLES),
        "audited_rows_total": rows_total,
        "full_duplicate_exports": require_full_exports,
    }


def _number(metrics: Mapping[str, Any], name: str) -> float:
    value = metrics.get(name)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise VerificationError(f"Metrica predictiva {name} ausente o no numerica")
    number = float(value)
    if not math.isfinite(number):
        raise VerificationError(f"Metrica predictiva {name} no es finita")
    return number


def _model_audit(events: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    latest: dict[str, Mapping[str, Any]] = {}
    for event in events:
        details = event.get("details")
        if event.get("event_type") != "model_run" or not isinstance(details, Mapping):
            continue
        model_name = details.get("model_name")
        if isinstance(model_name, str):
            latest[model_name] = details
    return latest


def _check_models(
    config: PipelineConfig,
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    gold_root = config.path("gold")
    models_root = config.path("models")
    for table in REQUIRED_METRICS_TABLES:
        _parquet_parts(gold_root / table)

    audited = _model_audit(events)
    metric_counts: dict[str, int] = {}
    for model_name, relative_path in REQUIRED_MODEL_ARTIFACTS.items():
        artifact = models_root / relative_path
        if not artifact.is_dir() or not any(path.is_file() for path in artifact.rglob("*")):
            raise VerificationError(f"Modelo Spark ausente o vacio: {artifact}")
        result = audited.get(model_name)
        if not isinstance(result, Mapping) or result.get("status") != "SUCCEEDED":
            raise VerificationError(f"No hay auditoria exitosa para {model_name}")
        metrics = result.get("metrics")
        if not isinstance(metrics, Mapping):
            raise VerificationError(f"Auditoria sin metricas para {model_name}")
        for metric_name in REQUIRED_MODEL_METRICS[model_name]:
            _number(metrics, metric_name)
        metric_counts[model_name] = len(metrics)

    forecast = audited["time_series_gbt"]["metrics"]
    if _number(forecast, "rmse") < 0 or _number(forecast, "mae") < 0:
        raise VerificationError("RMSE/MAE no pueden ser negativos")
    if _number(forecast, "train_rows") <= 0 or _number(forecast, "test_rows") <= 0:
        raise VerificationError("Serie de tiempo no tiene train/test validos")

    segmentation = audited["kmeans_zones"]["metrics"]
    silhouette = _number(segmentation, "silhouette")
    if not -1 <= silhouette <= 1:
        raise VerificationError("Silhouette debe estar entre -1 y 1")
    if _number(segmentation, "k") < 2 or _number(segmentation, "zones") <= 0:
        raise VerificationError("Segmentacion no tiene k/zonas validos")

    classification = audited["random_forest_high_demand"]["metrics"]
    for metric in ("auc_roc", "accuracy", "f1"):
        value = _number(classification, metric)
        if not 0 <= value <= 1:
            raise VerificationError(f"{metric} debe estar entre 0 y 1")
    if _number(classification, "train_rows") <= 0 or _number(classification, "test_rows") <= 0:
        raise VerificationError("Clasificacion no tiene train/test validos")
    return {"models": len(audited), "metric_counts": metric_counts}


def _check_audit_flow(
    config: PipelineConfig,
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    event_types = {str(event.get("event_type")) for event in events}
    required_types = {"pipeline_run", "file_status", "quality_result", "model_run"}
    missing = required_types.difference(event_types)
    if missing:
        raise VerificationError(f"Auditoria sin tipos de evento: {sorted(missing)}")
    file_statuses = {
        str(event.get("status")) for event in events if event.get("event_type") == "file_status"
    }
    if not file_statuses.intersection({"VALIDATED", "SKIPPED"}):
        raise VerificationError("Auditoria sin archivos validados")
    exports_root = config.path("exports")
    csv_path = exports_root / "audit_events.csv"
    json_path = exports_root / "audit_events.json"
    if not csv_path.is_file() or csv_path.stat().st_size <= 1:
        raise VerificationError(f"Export de auditoria ausente: {csv_path}")
    if not json_path.is_file() or json_path.stat().st_size <= 2:
        raise VerificationError(f"Export JSON de auditoria ausente: {json_path}")
    exported = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(exported, list) or not exported:
        raise VerificationError("Export JSON de auditoria vacio o invalido")
    return {
        "events": len(events),
        "event_types": sorted(event_types),
        "file_statuses": sorted(file_statuses),
        "exported_events": len(exported),
    }


def _powerbi_root(config: PipelineConfig, override: str | Path | None) -> Path:
    if override is not None:
        return Path(override).expanduser().resolve()
    return (config.config_file.parent.parent / "powerbi").resolve()


def _check_powerbi(config: PipelineConfig, *, powerbi_path: str | Path | None) -> dict[str, Any]:
    root = _powerbi_root(config, powerbi_path)
    pbip_files = sorted(root.glob("*.pbip")) if root.is_dir() else []
    if len(pbip_files) != 1:
        raise VerificationError(
            f"Se esperaba un unico .pbip en {root}; encontrados={len(pbip_files)}"
        )
    pbip = _read_json(pbip_files[0])
    artifacts = pbip.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise VerificationError("PBIP no declara artefactos")
    report_paths = [
        item.get("report", {}).get("path")
        for item in artifacts
        if isinstance(item, Mapping) and isinstance(item.get("report"), Mapping)
    ]
    report_paths = [path for path in report_paths if isinstance(path, str)]
    if len(report_paths) != 1:
        raise VerificationError("PBIP debe declarar exactamente un reporte")
    report_root = root / report_paths[0]
    definition = _read_json(report_root / "definition.pbir")
    dataset_reference = definition.get("datasetReference")
    if not isinstance(dataset_reference, Mapping):
        raise VerificationError("definition.pbir no contiene datasetReference")

    pages_root = report_root / "definition" / "pages"
    metadata = _read_json(pages_root / "pages.json")
    page_order = metadata.get("pageOrder")
    if not isinstance(page_order, list) or len(page_order) != 10:
        raise VerificationError(
            f"PBIP debe tener 10 paginas en pageOrder; tiene {len(page_order or [])}"
        )
    if len(set(page_order)) != 10 or not all(isinstance(page, str) for page in page_order):
        raise VerificationError("pageOrder contiene IDs duplicados o invalidos")

    titles: list[str] = []
    visual_counts: dict[str, int] = {}
    for page_id in page_order:
        page_root = pages_root / page_id
        page = _read_json(page_root / "page.json")
        if page.get("name") != page_id:
            raise VerificationError(f"page.json no coincide con pageOrder: {page_id}")
        title = page.get("displayName")
        if not isinstance(title, str) or not title.strip():
            raise VerificationError(f"Pagina sin displayName: {page_id}")
        visuals = sorted((page_root / "visuals").glob("*/visual.json"))
        if len(visuals) < 4:
            raise VerificationError(f"Pagina '{title}' tiene {len(visuals)} visuales; requiere 4")
        for visual in visuals:
            _read_json(visual)
        titles.append(title.strip())
        visual_counts[title.strip()] = len(visuals)

    expected_titles = {_normalise_text(title) for title in REQUIRED_PBIP_TITLES}
    observed_titles = {_normalise_text(title) for title in titles}
    if observed_titles != expected_titles:
        missing = sorted(expected_titles.difference(observed_titles))
        extra = sorted(observed_titles.difference(expected_titles))
        raise VerificationError(
            f"Titulos PBIP no cumplen el contrato; faltan={missing}, extra={extra}"
        )
    semantic_model = root / "TLC_BigData.SemanticModel" / "definition.pbism"
    _read_json(semantic_model)
    contracts_root = config.path("exports") / "powerbi"
    contracts: list[str] = []
    for table in (
        "D01_Resumen",
        "D02_Demanda",
        "D03_Ingresos",
        "D04_Causas",
        "D05_Rutas",
        "D06_Anomalias",
        "D07_Pronostico",
        "D08_Segmentacion",
        "D09_Clasificacion",
        "D10_Auditoria",
    ):
        contract = contracts_root / f"{table}.csv"
        if not contract.is_file() or contract.stat().st_size <= 1:
            raise VerificationError(f"Contrato Power BI ausente o vacío: {contract}")
        text = contract.read_text(encoding="utf-8", errors="replace")
        if "PLACEHOLDER" in text:
            raise VerificationError(f"Contrato Power BI aún contiene datos placeholder: {contract}")
        contracts.append(str(contract))
    return {
        "pbip": str(pbip_files[0]),
        "pages": len(page_order),
        "visuals": visual_counts,
        "contracts": contracts,
    }


def _run_check(name: str, operation: Callable[[], dict[str, Any]]) -> CheckResult:
    try:
        details = operation()
    except Exception as exc:
        return CheckResult(
            name=name,
            passed=False,
            message=f"{type(exc).__name__}: {exc}",
            details={},
        )
    return CheckResult(name=name, passed=True, message="OK", details=details)


def verify_project(
    config: PipelineConfig,
    *,
    powerbi_path: str | Path | None = None,
    recompute_checksums: bool = True,
) -> VerificationReport:
    """Ejecuta todos los controles y devuelve cada fallo, no solo el primero."""

    try:
        events = read_audit_events(config)
        events_error: Exception | None = None
    except Exception as exc:
        events = []
        events_error = exc

    def audited(operation: Callable[[Sequence[Mapping[str, Any]]], dict[str, Any]]):
        def execute() -> dict[str, Any]:
            if events_error is not None:
                raise events_error
            return operation(events)

        return execute

    checks = (
        _run_check("configuration_contract", lambda: _check_configuration(config)),
        _run_check(
            "bronze_144_manifest_sidecars_checksums",
            lambda: _check_bronze(config, recompute_checksums=recompute_checksums),
        ),
        _run_check(
            "zone_lookup",
            lambda: _check_zone_lookup(config, recompute_checksums=recompute_checksums),
        ),
        _run_check(
            "silver_row_reconciliation",
            audited(lambda audit_events: _check_reconciliation(config, audit_events)),
        ),
        _run_check(
            "nine_gold_tables",
            audited(lambda audit_events: _check_gold(config, audit_events)),
        ),
        _run_check(
            "three_predictive_models_and_metrics",
            audited(lambda audit_events: _check_models(config, audit_events)),
        ),
        _run_check(
            "audit_flow_and_export",
            audited(lambda audit_events: _check_audit_flow(config, audit_events)),
        ),
        _run_check(
            "powerbi_ten_pages",
            lambda: _check_powerbi(config, powerbi_path=powerbi_path),
        ),
    )
    return VerificationReport(
        checks=checks,
        generated_at_utc=datetime.now(UTC).isoformat(timespec="milliseconds"),
    )


def save_verification_report(
    report: VerificationReport,
    destination: str | Path,
) -> Path:
    """Persiste evidencia JSON legible de todos los controles ejecutados."""

    path = Path(destination).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(report.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)
    return path


# Alias corto para CLI, notebooks y pruebas de entrega.
verify = verify_project
