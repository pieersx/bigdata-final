"""CLI reproducible para ejecutar y auditar el pipeline medallion TLC."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, is_dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

from .audit import AuditLogger
from .catalog import (
    TLCFile,
    discover_catalog,
    expected_catalog,
    load_catalog,
    save_catalog,
    session_from_config,
)
from .config import PipelineConfig, load_config
from .ingest import (
    bronze_destination,
    download_catalog,
    download_zone_lookup,
    partial_metadata_path,
    partial_path,
    sidecar_path,
)
from .verify import (
    VerificationReport,
    read_audit_events,
    save_verification_report,
    verify_project,
)


class CLIError(RuntimeError):
    """Error de uso o de una etapa del pipeline."""


class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() != "a":
            return
        for name, value in attrs:
            if name.casefold() == "href" and value:
                self.links.append(value)


def _json_safe(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _json_safe(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple | set):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)


def discover_official_html_links(
    config: PipelineConfig,
    *,
    session: Any | None = None,
    audit: AuditLogger | None = None,
    run_id: str | None = None,
) -> dict[str, str]:
    """Lee el HTML oficial y extrae los enlaces Parquet publicados por TLC."""

    official_page = str(config.require("source.official_page"))
    owns_session = session is None
    current_session = session or session_from_config(config)
    response = None
    try:
        response = current_session.get(
            official_page,
            timeout=float(config.get("source.discovery_timeout_seconds", 30)),
            allow_redirects=True,
        )
        status = int(response.status_code)
        if status < 200 or status >= 300:
            raise CLIError(f"HTTP {status} leyendo la pagina oficial TLC")
        html = getattr(response, "text", None)
        if not isinstance(html, str):
            content = getattr(response, "content", b"")
            html = bytes(content).decode("utf-8", errors="replace")
        if "trip record data" not in html.casefold():
            raise CLIError("La respuesta oficial no parece ser la pagina TLC Trip Record Data")

        parser = _LinkParser()
        parser.feed(html)
        links: dict[str, str] = {}
        for raw_link in parser.links:
            absolute = urljoin(official_page, raw_link)
            filename = Path(unquote(urlparse(absolute).path)).name
            if not filename.endswith(".parquet") or "_tripdata_" not in filename:
                continue
            previous = links.get(filename)
            if previous is not None and previous != absolute:
                raise CLIError(f"El HTML publica dos URLs para {filename}")
            links[filename] = absolute

        required = {
            entry.filename for entry in expected_catalog(config, include_current_year=False)
        }
        # Los acordeones del sitio pueden omitir anchors históricos en el HTML
        # inicial. discover_catalog valida esos archivos por metadata HTTP; el
        # HTML se usa como autoridad para el año corriente.
        historical_links = len(required.intersection(links))
        if audit is not None:
            audit.record_event(
                "source_page",
                "VALIDATED",
                run_id=run_id,
                details={
                    "url": official_page,
                    "http_status": status,
                    "trip_links": len(links),
                    "historical_links": historical_links,
                    "historical_links_expected": len(required),
                },
            )
        return links
    finally:
        if response is not None:
            response.close()
        if owns_session:
            current_session.close()


def _parse_list(values: Sequence[str] | None) -> list[str] | None:
    if values is None:
        return None
    parsed: list[str] = []
    for value in values:
        parsed.extend(part.strip() for part in value.split(",") if part.strip())
    return parsed


def _selection(
    config: PipelineConfig, args: argparse.Namespace
) -> tuple[tuple[int, ...], tuple[str, ...]]:
    configured_years = tuple(int(year) for year in config.get("source.historical_years", []))
    current_year = int(config.get("source.current_year"))
    allowed_years = {*configured_years, current_year}
    raw_years = _parse_list(getattr(args, "years", None))
    if raw_years is None:
        years = list(configured_years)
        if not getattr(args, "no_current", False):
            years.append(current_year)
    else:
        try:
            years = [int(year) for year in raw_years]
        except ValueError as exc:
            raise CLIError("--years solo acepta anios enteros") from exc
        unknown_years = sorted(set(years).difference(allowed_years))
        if unknown_years:
            raise CLIError(f"Anios fuera del alcance configurado: {unknown_years}")
        if getattr(args, "no_current", False) and current_year in years:
            raise CLIError("--no-current no puede combinarse con el anio corriente en --years")
        if getattr(args, "no_current", False):
            years = [year for year in years if year != current_year]

    configured_services = tuple(str(service) for service in config.get("source.services", []))
    raw_services = _parse_list(getattr(args, "services", None))
    services = (
        configured_services
        if raw_services is None
        else tuple(service.casefold() for service in raw_services)
    )
    unknown_services = sorted(set(services).difference(configured_services))
    if unknown_services:
        raise CLIError(f"Servicios fuera del alcance configurado: {unknown_services}")
    if not years or not services:
        raise CLIError("La seleccion de anios y servicios no puede quedar vacia")
    return tuple(dict.fromkeys(years)), tuple(dict.fromkeys(services))


def _catalog_path(config: PipelineConfig, override: str | None) -> Path:
    return (
        Path(override).expanduser().resolve()
        if override
        else config.path("bronze") / "_catalog.json"
    )


def _discover_selected_catalog(
    config: PipelineConfig,
    args: argparse.Namespace,
    *,
    session: Any,
    audit: AuditLogger,
    run_id: str,
) -> tuple[list[TLCFile], dict[str, Any]]:
    years, services = _selection(config, args)
    links = discover_official_html_links(
        config,
        session=session,
        audit=audit,
        run_id=run_id,
    )
    include_current = not getattr(args, "no_current", False)
    discovered = discover_catalog(
        config,
        session=session,
        audit=audit,
        run_id=run_id,
        include_current_year=include_current,
    )
    # Para el año corriente solo se aceptan archivos publicados en el HTML. Los
    # históricos se conservan porque sus acordeones no siempre exponen anchors.
    historical_years = set(config.get("source.historical_years", []))
    official = [
        entry
        for entry in discovered
        if entry.year in historical_years or entry.filename in links
    ]
    selected = [
        entry for entry in official if entry.year in set(years) and entry.service in set(services)
    ]
    expected_historical = sum(
        1
        for entry in expected_catalog(config, include_current_year=False)
        if entry.year in set(years) and entry.service in set(services)
    )
    observed_historical = sum(
        entry.year in set(config.get("source.historical_years", [])) for entry in selected
    )
    if observed_historical != expected_historical:
        raise CLIError(
            f"Catalogo seleccionado incompleto: historicos={observed_historical}, "
            f"esperados={expected_historical}"
        )
    snapshot = _catalog_path(config, getattr(args, "catalog_output", None))
    save_catalog(selected, snapshot)
    return selected, {
        "path": str(snapshot),
        "files": len(selected),
        "historical_files": observed_historical,
        "current_files": len(selected) - observed_historical,
        "official_links": len(links),
        "years": list(years),
        "services": list(services),
    }


def _load_selected_catalog(
    config: PipelineConfig,
    args: argparse.Namespace,
) -> tuple[list[TLCFile], dict[str, Any]]:
    """Carga un snapshot ya sondeado para no repetir cientos de solicitudes HEAD."""

    raw_path = getattr(args, "catalog_input", None)
    if not raw_path:
        raise CLIError("catalog_input no fue indicado")
    path = Path(raw_path).expanduser().resolve()
    entries = load_catalog(path)
    years, services = _selection(config, args)
    selected = [
        entry
        for entry in entries
        if entry.available and entry.year in set(years) and entry.service in set(services)
    ]
    historical_years = set(int(year) for year in config.get("source.historical_years", []))
    expected_historical = sum(
        1
        for entry in expected_catalog(config, include_current_year=False)
        if entry.year in set(years) and entry.service in set(services)
    )
    observed_historical = sum(entry.year in historical_years for entry in selected)
    if observed_historical != expected_historical:
        raise CLIError(
            f"Snapshot incompleto: historicos={observed_historical}, "
            f"esperados={expected_historical}"
        )
    if not selected:
        raise CLIError(f"El snapshot no contiene archivos para la selección: {path}")
    return selected, {
        "path": str(path),
        "files": len(selected),
        "historical_files": observed_historical,
        "current_files": len(selected) - observed_historical,
        "years": list(years),
        "services": list(services),
        "source": "snapshot",
    }


def _catalog_for_operation(
    config: PipelineConfig,
    args: argparse.Namespace,
    *,
    session: Any,
    audit: AuditLogger,
    run_id: str,
) -> tuple[list[TLCFile], dict[str, Any]]:
    if getattr(args, "catalog_input", None):
        catalog, summary = _load_selected_catalog(config, args)
        audit.record_event(
            "source_catalog",
            "VALIDATED",
            run_id=run_id,
            details=summary,
        )
        return catalog, summary
    return _discover_selected_catalog(
        config,
        args,
        session=session,
        audit=audit,
        run_id=run_id,
    )


def _remove_forced_files(entries: Iterable[TLCFile], config: PipelineConfig) -> int:
    removed = 0
    bronze_root = config.path("bronze")
    for entry in entries:
        destination = bronze_destination(bronze_root, entry) / entry.filename
        for candidate in (
            destination,
            sidecar_path(destination),
            partial_path(destination),
            partial_metadata_path(destination),
        ):
            if candidate.is_file():
                candidate.unlink()
                removed += 1
    zone = bronze_root / "reference" / "taxi_zone_lookup.csv"
    for candidate in (zone, sidecar_path(zone), partial_path(zone), partial_metadata_path(zone)):
        if candidate.is_file():
            candidate.unlink()
            removed += 1
    return removed


def _with_audit(
    config: PipelineConfig,
    pipeline_name: str,
    parameters: Mapping[str, Any],
    operation: Callable[[AuditLogger, str], dict[str, Any]],
) -> dict[str, Any]:
    with AuditLogger.from_config(config) as audit:
        run_id = audit.start_run(pipeline_name, parameters=parameters)
        try:
            result = operation(audit, run_id)
        except BaseException as exc:
            audit.finish_run(run_id, success=False, error=f"{type(exc).__name__}: {exc}")
            raise
        audit.finish_run(run_id, success=True, metrics=result)
        return result


def command_catalog(config: PipelineConfig, args: argparse.Namespace) -> dict[str, Any]:
    years, services = _selection(config, args)

    def execute(audit: AuditLogger, run_id: str) -> dict[str, Any]:
        session = session_from_config(config)
        try:
            _, summary = _discover_selected_catalog(
                config,
                args,
                session=session,
                audit=audit,
                run_id=run_id,
            )
            return summary
        finally:
            session.close()

    return _with_audit(
        config,
        "catalog",
        {"years": years, "services": services, "no_current": args.no_current},
        execute,
    )


def command_ingest(config: PipelineConfig, args: argparse.Namespace) -> dict[str, Any]:
    years, services = _selection(config, args)

    def execute(audit: AuditLogger, run_id: str) -> dict[str, Any]:
        session = session_from_config(config)
        try:
            catalog, catalog_summary = _catalog_for_operation(
                config,
                args,
                session=session,
                audit=audit,
                run_id=run_id,
            )
            removed = _remove_forced_files(catalog, config) if args.force else 0
            complete_scope = set(years) >= set(config.get("source.historical_years", [])) and set(
                services
            ) == set(config.get("source.services", []))
            downloads = download_catalog(
                catalog,
                config,
                session=session,
                audit=audit,
                run_id=run_id,
                enforce_historical_completeness=complete_scope,
            )
            zone = download_zone_lookup(
                config,
                session=session,
                audit=audit,
                run_id=run_id,
            )
            return {
                "catalog": catalog_summary,
                "downloads": len(downloads),
                "downloaded_bytes": sum(result.transferred_bytes for result in downloads),
                "skipped": sum(result.status == "SKIPPED" for result in downloads),
                "zone_lookup": zone.to_dict(),
                "force_removed_files": removed,
            }
        finally:
            session.close()

    return _with_audit(
        config,
        "ingest",
        {"years": years, "services": services, "force": args.force},
        execute,
    )


def _spark_session(config: PipelineConfig):
    from .spark_session import create_spark_session

    return create_spark_session(config)


def _transform_selected(
    spark: Any,
    config: PipelineConfig,
    *,
    years: Sequence[int],
    services: Sequence[str],
    audit: AuditLogger,
    run_id: str,
):
    from .transform import (
        TransformSummary,
        discover_bronze_files,
        transform_file_to_silver,
    )

    files = [
        item
        for item in discover_bronze_files(config.path("bronze"), services=services)
        if item.year in set(years)
    ]
    if not files:
        raise CLIError("No hay archivos Bronze para la seleccion solicitada")
    def transform(item: Any):
        return transform_file_to_silver(
            spark,
            item,
            config=config,
            audit=audit,
            run_id=run_id,
        )

    file_workers = min(int(config.get("spark.silver_file_workers", 2)), len(files))
    if file_workers <= 0:
        raise CLIError("spark.silver_file_workers debe ser positivo")
    if file_workers == 1:
        results = tuple(transform(item) for item in files)
    else:
        # Spark admite trabajos concurrentes enviados desde varios threads. La
        # lista de map conserva el orden determinista del catálogo y el logger
        # usa un RLock, por lo que el resumen global sigue siendo reproducible.
        with ThreadPoolExecutor(
            max_workers=file_workers,
            thread_name_prefix="tlc-silver-file",
        ) as pool:
            results = tuple(pool.map(transform, files))
    source_rows = sum(item.source_rows for item in results)
    silver_rows = sum(item.silver_rows for item in results)
    valid_rows = sum(item.valid_rows for item in results)
    quarantine_rows = sum(item.quarantine_rows for item in results)
    reconciled = (
        all(item.reconciled for item in results)
        and source_rows == silver_rows
        and source_rows == valid_rows + quarantine_rows
    )
    summary = TransformSummary(
        files=results,
        source_rows=source_rows,
        silver_rows=silver_rows,
        valid_rows=valid_rows,
        quarantine_rows=quarantine_rows,
        reconciled=reconciled,
    )
    audit.record_quality(
        {
            "result_id": f"{run_id}:silver:reconciliation",
            "status": "PASSED" if reconciled else "FAILED",
            "layer": "silver",
            **summary.to_dict(),
        },
        run_id=run_id,
    )
    if not reconciled:
        raise CLIError("Fallo la reconciliacion total Bronze/Silver")
    return summary


def command_silver(config: PipelineConfig, args: argparse.Namespace) -> dict[str, Any]:
    years, services = _selection(config, args)

    def execute(audit: AuditLogger, run_id: str) -> dict[str, Any]:
        spark = _spark_session(config)
        try:
            summary = _transform_selected(
                spark,
                config,
                years=years,
                services=services,
                audit=audit,
                run_id=run_id,
            )
            return summary.to_dict()
        finally:
            spark.stop()

    return _with_audit(
        config,
        "silver",
        {"years": years, "services": services},
        execute,
    )


def _build_gold(
    spark: Any,
    config: PipelineConfig,
    audit: AuditLogger,
    run_id: str,
    *,
    tables: Sequence[str] | None = None,
    silver_scope: tuple[int, int] | None = None,
):
    from .gold import build_gold_tables

    return build_gold_tables(
        spark,
        config,
        audit=audit,
        run_id=run_id,
        tables=tables,
        silver_scope=silver_scope,
    )


def command_gold(config: PipelineConfig, args: argparse.Namespace) -> dict[str, Any]:
    if (args.year is None) != (args.month is None):
        raise CLIError("--year y --month deben indicarse juntos")
    if args.month is not None and not 1 <= args.month <= 12:
        raise CLIError("--month debe estar entre 1 y 12")
    scope = (args.year, args.month) if args.year is not None else None

    def execute(audit: AuditLogger, run_id: str) -> dict[str, Any]:
        spark = _spark_session(config)
        try:
            return _build_gold(
                spark,
                config,
                audit,
                run_id,
                tables=args.tables,
                silver_scope=scope,
            ).to_dict()
        finally:
            spark.stop()

    return _with_audit(
        config,
        "gold",
        {"tables": args.tables or "all", "scope": scope or "all"},
        execute,
    )


def _train_models(spark: Any, config: PipelineConfig, audit: AuditLogger, run_id: str):
    try:
        from .models import run_models
    except ImportError as exc:
        raise CLIError(f"No se pudo importar tlc_pipeline.models.run_models: {exc}") from exc
    if not callable(run_models):
        raise CLIError("tlc_pipeline.models no expone run_models")
    return run_models(spark, config, audit=audit, run_id=run_id)


def _build_powerbi_contracts(spark: Any, config: PipelineConfig, audit: AuditLogger, run_id: str):
    from .powerbi import build_powerbi_contracts

    return build_powerbi_contracts(spark, config, audit=audit, run_id=run_id)


def _model_summary(results: Mapping[str, Any]) -> dict[str, Any]:
    return {
        name: {
            "model_name": result.model_name,
            "metrics": _json_safe(result.metrics),
            "paths": _json_safe(result.paths),
            "tables": sorted(result.frames),
        }
        for name, result in results.items()
    }


def command_models(config: PipelineConfig, _: argparse.Namespace) -> dict[str, Any]:
    def execute(audit: AuditLogger, run_id: str) -> dict[str, Any]:
        spark = _spark_session(config)
        try:
            return _model_summary(_train_models(spark, config, audit, run_id))
        finally:
            spark.stop()

    return _with_audit(config, "models", {}, execute)


def command_powerbi(config: PipelineConfig, _: argparse.Namespace) -> dict[str, Any]:
    """Regenera los contratos Power BI desde Gold, modelos y auditoría."""

    def execute(audit: AuditLogger, run_id: str) -> dict[str, Any]:
        # La exportación se hace antes de iniciar Spark para que D10 incluya los
        # eventos persistidos más recientes de todas las fases anteriores.
        audit_export = export_audit_events(config)
        spark = _spark_session(config)
        try:
            contracts = _build_powerbi_contracts(spark, config, audit, run_id)
            return {
                "audit_export": audit_export,
                "contracts": [item.to_dict() for item in contracts],
            }
        finally:
            spark.stop()

    return _with_audit(config, "powerbi", {}, execute)


_AUDIT_FIELDS = (
    "event_id",
    "event_type",
    "status",
    "timestamp_utc",
    "run_id",
    "filename",
    "service",
    "year",
    "month",
    "local_path",
    "size_bytes",
    "sha256",
    "error",
    "layer",
    "source_rows",
    "silver_rows",
    "valid_rows",
    "quarantine_rows",
    "reconciled",
    "model_name",
    "metrics_json",
    "details_json",
)


def _audit_row(event: Mapping[str, Any]) -> dict[str, Any]:
    details = event.get("details") if isinstance(event.get("details"), Mapping) else {}
    source = details.get("source") if isinstance(details.get("source"), Mapping) else {}
    row = {field: event.get(field) for field in _AUDIT_FIELDS}
    for field in (
        "filename",
        "service",
        "year",
        "month",
        "local_path",
        "size_bytes",
        "sha256",
        "error",
        "layer",
        "source_rows",
        "silver_rows",
        "valid_rows",
        "quarantine_rows",
        "reconciled",
        "model_name",
    ):
        row[field] = details.get(field, source.get(field))
    row["metrics_json"] = json.dumps(details.get("metrics", {}), ensure_ascii=False, sort_keys=True)
    row["details_json"] = json.dumps(details, ensure_ascii=False, sort_keys=True)
    return row


def export_audit_events(
    config: PipelineConfig,
    *,
    output: str | Path | None = None,
) -> dict[str, Any]:
    events = read_audit_events(config)
    csv_path = (
        Path(output).expanduser().resolve()
        if output
        else config.path("exports") / "audit_events.csv"
    )
    if csv_path.suffix.casefold() != ".csv":
        raise CLIError("El destino de audit-export debe terminar en .csv")
    json_path = csv_path.with_suffix(".json")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_csv = csv_path.with_name(f".{csv_path.name}.{uuid.uuid4().hex}.tmp")
    with temporary_csv.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=_AUDIT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(_audit_row(event) for event in events)
    os.replace(temporary_csv, csv_path)
    temporary_json = json_path.with_name(f".{json_path.name}.{uuid.uuid4().hex}.tmp")
    temporary_json.write_text(
        json.dumps(events, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary_json, json_path)
    return {"events": len(events), "csv": str(csv_path), "json": str(json_path)}


def command_audit_export(config: PipelineConfig, args: argparse.Namespace) -> dict[str, Any]:
    return export_audit_events(config, output=args.output)


def command_verify(config: PipelineConfig, args: argparse.Namespace) -> VerificationReport:
    report = verify_project(config, powerbi_path=args.powerbi_path)
    save_verification_report(report, config.path("exports") / "verification_report.json")
    return report


def command_full(config: PipelineConfig, args: argparse.Namespace) -> dict[str, Any]:
    years, services = _selection(config, args)
    audit = AuditLogger.from_config(config)
    run_id = audit.start_run(
        "full",
        parameters={
            "years": years,
            "services": services,
            "force": args.force,
            "no_current": args.no_current,
        },
    )
    spark = None
    session = session_from_config(config)
    try:
        catalog, catalog_summary = _catalog_for_operation(
            config,
            args,
            session=session,
            audit=audit,
            run_id=run_id,
        )
        removed = _remove_forced_files(catalog, config) if args.force else 0
        complete_scope = set(years) >= set(config.get("source.historical_years", [])) and set(
            services
        ) == set(config.get("source.services", []))
        downloads = download_catalog(
            catalog,
            config,
            session=session,
            audit=audit,
            run_id=run_id,
            enforce_historical_completeness=complete_scope,
        )
        zone = download_zone_lookup(
            config,
            session=session,
            audit=audit,
            run_id=run_id,
        )

        spark = _spark_session(config)
        silver = _transform_selected(
            spark,
            config,
            years=years,
            services=services,
            audit=audit,
            run_id=run_id,
        )
        gold = _build_gold(spark, config, audit, run_id)
        models = _train_models(spark, config, audit, run_id)
        export_before_verify = export_audit_events(config)
        powerbi_contracts = _build_powerbi_contracts(spark, config, audit, run_id)
        report = verify_project(config, powerbi_path=args.powerbi_path)
        save_verification_report(report, config.path("exports") / "verification_report.json")
        report.raise_for_failures()
        result = {
            "run_id": run_id,
            "catalog": catalog_summary,
            "downloads": len(downloads),
            "downloaded_bytes": sum(item.transferred_bytes for item in downloads),
            "zone_lookup": zone.to_dict(),
            "silver": silver.to_dict(),
            "gold": gold.to_dict(),
            "models": _model_summary(models),
            "powerbi_contracts": [item.to_dict() for item in powerbi_contracts],
            "audit_export": export_before_verify,
            "verification": report.to_dict(),
            "force_removed_files": removed,
        }
    except BaseException as exc:
        audit.finish_run(run_id, success=False, error=f"{type(exc).__name__}: {exc}")
        try:
            export_audit_events(config)
        except Exception:
            pass
        raise
    else:
        audit.finish_run(
            run_id,
            success=True,
            metrics={
                "downloads": len(downloads),
                "source_rows": silver.source_rows,
                "gold_tables": gold.tables_created,
                "models": len(models),
            },
        )
        result["audit_export"] = export_audit_events(config)
        return result
    finally:
        if spark is not None:
            spark.stop()
        session.close()
        audit.close()


def _add_selection_arguments(parser: argparse.ArgumentParser, *, force: bool = False) -> None:
    parser.add_argument("--years", nargs="+", help="Anios separados por espacio o coma")
    parser.add_argument("--services", nargs="+", help="yellow green fhv fhvhv")
    parser.add_argument("--no-current", action="store_true", help="Excluye 2026")
    if force:
        parser.add_argument("--force", action="store_true", help="Redescarga los archivos elegidos")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tlc-pipeline",
        description="Pipeline medallion completo para NYC TLC Trip Record Data",
    )
    parser.add_argument("--config", help="Ruta a config/pipeline.yaml")
    subparsers = parser.add_subparsers(dest="command", required=True)

    catalog = subparsers.add_parser("catalog", help="Descubre y guarda el catalogo oficial")
    _add_selection_arguments(catalog)
    catalog.add_argument("--catalog-output", help="Snapshot JSON de catalogo")

    ingest = subparsers.add_parser("ingest", help="Descarga Bronze y zone lookup")
    _add_selection_arguments(ingest, force=True)
    ingest.add_argument("--catalog-output", help="Snapshot JSON de catalogo")
    ingest.add_argument(
        "--catalog-input",
        help="Snapshot JSON previamente validado; evita repetir el sondeo del CDN",
    )

    silver = subparsers.add_parser("silver", help="Transforma y reconcilia Bronze -> Silver")
    _add_selection_arguments(silver)

    gold = subparsers.add_parser("gold", help="Construye todas o una selección Gold")
    gold.add_argument("--tables", nargs="+", help="Nombres de tablas Gold a materializar")
    gold.add_argument("--year", type=int, help="Año Silver para materialización incremental")
    gold.add_argument("--month", type=int, help="Mes Silver para materialización incremental")
    subparsers.add_parser("models", help="Entrena los tres modelos predictivos")
    subparsers.add_parser("powerbi", help="Genera los diez contratos CSV del PBIP")

    audit_export = subparsers.add_parser("audit-export", help="Exporta JSONL de auditoria")
    audit_export.add_argument("--output", help="Ruta CSV; tambien se genera JSON")

    verify = subparsers.add_parser("verify", help="Comprueba todos los requisitos del examen")
    verify.add_argument("--powerbi-path", help="Directorio que contiene el .pbip")

    full = subparsers.add_parser("full", help="Ejecuta ingesta, medallion, modelos y verificacion")
    _add_selection_arguments(full, force=True)
    full.add_argument("--catalog-output", help="Snapshot JSON de catalogo")
    full.add_argument(
        "--catalog-input",
        help="Snapshot JSON previamente validado; evita repetir el sondeo del CDN",
    )
    full.add_argument("--powerbi-path", help="Directorio que contiene el .pbip")
    return parser


_COMMANDS: dict[str, Callable[[PipelineConfig, argparse.Namespace], Any]] = {
    "catalog": command_catalog,
    "ingest": command_ingest,
    "silver": command_silver,
    "gold": command_gold,
    "models": command_models,
    "powerbi": command_powerbi,
    "audit-export": command_audit_export,
    "verify": command_verify,
    "full": command_full,
}


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        result = _COMMANDS[args.command](config, args)
        document = result.to_dict() if isinstance(result, VerificationReport) else result
        print(json.dumps(_json_safe(document), ensure_ascii=False, indent=2, sort_keys=True))
        if isinstance(result, VerificationReport) and not result.passed:
            return 2
        return 0
    except KeyboardInterrupt:
        print(json.dumps({"status": "CANCELLED"}, ensure_ascii=False), file=sys.stderr)
        return 130
    except Exception as exc:
        print(
            json.dumps(
                {"status": "FAILED", "error_type": type(exc).__name__, "error": str(exc)},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
