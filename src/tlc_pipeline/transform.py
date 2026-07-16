"""Transformacion Bronze -> Silver para las cuatro familias NYC TLC.

El procesamiento es deliberadamente secuencial por archivo mensual: evita que
Spark intente materializar a la vez esquemas heterogeneos y limita el espacio
temporal necesario. Silver conserva cada fila de Bronze; los controles de
calidad son columnas y las filas invalidas se copian, no se mueven, a la capa
de cuarentena.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .utils import (
    column_key,
    config_get,
    configured_path,
    hive_partition_path,
    quote_spark_identifier,
    safe_remove_tree,
)

TLC_SERVICES = ("yellow", "green", "fhv", "fhvhv")
BRONZE_FILENAME_RE = re.compile(
    r"^(yellow|green|fhv|fhvhv)_tripdata_(\d{4})-(0[1-9]|1[0-2])\.parquet$",
    re.IGNORECASE,
)
PARTITION_COLUMNS = ("service", "year", "month")


@dataclass(frozen=True, slots=True)
class BronzeFile:
    path: Path
    service: str
    year: int
    month: int

    @property
    def filename(self) -> str:
        return self.path.name


@dataclass(frozen=True, slots=True)
class FileTransformResult:
    source_file: str
    service: str
    year: int
    month: int
    source_rows: int
    silver_rows: int
    valid_rows: int
    quarantine_rows: int
    reconciled: bool
    silver_partition: str
    quarantine_partition: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class TransformSummary:
    files: tuple[FileTransformResult, ...]
    source_rows: int
    silver_rows: int
    valid_rows: int
    quarantine_rows: int
    reconciled: bool

    @property
    def files_processed(self) -> int:
        return len(self.files)

    def to_dict(self) -> dict[str, Any]:
        document = asdict(self)
        document["files_processed"] = self.files_processed
        return document


def parse_bronze_filename(path: str | Path) -> BronzeFile:
    """Extrae servicio y periodo exclusivamente del nombre oficial TLC."""

    source = Path(path)
    match = BRONZE_FILENAME_RE.fullmatch(source.name)
    if match is None:
        raise ValueError(f"Nombre Bronze no reconocido: {source.name}")
    return BronzeFile(
        path=source.resolve(),
        service=match.group(1).lower(),
        year=int(match.group(2)),
        month=int(match.group(3)),
    )


def discover_bronze_files(
    bronze_path: str | Path,
    *,
    services: Iterable[str] | None = None,
) -> list[BronzeFile]:
    """Descubre recursivamente todos los Parquet mensuales sin muestreo."""

    root = Path(bronze_path).expanduser().resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"No existe el directorio Bronze: {root}")
    selected = {service.casefold() for service in (services or TLC_SERVICES)}
    unknown = selected.difference(TLC_SERVICES)
    if unknown:
        raise ValueError(f"Servicios TLC no soportados: {', '.join(sorted(unknown))}")

    discovered: list[BronzeFile] = []
    seen_partitions: dict[tuple[str, int, int], Path] = {}
    for candidate in root.rglob("*.parquet"):
        if not candidate.is_file():
            continue
        match = BRONZE_FILENAME_RE.fullmatch(candidate.name)
        if match is None or match.group(1).casefold() not in selected:
            continue
        item = parse_bronze_filename(candidate)
        key = (item.service, item.year, item.month)
        previous = seen_partitions.get(key)
        if previous is not None:
            raise ValueError(
                "Hay mas de un archivo Bronze para la misma particion "
                f"{key}: {previous} y {item.path}"
            )
        seen_partitions[key] = item.path
        discovered.append(item)

    service_order = {name: position for position, name in enumerate(TLC_SERVICES)}
    return sorted(
        discovered,
        key=lambda item: (item.year, item.month, service_order[item.service], str(item.path)),
    )


class _ColumnResolver:
    """Resuelve las variaciones historicas de casing del esquema TLC."""

    def __init__(self, dataframe: Any) -> None:
        self._columns: dict[str, str] = {}
        for column in dataframe.columns:
            self._columns.setdefault(column_key(column), column)

    def expression(self, functions: Any, aliases: Sequence[str], data_type: str):
        for alias in aliases:
            actual = self._columns.get(column_key(alias))
            if actual is not None:
                return functions.col(quote_spark_identifier(actual)).cast(data_type)
        return functions.lit(None).cast(data_type)


def _sum_when_present(functions: Any, columns: Sequence[Any]):
    present = columns[0].isNotNull()
    total = functions.coalesce(columns[0], functions.lit(0.0))
    for column in columns[1:]:
        present = present | column.isNotNull()
        total = total + functions.coalesce(column, functions.lit(0.0))
    return functions.when(present, total).otherwise(functions.lit(None).cast("double"))


def canonicalize_trip_data(
    dataframe: Any,
    *,
    service: str,
    year: int,
    month: int,
    source_file: str | None = None,
    source_path: str | None = None,
    config: Any | None = None,
):
    """Proyecta cualquier familia TLC al esquema Silver canonico.

    La operacion es una proyeccion uno-a-uno: no hay ``filter``, ``dropDuplicates``
    ni ``sample``. Las columnas inexistentes se agregan como nulos tipados, lo
    cual hace compatible el cargo CBD nuevo con periodos historicos.
    """

    from pyspark.sql import functions as F

    selected_service = service.casefold()
    if selected_service not in TLC_SERVICES:
        raise ValueError(f"Servicio TLC no soportado: {service}")
    if not 1 <= int(month) <= 12:
        raise ValueError(f"Mes invalido: {month}")

    resolver = _ColumnResolver(dataframe)
    pickup_aliases = {
        "yellow": ("tpep_pickup_datetime", "pickup_datetime"),
        "green": ("lpep_pickup_datetime", "pickup_datetime"),
        "fhv": ("pickup_datetime",),
        "fhvhv": ("pickup_datetime",),
    }[selected_service]
    dropoff_aliases = {
        "yellow": ("tpep_dropoff_datetime", "dropoff_datetime"),
        "green": ("lpep_dropoff_datetime", "dropoff_datetime"),
        "fhv": ("dropoff_datetime", "dropOff_datetime"),
        "fhvhv": ("dropoff_datetime", "dropOff_datetime"),
    }[selected_service]

    def source(aliases: str | Sequence[str], data_type: str):
        names = (aliases,) if isinstance(aliases, str) else tuple(aliases)
        return resolver.expression(F, names, data_type)

    canonical = dataframe.select(
        F.lit(source_file).cast("string").alias("source_file"),
        F.lit(source_path).cast("string").alias("source_path"),
        F.lit(selected_service).alias("service"),
        F.lit(int(year)).cast("int").alias("year"),
        F.lit(int(month)).cast("int").alias("month"),
        source(pickup_aliases, "timestamp").alias("pickup_datetime"),
        source(dropoff_aliases, "timestamp").alias("dropoff_datetime"),
        source("request_datetime", "timestamp").alias("request_datetime"),
        source("on_scene_datetime", "timestamp").alias("on_scene_datetime"),
        source("vendor_id", "int").alias("vendor_id"),
        source("hvfhs_license_num", "string").alias("hvfhs_license_num"),
        source("dispatching_base_num", "string").alias("dispatching_base_num"),
        source("originating_base_num", "string").alias("originating_base_num"),
        source(("affiliated_base_number", "affiliated_base_num"), "string").alias(
            "affiliated_base_number"
        ),
        source(("pu_location_id", "pulocationid"), "int").alias("pickup_location_id"),
        source(("do_location_id", "dolocationid"), "int").alias("dropoff_location_id"),
        source("passenger_count", "int").alias("passenger_count"),
        source(("trip_distance", "trip_miles"), "double").alias("trip_distance"),
        source("trip_time", "long").alias("reported_trip_time_seconds"),
        source(("rate_code_id", "ratecodeid"), "int").alias("rate_code_id"),
        source("store_and_fwd_flag", "string").alias("store_and_fwd_flag"),
        source("payment_type", "int").alias("payment_type"),
        source("trip_type", "int").alias("trip_type"),
        source("fare_amount", "double").alias("fare_amount"),
        source("base_passenger_fare", "double").alias("base_passenger_fare"),
        source("extra", "double").alias("extra"),
        source("mta_tax", "double").alias("mta_tax"),
        source(("tip_amount", "tips"), "double").alias("tip_amount"),
        source(("tolls_amount", "tolls"), "double").alias("tolls_amount"),
        source("ehail_fee", "double").alias("ehail_fee"),
        source("improvement_surcharge", "double").alias("improvement_surcharge"),
        source("total_amount", "double").alias("_source_total_amount"),
        source("congestion_surcharge", "double").alias("congestion_surcharge"),
        source("airport_fee", "double").alias("airport_fee"),
        source("cbd_congestion_fee", "double").alias("cbd_congestion_fee"),
        source("bcf", "double").alias("bcf"),
        source("sales_tax", "double").alias("sales_tax"),
        source("driver_pay", "double").alias("driver_pay"),
        source("shared_request_flag", "string").alias("shared_request_flag"),
        source("shared_match_flag", "string").alias("shared_match_flag"),
        source("access_a_ride_flag", "string").alias("access_a_ride_flag"),
        source("wav_request_flag", "string").alias("wav_request_flag"),
        source("wav_match_flag", "string").alias("wav_match_flag"),
        source(("sr_flag", "shared_ride_flag"), "string").alias("shared_ride_flag"),
    )

    fhvhv_total = _sum_when_present(
        F,
        (
            F.col("base_passenger_fare"),
            F.col("tolls_amount"),
            F.col("bcf"),
            F.col("sales_tax"),
            F.col("congestion_surcharge"),
            F.col("airport_fee"),
            F.col("cbd_congestion_fee"),
            F.col("tip_amount"),
        ),
    )
    canonical = (
        canonical.withColumn(
            "fare_amount", F.coalesce(F.col("fare_amount"), F.col("base_passenger_fare"))
        )
        .withColumn("total_amount", F.coalesce(F.col("_source_total_amount"), fhvhv_total))
        .drop("_source_total_amount")
        .withColumn(
            "trip_duration_minutes",
            (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long"))
            / F.lit(60.0),
        )
    )

    location_min = int(config_get(config or {}, "quality.valid_location_min", 1))
    location_max = int(config_get(config or {}, "quality.valid_location_max", 265))
    max_duration = float(config_get(config or {}, "quality.max_trip_duration_minutes", 1_440))
    max_distance = float(config_get(config or {}, "quality.max_trip_distance_miles", 500))
    max_total = float(config_get(config or {}, "quality.max_total_amount", 10_000))

    checks = (
        ("dq_pickup_datetime_valid", F.col("pickup_datetime").isNotNull(), "PICKUP_DATETIME"),
        (
            "dq_dropoff_datetime_valid",
            F.col("dropoff_datetime").isNotNull(),
            "DROPOFF_DATETIME",
        ),
        (
            "dq_period_valid",
            F.col("pickup_datetime").isNotNull()
            & (F.year("pickup_datetime") == F.lit(int(year)))
            & (F.month("pickup_datetime") == F.lit(int(month))),
            "PICKUP_OUTSIDE_FILE_PERIOD",
        ),
        (
            "dq_duration_valid",
            F.col("trip_duration_minutes").isNotNull()
            & (F.col("trip_duration_minutes") > F.lit(0.0))
            & (F.col("trip_duration_minutes") <= F.lit(max_duration)),
            "TRIP_DURATION",
        ),
        (
            "dq_pickup_location_valid",
            F.col("pickup_location_id").isNotNull()
            & F.col("pickup_location_id").between(location_min, location_max),
            "PICKUP_LOCATION",
        ),
        (
            "dq_dropoff_location_valid",
            F.col("dropoff_location_id").isNotNull()
            & F.col("dropoff_location_id").between(location_min, location_max),
            "DROPOFF_LOCATION",
        ),
        (
            "dq_distance_valid",
            F.lit(True)
            if selected_service == "fhv"
            else (
                F.col("trip_distance").isNotNull()
                & F.col("trip_distance").between(0.0, max_distance)
            ),
            "TRIP_DISTANCE",
        ),
        (
            "dq_total_amount_valid",
            F.lit(True)
            if selected_service == "fhv"
            else (
                F.col("total_amount").isNotNull()
                & (F.abs(F.col("total_amount")) <= F.lit(max_total))
            ),
            "TOTAL_AMOUNT",
        ),
    )
    for name, condition, _ in checks:
        canonical = canonical.withColumn(name, condition.cast("boolean"))

    errors = F.filter(
        F.array(
            *(
                F.when(~F.col(name), F.lit(error)).otherwise(F.lit(None).cast("string"))
                for name, _, error in checks
            )
        ),
        lambda item: item.isNotNull(),
    )
    return (
        canonical.withColumn("dq_errors", errors)
        .withColumn("dq_error_count", F.size("dq_errors"))
        .withColumn("dq_valid", F.col("dq_error_count") == F.lit(0))
    )


def _observed_counts(dataframe: Any, destination: Path) -> tuple[int, int, int]:
    """Escribe Silver y obtiene conteos en la misma accion distribuida."""

    from pyspark.sql import functions as F
    from pyspark.sql.observation import Observation

    observation = Observation(f"silver_{uuid.uuid4().hex}")
    observed = dataframe.observe(
        observation,
        F.count(F.lit(1)).alias("rows"),
        F.coalesce(F.sum(F.when(F.col("dq_valid"), F.lit(1)).otherwise(F.lit(0))), F.lit(0)).alias(
            "valid_rows"
        ),
        F.coalesce(F.sum(F.when(~F.col("dq_valid"), F.lit(1)).otherwise(F.lit(0))), F.lit(0)).alias(
            "invalid_rows"
        ),
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    observed.drop(*PARTITION_COLUMNS).write.mode("overwrite").parquet(str(destination))
    metrics = observation.get
    return int(metrics["rows"]), int(metrics["valid_rows"]), int(metrics["invalid_rows"])


def transform_file_to_silver(
    spark: Any,
    bronze_file: BronzeFile | str | Path,
    *,
    config: Any,
    silver_path: str | Path | None = None,
    quarantine_path: str | Path | None = None,
    audit: Any | None = None,
    run_id: str | None = None,
) -> FileTransformResult:
    """Procesa, reconcilia y escribe una unica particion mensual."""

    item = (
        bronze_file if isinstance(bronze_file, BronzeFile) else parse_bronze_filename(bronze_file)
    )
    silver_root = configured_path(config, "silver", silver_path)
    quarantine_root = configured_path(config, "quarantine", quarantine_path)
    silver_partition = hive_partition_path(
        silver_root, item.service, item.year, item.month
    ).resolve()
    quarantine_partition = hive_partition_path(
        quarantine_root, item.service, item.year, item.month
    ).resolve()

    source_df = spark.read.parquet(str(item.path))
    canonical = canonicalize_trip_data(
        source_df,
        service=item.service,
        year=item.year,
        month=item.month,
        source_file=item.filename,
        source_path=str(item.path),
        config=config,
    )
    source_rows, valid_rows, invalid_rows = _observed_counts(canonical, silver_partition)

    # Se relee la particion ya compactada por Spark, no el archivo Bronze, para
    # construir la copia de cuarentena sin mantener un cache global en disco.
    if invalid_rows:
        quarantined = spark.read.parquet(str(silver_partition)).filter(~F_col("dq_valid"))
        quarantine_partition.parent.mkdir(parents=True, exist_ok=True)
        quarantined.write.mode("overwrite").parquet(str(quarantine_partition))
    else:
        safe_remove_tree(quarantine_partition, allowed_root=quarantine_root)

    silver_rows = source_rows
    reconciled = source_rows == silver_rows == valid_rows + invalid_rows
    result = FileTransformResult(
        source_file=str(item.path),
        service=item.service,
        year=item.year,
        month=item.month,
        source_rows=source_rows,
        silver_rows=silver_rows,
        valid_rows=valid_rows,
        quarantine_rows=invalid_rows,
        reconciled=reconciled,
        silver_partition=str(silver_partition),
        quarantine_partition=str(quarantine_partition),
    )
    if audit is not None:
        audit.record_quality(
            {
                "result_id": (
                    f"{run_id or 'standalone'}:silver:{item.service}:{item.year}-{item.month:02d}"
                ),
                "status": "PASSED" if reconciled else "FAILED",
                "layer": "silver",
                **result.to_dict(),
            },
            run_id=run_id,
        )
    if not reconciled and bool(config_get(config, "quality.fail_on_row_loss", True)):
        raise RuntimeError(f"Fallo la reconciliacion Bronze/Silver para {item.filename}")
    return result


def F_col(name: str):
    """Import local minimo usado al releer la cuarentena."""

    from pyspark.sql import functions as F

    return F.col(name)


def transform_bronze_to_silver(
    spark: Any,
    config: Any,
    *,
    bronze_path: str | Path | None = None,
    silver_path: str | Path | None = None,
    quarantine_path: str | Path | None = None,
    services: Iterable[str] | None = None,
    audit: Any | None = None,
    run_id: str | None = None,
) -> TransformSummary:
    """Ejecuta el corpus Bronze completo, archivo por archivo, sin sampling."""

    bronze_root = configured_path(config, "bronze", bronze_path)
    selected_services = tuple(services or config_get(config, "source.services", TLC_SERVICES))
    files = discover_bronze_files(bronze_root, services=selected_services)
    if not files:
        raise FileNotFoundError(
            f"No se encontraron archivos {{service}}_tripdata_YYYY-MM.parquet en {bronze_root}"
        )

    results: list[FileTransformResult] = []
    for item in files:
        results.append(
            transform_file_to_silver(
                spark,
                item,
                config=config,
                silver_path=silver_path,
                quarantine_path=quarantine_path,
                audit=audit,
                run_id=run_id,
            )
        )

    source_rows = sum(result.source_rows for result in results)
    silver_rows = sum(result.silver_rows for result in results)
    valid_rows = sum(result.valid_rows for result in results)
    quarantine_rows = sum(result.quarantine_rows for result in results)
    reconciled = (
        all(result.reconciled for result in results)
        and source_rows == silver_rows
        and source_rows == valid_rows + quarantine_rows
    )
    summary = TransformSummary(
        files=tuple(results),
        source_rows=source_rows,
        silver_rows=silver_rows,
        valid_rows=valid_rows,
        quarantine_rows=quarantine_rows,
        reconciled=reconciled,
    )
    if audit is not None:
        audit.record_quality(
            {
                "result_id": f"{run_id or 'standalone'}:silver:reconciliation",
                "status": "PASSED" if reconciled else "FAILED",
                "layer": "silver",
                **summary.to_dict(),
            },
            run_id=run_id,
        )
    if not reconciled and bool(config_get(config, "quality.fail_on_row_loss", True)):
        raise RuntimeError("Fallo la reconciliacion total Bronze/Silver")
    return summary


# Nombres cortos utiles para el CLI y notebooks del examen.
run_silver = transform_bronze_to_silver
bronze_to_silver = transform_bronze_to_silver
