"""Construccion de marts Gold completos para analitica y modelos.

Cada tabla se materializa antes de crear sus exportaciones. De este modo las
agregaciones sobre Silver se ejecutan una sola vez por mart y los archivos CSV
y Parquet para Power BI se leen desde una salida Gold mucho mas pequena. No se
mantiene un cache global ni se aplica sampling al corpus.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .utils import (
    column_key,
    config_get,
    configured_path,
    export_dataframe,
    quote_spark_identifier,
)

DESCRIPTIVE_TABLES = (
    "descriptive_daily_demand",
    "descriptive_hourly_profile",
    "descriptive_service_financials",
)
DIAGNOSTIC_TABLES = (
    "diagnostic_route_performance",
    "diagnostic_tip_factors",
    "diagnostic_daily_anomalies",
)
MODEL_BASE_TABLES = (
    "model_timeseries_daily",
    "model_segmentation_zones",
    "model_classification_demand",
)
ALL_GOLD_TABLES = DESCRIPTIVE_TABLES + DIAGNOSTIC_TABLES + MODEL_BASE_TABLES


@dataclass(frozen=True, slots=True)
class GoldTableResult:
    name: str
    rows: int
    parquet_path: str
    export_parquet: str
    export_csv: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class GoldSummary:
    tables: tuple[GoldTableResult, ...]
    total_rows: int

    @property
    def tables_created(self) -> int:
        return len(self.tables)

    def to_dict(self) -> dict[str, Any]:
        document = asdict(self)
        document["tables_created"] = self.tables_created
        return document


def find_zone_lookup(config: Any, override: str | Path | None = None) -> Path:
    """Localiza el catalogo oficial de zonas descargado por la ingesta."""

    if override is not None:
        selected = Path(override).expanduser().resolve()
        if not selected.is_file():
            raise FileNotFoundError(f"No existe taxi_zone_lookup.csv: {selected}")
        return selected

    configured = config_get(config, "paths.zone_lookup") or config_get(
        config, "source.zone_lookup_path"
    )
    if configured:
        selected = Path(str(configured)).expanduser().resolve()
        if selected.is_file():
            return selected

    candidates: list[Path] = []
    for path_name in ("bronze", "root"):
        try:
            root = configured_path(config, path_name)
        except (KeyError, TypeError, ValueError):
            continue
        direct = root / "taxi_zone_lookup.csv"
        if direct.is_file():
            candidates.append(direct)
        if root.is_dir():
            candidates.extend(
                candidate for candidate in root.rglob("taxi_zone_lookup.csv") if candidate.is_file()
            )
    if not candidates:
        raise FileNotFoundError(
            "No se encontro taxi_zone_lookup.csv bajo paths.bronze o paths.root; "
            "ejecute primero la ingesta de datos de referencia"
        )
    return sorted({candidate.resolve() for candidate in candidates}, key=lambda p: str(p))[0]


def load_zone_lookup(spark: Any, path: str | Path):
    """Lee y canoniza LocationID/Borough/Zone/service_zone sin inferencia fragil."""

    from pyspark.sql import functions as F

    dataframe = spark.read.option("header", "true").option("mode", "FAILFAST").csv(str(path))
    resolver: dict[str, str] = {}
    for name in dataframe.columns:
        resolver.setdefault(column_key(name), name)

    def source(aliases: Sequence[str], data_type: str):
        for alias in aliases:
            actual = resolver.get(column_key(alias))
            if actual is not None:
                return F.col(quote_spark_identifier(actual)).cast(data_type)
        raise ValueError(f"taxi_zone_lookup.csv no contiene ninguna de {aliases}")

    return (
        dataframe.select(
            source(("location_id", "LocationID"), "int").alias("location_id"),
            source(("borough",), "string").alias("borough"),
            source(("zone",), "string").alias("zone"),
            source(("service_zone",), "string").alias("service_zone"),
        )
        .filter(F.col("location_id").isNotNull())
        .dropDuplicates(["location_id"])
    )


_OPTIONAL_SILVER_TYPES = {
    "passenger_count": "int",
    "trip_distance": "double",
    "trip_duration_minutes": "double",
    "payment_type": "int",
    "fare_amount": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "total_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "improvement_surcharge": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
    "cbd_congestion_fee": "double",
    "bcf": "double",
    "sales_tax": "double",
}
_REQUIRED_SILVER = {
    "service",
    "year",
    "month",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "dq_valid",
}


def _ensure_silver_contract(dataframe: Any):
    from pyspark.sql import functions as F

    missing_required = sorted(_REQUIRED_SILVER.difference(dataframe.columns))
    if missing_required:
        raise ValueError(f"Silver no cumple el contrato; faltan: {', '.join(missing_required)}")
    result = dataframe
    for name, data_type in _OPTIONAL_SILVER_TYPES.items():
        if name not in result.columns:
            result = result.withColumn(name, F.lit(None).cast(data_type))
    return result


def read_valid_silver(
    spark: Any,
    silver_path: str | Path,
    zone_lookup: Any,
    *,
    input_paths: Sequence[str | Path] | None = None,
):
    """Carga exclusivamente DQ validos y agrega nombres de zonas PU/DO."""

    from pyspark.sql import functions as F

    reader = spark.read
    if input_paths:
        reader = reader.option("basePath", str(silver_path))
        raw = reader.parquet(*(str(path) for path in input_paths))
    else:
        raw = reader.parquet(str(silver_path))
    source = _ensure_silver_contract(raw).filter(
        F.coalesce(F.col("dq_valid"), F.lit(False))
    )
    pickup = F.broadcast(
        zone_lookup.select(
            F.col("location_id").alias("_pickup_lookup_id"),
            F.col("borough").alias("pickup_borough"),
            F.col("zone").alias("pickup_zone"),
            F.col("service_zone").alias("pickup_service_zone"),
        )
    )
    dropoff = F.broadcast(
        zone_lookup.select(
            F.col("location_id").alias("_dropoff_lookup_id"),
            F.col("borough").alias("dropoff_borough"),
            F.col("zone").alias("dropoff_zone"),
            F.col("service_zone").alias("dropoff_service_zone"),
        )
    )
    enriched = (
        source.join(
            pickup,
            source.pickup_location_id == pickup._pickup_lookup_id,
            "left",
        )
        .drop("_pickup_lookup_id")
        .join(
            dropoff,
            source.dropoff_location_id == dropoff._dropoff_lookup_id,
            "left",
        )
        .drop("_dropoff_lookup_id")
    )
    for name in (
        "pickup_borough",
        "pickup_zone",
        "pickup_service_zone",
        "dropoff_borough",
        "dropoff_zone",
        "dropoff_service_zone",
    ):
        enriched = enriched.withColumn(name, F.coalesce(F.col(name), F.lit("Unknown")))
    return enriched


def _sum0(name: str):
    from pyspark.sql import functions as F

    return F.sum(F.coalesce(F.col(name), F.lit(0.0)))


def _calendar_fields(dataframe: Any):
    from pyspark.sql import functions as F

    return (
        dataframe.withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_year", F.year("pickup_datetime"))
        .withColumn("pickup_month", F.month("pickup_datetime"))
        .withColumn("pickup_day_of_week", F.dayofweek("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
    )


def descriptive_daily_demand(dataframe: Any):
    from pyspark.sql import functions as F

    source = _calendar_fields(dataframe)
    grouped = source.groupBy(
        "pickup_date",
        "pickup_year",
        "pickup_month",
        "pickup_day_of_week",
        "service",
        "pickup_location_id",
        "pickup_borough",
        "pickup_zone",
        "pickup_service_zone",
    ).agg(
        F.count(F.lit(1)).alias("trip_count"),
        _sum0("passenger_count").alias("passenger_total"),
        _sum0("total_amount").alias("total_revenue"),
        _sum0("fare_amount").alias("fare_revenue"),
        _sum0("tip_amount").alias("tip_revenue"),
        _sum0("tolls_amount").alias("tolls_revenue"),
        _sum0("trip_distance").alias("distance_miles"),
        _sum0("trip_duration_minutes").alias("duration_minutes"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
        F.avg("total_amount").alias("avg_total_amount"),
        F.sum(F.when(F.col("pickup_hour").between(0, 5), 1).otherwise(0)).alias("night_trips"),
        F.sum(
            F.when(
                F.col("pickup_hour").between(7, 9) | F.col("pickup_hour").between(16, 19),
                1,
            ).otherwise(0)
        ).alias("rush_hour_trips"),
        F.sum(F.when(F.col("pickup_day_of_week").isin(1, 7), 1).otherwise(0)).alias(
            "weekend_trips"
        ),
    )
    return (
        grouped.withColumn(
            "revenue_per_trip",
            F.when(F.col("trip_count") > 0, F.col("total_revenue") / F.col("trip_count")),
        )
        .withColumn(
            "avg_speed_mph",
            F.when(
                F.col("duration_minutes") > 0,
                F.col("distance_miles") / (F.col("duration_minutes") / F.lit(60.0)),
            ),
        )
        .withColumn(
            "tip_share",
            F.when(F.col("fare_revenue") > 0, F.col("tip_revenue") / F.col("fare_revenue")),
        )
    )


def descriptive_hourly_profile(dataframe: Any):
    from pyspark.sql import functions as F

    source = _calendar_fields(dataframe)
    return source.groupBy(
        "pickup_date",
        "pickup_year",
        "pickup_month",
        "pickup_day_of_week",
        "pickup_hour",
        "service",
        "pickup_location_id",
        "pickup_borough",
        "pickup_zone",
        "pickup_service_zone",
    ).agg(
        F.count(F.lit(1)).alias("trip_count"),
        _sum0("total_amount").alias("total_revenue"),
        _sum0("tip_amount").alias("tip_revenue"),
        _sum0("trip_distance").alias("distance_miles"),
        _sum0("trip_duration_minutes").alias("duration_minutes"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
        F.avg("total_amount").alias("avg_total_amount"),
    )


def descriptive_service_financials(dataframe: Any):
    from pyspark.sql import functions as F

    source = _calendar_fields(dataframe)
    surcharge = sum(
        (
            F.coalesce(F.col(name), F.lit(0.0))
            for name in (
                "extra",
                "mta_tax",
                "improvement_surcharge",
                "congestion_surcharge",
                "airport_fee",
                "cbd_congestion_fee",
                "bcf",
                "sales_tax",
            )
        ),
        F.lit(0.0),
    )
    return source.groupBy("pickup_year", "pickup_month", "service", "payment_type").agg(
        F.count(F.lit(1)).alias("trip_count"),
        _sum0("total_amount").alias("total_revenue"),
        _sum0("fare_amount").alias("fare_revenue"),
        _sum0("tip_amount").alias("tip_revenue"),
        _sum0("tolls_amount").alias("tolls_revenue"),
        F.sum(surcharge).alias("taxes_and_surcharges"),
        F.avg("total_amount").alias("avg_total_amount"),
        F.avg("tip_amount").alias("avg_tip_amount"),
        F.sum(F.when(F.col("tip_amount") > 0, 1).otherwise(0)).alias("tipped_trips"),
    )


def diagnostic_route_performance(dataframe: Any):
    from pyspark.sql import functions as F

    source = (
        _calendar_fields(dataframe)
        .withColumn(
            "_speed_mph",
            F.when(
                F.col("trip_duration_minutes") > 0,
                F.col("trip_distance") / (F.col("trip_duration_minutes") / F.lit(60.0)),
            ),
        )
        .withColumn(
            "_fare_per_mile",
            F.when(F.col("trip_distance") > 0, F.col("total_amount") / F.col("trip_distance")),
        )
    )
    return source.groupBy(
        "pickup_year",
        "pickup_month",
        "service",
        "pickup_location_id",
        "pickup_borough",
        "pickup_zone",
        "dropoff_location_id",
        "dropoff_borough",
        "dropoff_zone",
    ).agg(
        F.count(F.lit(1)).alias("trip_count"),
        _sum0("total_amount").alias("total_revenue"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
        F.avg("_speed_mph").alias("avg_speed_mph"),
        F.expr("percentile_approx(trip_duration_minutes, 0.5, 10000)").alias(
            "median_trip_duration_minutes"
        ),
        F.avg("_fare_per_mile").alias("avg_fare_per_mile"),
    )


def diagnostic_tip_factors(dataframe: Any):
    from pyspark.sql import functions as F

    source = _calendar_fields(dataframe).withColumn(
        "_tip_rate",
        F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount")),
    )
    grouped = source.groupBy(
        "pickup_year",
        "pickup_month",
        "pickup_day_of_week",
        "pickup_hour",
        "service",
        "payment_type",
        "pickup_borough",
    ).agg(
        F.count(F.lit(1)).alias("trip_count"),
        F.sum(F.when(F.col("tip_amount") > 0, 1).otherwise(0)).alias("tipped_trips"),
        _sum0("fare_amount").alias("fare_revenue"),
        _sum0("tip_amount").alias("tip_revenue"),
        F.avg("_tip_rate").alias("avg_trip_tip_rate"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
    )
    return grouped.withColumn(
        "tipped_trip_share",
        F.when(F.col("trip_count") > 0, F.col("tipped_trips") / F.col("trip_count")),
    ).withColumn(
        "aggregate_tip_rate",
        F.when(F.col("fare_revenue") > 0, F.col("tip_revenue") / F.col("fare_revenue")),
    )


def diagnostic_daily_anomalies(daily: Any, *, zscore_threshold: float = 3.0):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    baseline = Window.partitionBy("service", "pickup_location_id", "pickup_day_of_week")
    result = (
        daily.withColumn("baseline_avg_trips", F.avg("trip_count").over(baseline))
        .withColumn("baseline_stddev_trips", F.stddev_pop("trip_count").over(baseline))
        .withColumn("baseline_avg_revenue", F.avg("total_revenue").over(baseline))
        .withColumn("baseline_stddev_revenue", F.stddev_pop("total_revenue").over(baseline))
        .withColumn(
            "demand_zscore",
            F.when(
                F.col("baseline_stddev_trips") > 0,
                (F.col("trip_count") - F.col("baseline_avg_trips"))
                / F.col("baseline_stddev_trips"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "revenue_zscore",
            F.when(
                F.col("baseline_stddev_revenue") > 0,
                (F.col("total_revenue") - F.col("baseline_avg_revenue"))
                / F.col("baseline_stddev_revenue"),
            ).otherwise(F.lit(0.0)),
        )
    )
    return result.withColumn(
        "is_anomaly",
        (F.abs(F.col("demand_zscore")) >= F.lit(float(zscore_threshold)))
        | (F.abs(F.col("revenue_zscore")) >= F.lit(float(zscore_threshold))),
    ).withColumn(
        "anomaly_direction",
        F.when(F.col("demand_zscore") >= F.lit(float(zscore_threshold)), F.lit("HIGH"))
        .when(F.col("demand_zscore") <= F.lit(-float(zscore_threshold)), F.lit("LOW"))
        .otherwise(F.lit("NORMAL")),
    )


def model_timeseries_daily(daily: Any):
    from pyspark.sql import functions as F

    return (
        daily.groupBy("pickup_date", "pickup_year", "pickup_month", "service")
        .agg(
            F.sum("trip_count").alias("trip_count"),
            F.sum("total_revenue").alias("total_revenue"),
            F.sum("distance_miles").alias("distance_miles"),
            F.sum("duration_minutes").alias("duration_minutes"),
        )
        .withColumn("ds", F.col("pickup_date"))
        .withColumn("y", F.col("trip_count").cast("double"))
        .withColumn("is_weekend", F.dayofweek("pickup_date").isin(1, 7).cast("int"))
    )


def model_segmentation_zones(daily: Any):
    from pyspark.sql import functions as F

    grouped = daily.groupBy(
        "service",
        "pickup_location_id",
        "pickup_borough",
        "pickup_zone",
        "pickup_service_zone",
    ).agg(
        F.sum("trip_count").alias("total_trips"),
        F.countDistinct("pickup_date").alias("active_days"),
        F.avg("trip_count").alias("avg_daily_trips"),
        F.stddev_pop("trip_count").alias("stddev_daily_trips"),
        F.sum("total_revenue").alias("total_revenue"),
        F.sum("distance_miles").alias("total_distance_miles"),
        F.sum("duration_minutes").alias("total_duration_minutes"),
        F.sum("weekend_trips").alias("weekend_trips"),
        F.sum("night_trips").alias("night_trips"),
        F.sum("rush_hour_trips").alias("rush_hour_trips"),
    )
    return (
        grouped.withColumn(
            "revenue_per_trip",
            F.when(F.col("total_trips") > 0, F.col("total_revenue") / F.col("total_trips")),
        )
        .withColumn(
            "avg_trip_distance",
            F.when(
                F.col("total_trips") > 0,
                F.col("total_distance_miles") / F.col("total_trips"),
            ),
        )
        .withColumn(
            "avg_trip_duration_minutes",
            F.when(
                F.col("total_trips") > 0,
                F.col("total_duration_minutes") / F.col("total_trips"),
            ),
        )
        .withColumn(
            "weekend_share",
            F.when(F.col("total_trips") > 0, F.col("weekend_trips") / F.col("total_trips")),
        )
        .withColumn(
            "night_share",
            F.when(F.col("total_trips") > 0, F.col("night_trips") / F.col("total_trips")),
        )
        .withColumn(
            "rush_hour_share",
            F.when(F.col("total_trips") > 0, F.col("rush_hour_trips") / F.col("total_trips")),
        )
    )


def model_classification_demand(hourly: Any, *, high_demand_quantile: float = 0.75):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if not 0.0 < float(high_demand_quantile) < 1.0:
        raise ValueError("models.classification.high_demand_quantile debe estar entre 0 y 1")
    segment = Window.partitionBy("service", "pickup_location_id")
    sequence = Window.partitionBy("service", "pickup_location_id").orderBy(
        "pickup_date", "pickup_hour"
    )
    threshold_expression = F.expr(
        f"percentile_approx(trip_count, {float(high_demand_quantile)}, 10000)"
    ).over(segment)
    return (
        hourly.withColumn("demand_threshold", threshold_expression.cast("double"))
        .withColumn(
            "previous_observed_trip_count", F.lag("trip_count", 1).over(sequence).cast("double")
        )
        .withColumn("is_weekend", F.col("pickup_day_of_week").isin(1, 7).cast("int"))
        .withColumn(
            "is_rush_hour",
            (F.col("pickup_hour").between(7, 9) | F.col("pickup_hour").between(16, 19)).cast("int"),
        )
        .withColumn(
            "label_high_demand",
            (F.col("trip_count") >= F.col("demand_threshold")).cast("int"),
        )
        .withColumn("label", F.col("label_high_demand").cast("double"))
    )


def _materialize_and_export(
    dataframe: Any,
    *,
    table_name: str,
    gold_root: Path,
    exports_root: Path,
    audit: Any | None,
    run_id: str | None,
    export_full_tables: bool,
    materialized_scope: tuple[int, int] | None = None,
) -> GoldTableResult:
    from pyspark.sql import functions as F

    target = (gold_root / table_name).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    writer = dataframe.write.mode("overwrite").option("partitionOverwriteMode", "dynamic")
    partition_columns = [
        name for name in ("pickup_year", "pickup_month") if name in dataframe.columns
    ]
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.parquet(str(target))

    materialized = dataframe.sparkSession.read.parquet(str(target))
    if materialized_scope is not None and {
        "pickup_year",
        "pickup_month",
    }.issubset(materialized.columns):
        scope_year, scope_month = materialized_scope
        materialized = materialized.filter(
            (F.col("pickup_year") == F.lit(int(scope_year)))
            & (F.col("pickup_month") == F.lit(int(scope_month)))
        )
    rows = int(materialized.count())
    # El Gold Parquet es el artefacto canónico. La copia completa Parquet+CSV
    # es opcional porque triplica almacenamiento; Power BI usa contratos
    # agregados propios derivados del 100 % de estas tablas.
    exports = (
        export_dataframe(materialized, exports_root, table_name)
        if export_full_tables
        else {"parquet": str(target), "csv": ""}
    )
    result = GoldTableResult(
        name=table_name,
        rows=rows,
        parquet_path=str(target),
        export_parquet=exports["parquet"],
        export_csv=exports["csv"],
    )
    if audit is not None:
        audit.record_quality(
            {
                "result_id": f"{run_id or 'standalone'}:gold:{table_name}",
                "status": "PASSED",
                "layer": "gold",
                **result.to_dict(),
            },
            run_id=run_id,
        )
    return result


def build_gold_tables(
    spark: Any,
    config: Any,
    *,
    silver_path: str | Path | None = None,
    gold_path: str | Path | None = None,
    exports_path: str | Path | None = None,
    zone_lookup_path: str | Path | None = None,
    audit: Any | None = None,
    run_id: str | None = None,
    tables: Sequence[str] | None = None,
    silver_scope: tuple[int, int] | None = None,
) -> GoldSummary:
    """Materializa todas o una selección de tablas Gold sin aplicar sampling."""

    selected = tuple(ALL_GOLD_TABLES if tables is None else dict.fromkeys(tables))
    unknown = sorted(set(selected).difference(ALL_GOLD_TABLES))
    if unknown:
        raise ValueError(f"Tablas Gold desconocidas: {unknown}")
    if not selected:
        raise ValueError("La selección Gold no puede estar vacía")

    silver_root = configured_path(config, "silver", silver_path)
    gold_root = configured_path(config, "gold", gold_path)
    exports_root = configured_path(config, "exports", exports_path)
    lookup_path = find_zone_lookup(config, zone_lookup_path)
    gold_root.mkdir(parents=True, exist_ok=True)
    exports_root.mkdir(parents=True, exist_ok=True)
    export_full_tables = bool(config_get(config, "gold.export_full_tables", True))

    # El lookup es diminuto y Spark lo distribuye por broadcast. Silver se
    # relee por mart para evitar una copia cacheada del corpus completo.
    lookup = load_zone_lookup(spark, lookup_path)

    scoped_paths: list[Path] | None = None
    if silver_scope is not None:
        scope_year, scope_month = silver_scope
        scoped_paths = sorted(
            path
            for path in silver_root.glob(
                f"service=*/year={int(scope_year)}/month={int(scope_month)}"
            )
            if path.is_dir()
        )
        if not scoped_paths:
            raise FileNotFoundError(
                f"No hay Silver para year={scope_year}, month={scope_month}"
            )

    def valid():
        return read_valid_silver(
            spark,
            silver_root,
            lookup,
            input_paths=scoped_paths,
        )

    results: list[GoldTableResult] = []

    def save(name: str, dataframe: Any) -> None:
        results.append(
            _materialize_and_export(
                dataframe,
                table_name=name,
                gold_root=gold_root,
                exports_root=exports_root,
                audit=audit,
                run_id=run_id,
                export_full_tables=export_full_tables,
                materialized_scope=silver_scope,
            )
        )

    if "descriptive_daily_demand" in selected:
        save("descriptive_daily_demand", descriptive_daily_demand(valid()))
    if "descriptive_hourly_profile" in selected:
        save("descriptive_hourly_profile", descriptive_hourly_profile(valid()))
    if "descriptive_service_financials" in selected:
        save("descriptive_service_financials", descriptive_service_financials(valid()))
    if "diagnostic_route_performance" in selected:
        save("diagnostic_route_performance", diagnostic_route_performance(valid()))
    if "diagnostic_tip_factors" in selected:
        save("diagnostic_tip_factors", diagnostic_tip_factors(valid()))

    needs_daily = bool(
        {"diagnostic_daily_anomalies", "model_timeseries_daily", "model_segmentation_zones"}
        & set(selected)
    )
    daily = (
        spark.read.parquet(str(gold_root / "descriptive_daily_demand"))
        if needs_daily
        else None
    )
    if "diagnostic_daily_anomalies" in selected:
        save(
            "diagnostic_daily_anomalies",
            diagnostic_daily_anomalies(
                daily,
                zscore_threshold=float(
                    config_get(config, "gold.anomaly_zscore_threshold", 3.0)
                ),
            ),
        )
    if "model_timeseries_daily" in selected:
        save("model_timeseries_daily", model_timeseries_daily(daily))
    if "model_segmentation_zones" in selected:
        save("model_segmentation_zones", model_segmentation_zones(daily))
    if "model_classification_demand" in selected:
        hourly = spark.read.parquet(str(gold_root / "descriptive_hourly_profile"))
        save(
            "model_classification_demand",
            model_classification_demand(
                hourly,
                high_demand_quantile=float(
                    config_get(config, "models.classification.high_demand_quantile", 0.75)
                ),
            ),
        )

    expected_order = tuple(name for name in ALL_GOLD_TABLES if name in set(selected))
    if tuple(result.name for result in results) != expected_order:
        raise RuntimeError("No se materializó la selección solicitada de tablas Gold")
    return GoldSummary(tables=tuple(results), total_rows=sum(result.rows for result in results))


# Aliases de integracion para CLI/notebooks.
build_gold = build_gold_tables
run_gold = build_gold_tables
