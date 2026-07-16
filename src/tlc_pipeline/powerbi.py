"""Contratos compactos para las diez páginas del proyecto Power BI.

Cada contrato se deriva con Spark de una tabla Gold o del flujo de auditoría. No
se exportan viajes individuales a Power BI y no se usa sampling: todos los KPI
provienen de agregados construidos sobre el corpus completo.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .utils import configured_path, write_single_spark_file


@dataclass(frozen=True, slots=True)
class PowerBIContractResult:
    table: str
    rows: int
    csv_path: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


CONTRACTS = (
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
)


def _existing(dataframe: Any, name: str, default: Any, data_type: str):
    from pyspark.sql import functions as F

    if name in dataframe.columns:
        return F.col(name).cast(data_type)
    return F.lit(default).cast(data_type)


def _first(dataframe: Any, names: tuple[str, ...], default: Any, data_type: str):
    from pyspark.sql import functions as F

    available = [F.col(name).cast(data_type) for name in names if name in dataframe.columns]
    if not available:
        return F.lit(default).cast(data_type)
    return F.coalesce(*available)


def _date_text(dataframe: Any, *names: str):
    from pyspark.sql import functions as F

    available = [F.to_date(F.col(name)) for name in names if name in dataframe.columns]
    value = F.coalesce(*available) if available else F.lit(None).cast("date")
    return F.coalesce(F.date_format(value, "yyyy-MM-dd"), F.lit("Sin fecha"))


def _month_date(dataframe: Any, year: str, month: str):
    from pyspark.sql import functions as F

    if year not in dataframe.columns or month not in dataframe.columns:
        return F.lit("Sin fecha")
    return F.format_string("%04d-%02d-01", F.col(year).cast("int"), F.col(month).cast("int"))


def _contract(
    dataframe: Any,
    *,
    date: Any,
    year: Any,
    month: Any,
    service: Any,
    category: Any,
    series: Any,
    metric_name: str,
    metric_value: Any,
    metric_aux: Any,
    status: Any,
    detail: str,
):
    from pyspark.sql import functions as F

    return dataframe.select(
        date.cast("string").alias("date"),
        year.cast("int").alias("year"),
        month.cast("int").alias("month"),
        F.coalesce(service.cast("string"), F.lit("all")).alias("service"),
        F.coalesce(category.cast("string"), F.lit("Sin categoría")).alias("category"),
        F.coalesce(series.cast("string"), F.lit("Total")).alias("series"),
        F.lit(metric_name).alias("metric_name"),
        F.coalesce(metric_value.cast("double"), F.lit(0.0)).alias("metric_value"),
        F.coalesce(metric_aux.cast("double"), F.lit(0.0)).alias("metric_aux"),
        F.coalesce(status.cast("string"), F.lit("OK")).alias("status"),
        F.lit(detail).alias("detail"),
    )


def _read_gold(spark: Any, gold_root: Path, name: str):
    path = gold_root / name
    if not path.is_dir() and not path.is_file():
        raise FileNotFoundError(f"Tabla Gold ausente para Power BI: {path}")
    return spark.read.parquet(str(path))


def build_powerbi_contracts(
    spark: Any,
    config: Any,
    *,
    audit: Any | None = None,
    run_id: str | None = None,
) -> tuple[PowerBIContractResult, ...]:
    """Materializa los diez CSV que consume el PBIP."""

    from pyspark.sql import functions as F

    gold_root = configured_path(config, "gold")
    exports_root = configured_path(config, "exports")
    output_root = exports_root / "powerbi"
    output_root.mkdir(parents=True, exist_ok=True)

    daily = _read_gold(spark, gold_root, "descriptive_daily_demand")
    hourly = _read_gold(spark, gold_root, "descriptive_hourly_profile")
    financial = _read_gold(spark, gold_root, "descriptive_service_financials")
    routes = _read_gold(spark, gold_root, "diagnostic_route_performance")
    tips = _read_gold(spark, gold_root, "diagnostic_tip_factors")
    anomalies = _read_gold(spark, gold_root, "diagnostic_daily_anomalies")
    forecast = _read_gold(spark, gold_root, "model_timeseries_daily")
    segments = _read_gold(spark, gold_root, "model_segmentation_zones")
    classification = _read_gold(spark, gold_root, "model_classification_demand")

    frames: dict[str, Any] = {}
    frames["D01_Resumen"] = _contract(
        daily,
        date=_date_text(daily, "pickup_date"),
        year=_existing(daily, "pickup_year", 0, "int"),
        month=_existing(daily, "pickup_month", 0, "int"),
        service=_existing(daily, "service", "all", "string"),
        category=_first(daily, ("pickup_borough", "pickup_zone"), "NYC", "string"),
        series=_existing(daily, "service", "Total", "string"),
        metric_name="Viajes e ingresos",
        metric_value=_existing(daily, "trip_count", 0, "double"),
        metric_aux=_existing(daily, "total_revenue", 0, "double"),
        status=F.lit("OK"),
        detail="Resumen ejecutivo de demanda e ingresos",
    )
    frames["D02_Demanda"] = _contract(
        hourly,
        date=_date_text(hourly, "pickup_date"),
        year=_existing(hourly, "pickup_year", 0, "int"),
        month=_existing(hourly, "pickup_month", 0, "int"),
        service=_existing(hourly, "service", "all", "string"),
        category=_existing(hourly, "pickup_hour", "Sin hora", "string"),
        series=_existing(hourly, "service", "Total", "string"),
        metric_name="Perfil horario de viajes",
        metric_value=_existing(hourly, "trip_count", 0, "double"),
        metric_aux=_existing(hourly, "avg_trip_duration_minutes", 0, "double"),
        status=F.lit("OK"),
        detail="Demanda por fecha, hora y servicio",
    )
    frames["D03_Ingresos"] = _contract(
        financial,
        date=_month_date(financial, "pickup_year", "pickup_month"),
        year=_existing(financial, "pickup_year", 0, "int"),
        month=_existing(financial, "pickup_month", 0, "int"),
        service=_existing(financial, "service", "all", "string"),
        category=_existing(financial, "payment_type", "Sin tipo", "string"),
        series=_existing(financial, "service", "Total", "string"),
        metric_name="Ingresos y propinas",
        metric_value=_existing(financial, "total_revenue", 0, "double"),
        metric_aux=_existing(financial, "tip_revenue", 0, "double"),
        status=F.lit("OK"),
        detail="Composición financiera por servicio y pago",
    )
    frames["D04_Causas"] = _contract(
        anomalies,
        date=_date_text(anomalies, "pickup_date"),
        year=_existing(anomalies, "pickup_year", 0, "int"),
        month=_existing(anomalies, "pickup_month", 0, "int"),
        service=_existing(anomalies, "service", "all", "string"),
        category=_first(anomalies, ("pickup_borough", "pickup_zone"), "NYC", "string"),
        series=_existing(anomalies, "anomaly_direction", "NORMAL", "string"),
        metric_name="Desviación de la demanda",
        metric_value=_existing(anomalies, "demand_zscore", 0, "double"),
        metric_aux=_existing(anomalies, "revenue_zscore", 0, "double"),
        status=F.when(_existing(anomalies, "is_anomaly", False, "boolean"), "ANOMALÍA").otherwise(
            "NORMAL"
        ),
        detail="Diagnóstico de cambios frente a la línea base",
    )
    route_category = F.concat_ws(
        " → ",
        _first(routes, ("pickup_zone", "pickup_borough"), "Origen", "string"),
        _first(routes, ("dropoff_zone", "dropoff_borough"), "Destino", "string"),
    )
    frames["D05_Rutas"] = _contract(
        routes,
        date=_month_date(routes, "pickup_year", "pickup_month"),
        year=_existing(routes, "pickup_year", 0, "int"),
        month=_existing(routes, "pickup_month", 0, "int"),
        service=_existing(routes, "service", "all", "string"),
        category=route_category,
        series=_existing(routes, "service", "Total", "string"),
        metric_name="Rendimiento de rutas",
        metric_value=_existing(routes, "trip_count", 0, "double"),
        metric_aux=_existing(routes, "avg_trip_duration_minutes", 0, "double"),
        status=F.when(_existing(routes, "avg_speed_mph", 99, "double") < 8, "LENTA").otherwise(
            "NORMAL"
        ),
        detail="Volumen, duración y velocidad por ruta",
    )
    frames["D06_Anomalias"] = _contract(
        tips,
        date=_month_date(tips, "pickup_year", "pickup_month"),
        year=_existing(tips, "pickup_year", 0, "int"),
        month=_existing(tips, "pickup_month", 0, "int"),
        service=_existing(tips, "service", "all", "string"),
        category=_existing(tips, "pickup_borough", "NYC", "string"),
        series=_existing(tips, "payment_type", "Sin tipo", "string"),
        metric_name="Factores de propina",
        metric_value=_first(tips, ("aggregate_tip_rate", "avg_trip_tip_rate"), 0, "double"),
        metric_aux=_existing(tips, "tipped_trip_share", 0, "double"),
        status=F.lit("OK"),
        detail="Propensión y tasa de propina por contexto",
    )
    frames["D07_Pronostico"] = _contract(
        forecast,
        date=_date_text(forecast, "forecast_date", "pickup_date", "ds"),
        year=F.year(
            F.to_date(_first(forecast, ("forecast_date", "pickup_date", "ds"), None, "string"))
        ),
        month=F.month(
            F.to_date(_first(forecast, ("forecast_date", "pickup_date", "ds"), None, "string"))
        ),
        service=_first(forecast, ("service", "service_type"), "all", "string"),
        category=_first(forecast, ("service", "service_type"), "Total", "string"),
        series=F.lit("Pronóstico"),
        metric_name="Viajes pronosticados",
        metric_value=_first(forecast, ("forecast_trips", "yhat", "prediction"), 0, "double"),
        metric_aux=_first(forecast, ("forecast_upper_95", "forecast_lower_95"), 0, "double"),
        status=F.lit("FORECAST"),
        detail="Modelo GBT con calendario y rezagos",
    )
    frames["D08_Segmentacion"] = _contract(
        segments,
        date=F.lit("Periodo completo"),
        year=F.lit(0),
        month=F.lit(0),
        service=_first(segments, ("service", "service_type"), "all", "string"),
        category=_first(segments, ("pickup_zone", "zone_name", "zone_id"), "Zona", "string"),
        series=_first(segments, ("segment_label", "segment_id"), "Segmento", "string"),
        metric_name="Segmentación de zonas",
        metric_value=_first(segments, ("total_trips", "trip_count"), 0, "double"),
        metric_aux=_first(segments, ("revenue_per_trip", "total_revenue"), 0, "double"),
        status=F.lit("CLUSTER"),
        detail="K-Means sobre demanda, ingresos y comportamiento",
    )
    frames["D09_Clasificacion"] = _contract(
        classification,
        date=_date_text(classification, "prediction_date", "pickup_date"),
        year=F.year(
            F.to_date(_first(classification, ("prediction_date", "pickup_date"), None, "string"))
        ),
        month=F.month(
            F.to_date(_first(classification, ("prediction_date", "pickup_date"), None, "string"))
        ),
        service=_first(classification, ("service", "service_type"), "all", "string"),
        category=_first(classification, ("zone_name", "pickup_zone", "zone_id"), "Zona", "string"),
        series=F.when(
            _existing(classification, "predicted_high_demand", 0, "int") == 1, "Alta"
        ).otherwise("Normal"),
        metric_name="Probabilidad de alta demanda",
        metric_value=_existing(classification, "probability_high_demand", 0, "double"),
        metric_aux=_first(classification, ("actual_trips", "trip_count"), 0, "double"),
        status=F.when(
            _existing(classification, "actual_high_demand", 0, "int")
            == _existing(classification, "predicted_high_demand", 0, "int"),
            "ACIERTO",
        ).otherwise("ERROR"),
        detail="Random Forest con validación temporal",
    )

    audit_csv = exports_root / "audit_events.csv"
    if not audit_csv.is_file():
        raise FileNotFoundError(f"Export de auditoría ausente para Power BI: {audit_csv}")
    audit_df = spark.read.option("header", True).option("inferSchema", True).csv(str(audit_csv))
    audit_timestamp = _first(
        audit_df, ("timestamp_utc", "started_at", "finished_at"), None, "string"
    )
    frames["D10_Auditoria"] = _contract(
        audit_df,
        date=F.coalesce(F.substring(audit_timestamp, 1, 10), F.lit("Sin fecha")),
        year=F.year(F.to_date(F.substring(audit_timestamp, 1, 10))),
        month=F.month(F.to_date(F.substring(audit_timestamp, 1, 10))),
        service=_existing(audit_df, "service", "pipeline", "string"),
        category=_existing(audit_df, "event_type", "evento", "string"),
        series=_existing(audit_df, "status", "UNKNOWN", "string"),
        metric_name="Eventos de auditoría",
        metric_value=F.lit(1.0),
        metric_aux=_first(audit_df, ("size_bytes", "source_rows", "silver_rows"), 0, "double"),
        status=_existing(audit_df, "status", "UNKNOWN", "string"),
        detail="Trazabilidad de ejecuciones, archivos, calidad y modelos",
    )

    results: list[PowerBIContractResult] = []
    for name in CONTRACTS:
        frame = frames[name]
        rows = frame.count()
        if rows <= 0:
            raise ValueError(f"Contrato Power BI sin filas: {name}")
        path = write_single_spark_file(
            frame,
            output_root / f"{name}.csv",
            file_format="csv",
        )
        result = PowerBIContractResult(name, rows, str(path))
        results.append(result)
        if audit is not None:
            audit.record_quality(
                {
                    "result_id": f"{run_id}:powerbi:{name}",
                    "status": "PASSED",
                    "layer": "powerbi",
                    "name": name,
                    "rows": rows,
                    "path": str(path),
                },
                run_id=run_id,
            )
    return tuple(results)
