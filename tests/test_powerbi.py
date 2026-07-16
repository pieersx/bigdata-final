from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from tlc_pipeline.powerbi import CONTRACTS, build_powerbi_contracts


@pytest.fixture(scope="module")
def spark(tmp_path_factory: pytest.TempPathFactory):
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("powerbi-warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tlc-powerbi-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .getOrCreate()
    )
    yield session
    session.stop()


def test_builds_all_ten_non_placeholder_contracts(spark, tmp_path: Path) -> None:
    gold = tmp_path / "gold"
    exports = tmp_path / "exports"
    row = {
        "pickup_date": date(2025, 1, 2),
        "pickup_year": 2025,
        "pickup_month": 1,
        "pickup_hour": 8,
        "service": "yellow",
        "pickup_borough": "Manhattan",
        "pickup_zone": "Midtown Center",
        "dropoff_borough": "Manhattan",
        "dropoff_zone": "Upper East Side",
        "payment_type": "Credit card",
        "trip_count": 100,
        "total_revenue": 2500.0,
        "tip_revenue": 300.0,
        "avg_trip_duration_minutes": 22.0,
        "avg_speed_mph": 7.5,
        "demand_zscore": 3.5,
        "revenue_zscore": 2.0,
        "is_anomaly": True,
        "anomaly_direction": "HIGH",
        "aggregate_tip_rate": 0.15,
        "tipped_trip_share": 0.8,
        "forecast_date": date(2025, 2, 1),
        "forecast_trips": 120.0,
        "forecast_upper_95": 140.0,
        "segment_id": 1,
        "segment_label": "Muy alta demanda",
        "total_trips": 5000.0,
        "revenue_per_trip": 25.0,
        "prediction_date": date(2025, 1, 3),
        "zone_name": "Midtown Center",
        "predicted_high_demand": 1,
        "actual_high_demand": 1,
        "probability_high_demand": 0.9,
        "actual_trips": 130.0,
    }
    frame = spark.createDataFrame([row])
    tables = (
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
    for table in tables:
        frame.write.mode("overwrite").parquet(str(gold / table))

    exports.mkdir(parents=True)
    (exports / "audit_events.csv").write_text(
        "event_id,event_type,status,timestamp_utc,service,size_bytes\n"
        "1,pipeline_run,SUCCESS,2025-01-03T00:00:00Z,pipeline,1\n",
        encoding="utf-8",
    )
    config = {"paths": {"gold": str(gold), "exports": str(exports)}}

    results = build_powerbi_contracts(spark, config)

    assert tuple(result.table for result in results) == CONTRACTS
    assert all(result.rows == 1 for result in results)
    for result in results:
        content = Path(result.csv_path).read_text(encoding="utf-8")
        assert "PLACEHOLDER" not in content
        assert content.count("\n") >= 2
