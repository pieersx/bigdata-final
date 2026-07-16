from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from tlc_pipeline.gold import ALL_GOLD_TABLES, build_gold_tables
from tlc_pipeline.transform import canonicalize_trip_data


@pytest.fixture(scope="module")
def spark(tmp_path_factory: pytest.TempPathFactory):
    from pyspark.sql import SparkSession

    temporary = tmp_path_factory.mktemp("spark-gold")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-tlc-gold")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.local.dir", str(temporary / "local"))
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _config(root: Path, lookup: Path) -> dict:
    return {
        "paths": {
            "root": str(root),
            "bronze": str(root / "bronze"),
            "silver": str(root / "silver"),
            "quarantine": str(root / "quarantine"),
            "gold": str(root / "gold"),
            "exports": str(root / "exports"),
            "zone_lookup": str(lookup),
        },
        "quality": {
            "valid_location_min": 1,
            "valid_location_max": 265,
            "max_trip_duration_minutes": 1_440,
            "max_trip_distance_miles": 500,
            "max_total_amount": 10_000,
        },
        "gold": {"anomaly_zscore_threshold": 2.0, "csv_single_file": True},
        "models": {"classification": {"high_demand_quantile": 0.75}},
    }


def test_builds_nine_complete_gold_tables_from_only_valid_silver_rows(spark, tmp_path):
    lookup = tmp_path / "taxi_zone_lookup.csv"
    lookup.write_text(
        "LocationID,Borough,Zone,service_zone\n"
        "10,Manhattan,Central Harlem,Boro Zone\n"
        "20,Manhattan,Midtown Center,Yellow Zone\n"
        "30,Queens,JFK Airport,Airports\n",
        encoding="utf-8",
    )
    config = _config(tmp_path, lookup)
    raw = spark.createDataFrame(
        [
            {
                "tpep_pickup_datetime": datetime(2023, 1, 1, 8, 0),
                "tpep_dropoff_datetime": datetime(2023, 1, 1, 8, 15),
                "PULocationID": 10,
                "DOLocationID": 20,
                "passenger_count": 1,
                "trip_distance": 3.0,
                "payment_type": 1,
                "fare_amount": 12.0,
                "tip_amount": 2.0,
                "total_amount": 16.0,
            },
            {
                "tpep_pickup_datetime": datetime(2023, 1, 1, 9, 0),
                "tpep_dropoff_datetime": datetime(2023, 1, 1, 9, 30),
                "PULocationID": 10,
                "DOLocationID": 30,
                "passenger_count": 2,
                "trip_distance": 12.0,
                "payment_type": 2,
                "fare_amount": 30.0,
                "tip_amount": 0.0,
                "total_amount": 35.0,
            },
            {
                "tpep_pickup_datetime": datetime(2023, 1, 2, 23, 0),
                "tpep_dropoff_datetime": datetime(2023, 1, 2, 23, 20),
                "PULocationID": 30,
                "DOLocationID": 10,
                "passenger_count": 1,
                "trip_distance": 8.0,
                "payment_type": 1,
                "fare_amount": 22.0,
                "tip_amount": 4.0,
                "total_amount": 29.0,
            },
            {
                "tpep_pickup_datetime": datetime(2023, 1, 2, 12, 0),
                "tpep_dropoff_datetime": datetime(2023, 1, 2, 12, 10),
                "PULocationID": 999,
                "DOLocationID": 10,
                "passenger_count": 1,
                "trip_distance": 2.0,
                "payment_type": 1,
                "fare_amount": 8.0,
                "tip_amount": 1.0,
                "total_amount": 10.0,
            },
        ]
    )
    silver = canonicalize_trip_data(
        raw,
        service="yellow",
        year=2023,
        month=1,
        source_file="yellow_tripdata_2023-01.parquet",
        config=config,
    )
    silver.write.mode("overwrite").partitionBy("service", "year", "month").parquet(
        config["paths"]["silver"]
    )

    summary = build_gold_tables(spark, config)
    assert summary.tables_created == 9
    assert tuple(table.name for table in summary.tables) == ALL_GOLD_TABLES

    for table in summary.tables:
        assert Path(table.parquet_path).is_dir()
        assert Path(table.export_parquet).is_file()
        assert Path(table.export_csv).is_file()
        assert Path(table.export_csv).read_text(encoding="utf-8").splitlines()[0]

    daily = spark.read.parquet(str(Path(config["paths"]["gold"]) / "descriptive_daily_demand"))
    financials = spark.read.parquet(
        str(Path(config["paths"]["gold"]) / "descriptive_service_financials")
    )
    assert daily.agg({"trip_count": "sum"}).first()[0] == 3
    assert financials.agg({"trip_count": "sum"}).first()[0] == 3
    assert {row.pickup_borough for row in daily.select("pickup_borough").collect()} == {
        "Manhattan",
        "Queens",
    }

    anomalies = spark.read.parquet(
        str(Path(config["paths"]["gold"]) / "diagnostic_daily_anomalies")
    )
    classification = spark.read.parquet(
        str(Path(config["paths"]["gold"]) / "model_classification_demand")
    )
    assert {"demand_zscore", "revenue_zscore", "is_anomaly"}.issubset(anomalies.columns)
    assert {
        "demand_threshold",
        "previous_observed_trip_count",
        "label_high_demand",
        "label",
    }.issubset(classification.columns)
