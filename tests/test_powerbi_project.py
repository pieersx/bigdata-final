from __future__ import annotations

import re
from pathlib import Path

from tlc_pipeline.config import load_config
from tlc_pipeline.powerbi import CONTRACTS
from tlc_pipeline.verify import _check_powerbi


def test_native_pbip_has_ten_pages_visuals_and_contract_bindings(tmp_path: Path) -> None:
    exports = tmp_path / "exports"
    contracts = exports / "powerbi"
    contracts.mkdir(parents=True)
    for name in CONTRACTS:
        (contracts / f"{name}.csv").write_text(
            "date,year,month,service,category,series,metric_name,metric_value,metric_aux,status,detail\n"
            "2025-01-01,2025,1,yellow,NYC,Total,Viajes,1,1,OK,Prueba estructural\n",
            encoding="utf-8",
        )
    config = load_config(env={"TLC_EXPORTS_PATH": str(exports)})
    powerbi = Path(__file__).resolve().parents[1] / "powerbi"

    result = _check_powerbi(config, powerbi_path=powerbi)

    assert result["pages"] == 10
    assert sum(result["visuals"].values()) >= 80
    assert len(result["contracts"]) == 10


def test_semantic_model_measure_names_are_globally_unique() -> None:
    tables = (
        Path(__file__).resolve().parents[1]
        / "powerbi"
        / "TLC_BigData.SemanticModel"
        / "definition"
        / "tables"
    )
    measure_pattern = re.compile(r"^\s*measure\s+'([^']+)'", re.MULTILINE)
    names = [
        name
        for table in tables.glob("*.tmdl")
        for name in measure_pattern.findall(table.read_text(encoding="utf-8"))
    ]

    assert len(names) == len(set(names)), "Power BI exige nombres de medida únicos en el modelo"


def test_professional_semantic_model_loads_all_gold_facts_and_dimensions() -> None:
    tables = (
        Path(__file__).resolve().parents[1]
        / "powerbi"
        / "TLC_BigData.SemanticModel"
        / "definition"
        / "tables"
    )

    facts = sorted(path.stem for path in tables.glob("Fact_*.tmdl"))
    dimensions = sorted(path.stem for path in tables.glob("Dim*.tmdl"))
    assert len(facts) == 15
    assert len(dimensions) == 7
    content = "\n".join(path.read_text(encoding="utf-8") for path in tables.glob("*.tmdl"))
    assert "Folder.Files(\"C:/" in content
    assert "Parquet.Document([Content])" in content
    assert "/workspace/" not in content


def test_professional_model_has_relationships_measures_and_slicers() -> None:
    root = Path(__file__).resolve().parents[1] / "powerbi"
    relationships = (
        root / "TLC_BigData.SemanticModel" / "definition" / "relationships.tmdl"
    ).read_text(encoding="utf-8")
    assert relationships.count("relationship ") >= 25
    assert "DimFecha.'Fecha'" in relationships
    assert "DimServicio.'Servicio'" in relationships
    assert "DimZonaOrigen.'LocationID'" in relationships
    assert "DimZonaDestino.'LocationID'" in relationships

    tables = root / "TLC_BigData.SemanticModel" / "definition" / "tables"
    measures = "\n".join(path.read_text(encoding="utf-8") for path in tables.glob("*.tmdl"))
    for expected in (
        "D3 Tasa Propina Ponderada",
        "D4 Porcentaje Anomalias",
        "D7 WMAPE",
        "D8 Silhouette",
        "D9 AUC",
        "D9 Accuracy Calculada",
    ):
        assert expected in measures

    visuals = root / "TLC_BigData.Report" / "definition" / "pages"
    slicer_count = sum(
        '"visualType": "slicer"' in path.read_text(encoding="utf-8")
        for path in visuals.rglob("visual.json")
    )
    assert slicer_count >= 20
