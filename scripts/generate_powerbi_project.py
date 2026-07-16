#!/usr/bin/env python3
# ruff: noqa: E501
"""Genera el proyecto PBIP versionable de 10 páginas para el caso NYC TLC.

El script solo usa la biblioteca estándar. Las definiciones PBIR/TMDL se basan en
los formatos que Power BI Desktop genera al guardar un proyecto PBIP. Se ejecuta
en Windows para que las particiones M queden apuntando a la ruta local de exports.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Dashboard:
    code: str
    table: str
    title: str
    category: str
    description: str


DASHBOARDS = (
    Dashboard(
        "01",
        "D01_Resumen",
        "01 Resumen ejecutivo",
        "descriptivo",
        "KPIs globales, demanda e ingresos",
    ),
    Dashboard(
        "02",
        "D02_Demanda",
        "02 Demanda temporal",
        "descriptivo",
        "Evolución diaria, mensual y por servicio",
    ),
    Dashboard(
        "03",
        "D03_Ingresos",
        "03 Ingresos y tarifas",
        "descriptivo",
        "Ingresos, tarifas, distancia y propinas",
    ),
    Dashboard(
        "04",
        "D04_Causas",
        "04 Causas del cambio",
        "diagnóstico",
        "Descomposición interanual y factores de cambio",
    ),
    Dashboard(
        "05",
        "D05_Rutas",
        "05 Rutas y congestión",
        "diagnóstico",
        "Duración, velocidad y desempeño de rutas",
    ),
    Dashboard(
        "06",
        "D06_Anomalias",
        "06 Propinas y anomalías",
        "diagnóstico",
        "Factores de propina y eventos atípicos",
    ),
    Dashboard(
        "07",
        "D07_Pronostico",
        "07 Pronóstico de demanda",
        "predictivo",
        "Serie de tiempo, validación y horizonte futuro",
    ),
    Dashboard(
        "08",
        "D08_Segmentacion",
        "08 Segmentación de zonas",
        "predictivo",
        "Clusters K-Means y perfiles operativos",
    ),
    Dashboard(
        "09",
        "D09_Clasificacion",
        "09 Clasificación de alta demanda",
        "predictivo",
        "Probabilidad, clases y matriz de confusión",
    ),
    Dashboard(
        "10",
        "D10_Auditoria",
        "10 Control y auditoría",
        "auditoría",
        "Ejecuciones, archivos, calidad y modelos",
    ),
)

CSV_HEADER = (
    "date",
    "year",
    "month",
    "service",
    "category",
    "series",
    "metric_name",
    "metric_value",
    "metric_aux",
    "status",
    "detail",
)

VISUAL_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/fabric/item/report/"
    "definition/visualContainer/2.10.0/schema.json"
)


def _dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _slug(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    return "".join(character if character.isalnum() else "_" for character in normalized).strip("_")


def _column(entity: str, prop: str) -> dict[str, Any]:
    return {
        "Column": {
            "Expression": {"SourceRef": {"Entity": entity}},
            "Property": prop,
        }
    }


def _aggregation(entity: str, prop: str, function: int = 0) -> dict[str, Any]:
    return {
        "Aggregation": {
            "Expression": {"Column": _column(entity, prop)["Column"]},
            "Function": function,
        }
    }


def _projection(field: dict[str, Any], query_ref: str, native_ref: str) -> dict[str, Any]:
    return {"field": field, "queryRef": query_ref, "nativeQueryRef": native_ref}


def _visual_base(name: str, x: int, y: int, width: int, height: int, z: int) -> dict[str, Any]:
    return {
        "$schema": VISUAL_SCHEMA,
        "name": name,
        "position": {
            "x": x,
            "y": y,
            "z": z,
            "height": height,
            "width": width,
            "tabOrder": z,
        },
    }


def _card(name: str, entity: str, prop: str, x: int, y: int, z: int) -> dict[str, Any]:
    visual = _visual_base(name, x, y, 250, 135, z)
    agg = _aggregation(entity, prop)
    visual["visual"] = {
        "visualType": "cardVisual",
        "query": {
            "queryState": {
                "Data": {
                    "projections": [_projection(agg, f"Sum({entity}.{prop})", f"Suma de {prop}")]
                }
            }
        },
        "drillFilterOtherVisuals": True,
    }
    visual["filterConfig"] = {"filters": [{"name": f"f_{name}", "field": agg, "type": "Advanced"}]}
    return visual


def _category_chart(
    name: str,
    entity: str,
    visual_type: str,
    category_prop: str,
    value_prop: str,
    x: int,
    y: int,
    width: int,
    height: int,
    z: int,
    series_prop: str | None = None,
) -> dict[str, Any]:
    visual = _visual_base(name, x, y, width, height, z)
    category = _column(entity, category_prop)
    value = _aggregation(entity, value_prop)
    state: dict[str, Any] = {
        "Category": {
            "projections": [_projection(category, f"{entity}.{category_prop}", category_prop)]
        },
        "Y": {
            "projections": [
                _projection(value, f"Sum({entity}.{value_prop})", f"Suma de {value_prop}")
            ]
        },
    }
    filters: list[dict[str, Any]] = [
        {"name": f"fc_{name}", "field": category, "type": "Categorical"},
        {"name": f"fv_{name}", "field": value, "type": "Advanced"},
    ]
    if series_prop:
        series = _column(entity, series_prop)
        state["Series"] = {
            "projections": [_projection(series, f"{entity}.{series_prop}", series_prop)]
        }
        filters.append({"name": f"fs_{name}", "field": series, "type": "Categorical"})

    visual["visual"] = {
        "visualType": visual_type,
        "query": {
            "queryState": state,
            "sortDefinition": {
                "sort": [{"field": value, "direction": "Descending"}],
                "isDefaultSort": True,
            },
        },
        "drillFilterOtherVisuals": True,
    }
    visual["filterConfig"] = {"filters": filters}
    return visual


def _page_visuals(dashboard: Dashboard) -> dict[str, dict[str, Any]]:
    entity = dashboard.table
    prefix = dashboard.table.lower()
    return {
        f"{prefix}_kpi_principal": _card(f"{prefix}_kpi_principal", entity, "Valor", 20, 20, 0),
        f"{prefix}_kpi_auxiliar": _card(
            f"{prefix}_kpi_auxiliar", entity, "Valor auxiliar", 20, 165, 1
        ),
        f"{prefix}_barras": _category_chart(
            f"{prefix}_barras",
            entity,
            "clusteredBarChart",
            "Categoría",
            "Valor",
            290,
            20,
            460,
            300,
            2,
        ),
        f"{prefix}_linea": _category_chart(
            f"{prefix}_linea",
            entity,
            "lineChart",
            "Fecha",
            "Valor",
            760,
            20,
            500,
            300,
            3,
            "Serie",
        ),
        f"{prefix}_columnas": _category_chart(
            f"{prefix}_columnas",
            entity,
            "clusteredColumnChart",
            "Servicio",
            "Valor auxiliar",
            20,
            340,
            610,
            350,
            4,
            "Serie",
        ),
        f"{prefix}_donut": _category_chart(
            f"{prefix}_donut",
            entity,
            "donutChart",
            "Estado",
            "Valor",
            650,
            340,
            610,
            350,
            5,
        ),
    }


def _table_tmdl(dashboard: Dashboard, csv_path: Path | str) -> str:
    if isinstance(csv_path, Path):
        m_path = str(csv_path.resolve())
    else:
        m_path = csv_path
    m_path = m_path.replace("\\", "/")
    return f"""table {dashboard.table}
\tmeasure '{dashboard.code} Total valor' = SUM([Valor])
\t\tformatString: #,0.00

\tmeasure '{dashboard.code} Promedio auxiliar' = AVERAGE([Valor auxiliar])
\t\tformatString: #,0.00

\tcolumn Fecha
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: date

\tcolumn Año
\t\tdataType: int64
\t\tformatString: 0
\t\tsummarizeBy: none
\t\tsourceColumn: year

\tcolumn Mes
\t\tdataType: int64
\t\tformatString: 0
\t\tsummarizeBy: none
\t\tsourceColumn: month

\tcolumn Servicio
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: service

\tcolumn Categoría
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: category

\tcolumn Serie
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: series

\tcolumn Indicador
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: metric_name

\tcolumn Valor
\t\tdataType: double
\t\tformatString: #,0.00
\t\tsummarizeBy: sum
\t\tsourceColumn: metric_value

\tcolumn 'Valor auxiliar'
\t\tdataType: double
\t\tformatString: #,0.00
\t\tsummarizeBy: sum
\t\tsourceColumn: metric_aux

\tcolumn Estado
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: status

\tcolumn Detalle
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: detail

\tpartition {dashboard.table} = m
\t\tmode: import
\t\tsource =
\t\t\t\tlet
\t\t\t\t    Origen = Csv.Document(File.Contents(\"{m_path}\"),[Delimiter=\",\", Columns=11, Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
\t\t\t\t    #\"Encabezados promovidos\" = Table.PromoteHeaders(Origen, [PromoteAllScalars=true]),
\t\t\t\t    #\"Tipo cambiado\" = Table.TransformColumnTypes(#\"Encabezados promovidos\",{{{{\"date\", type text}}, {{\"year\", Int64.Type}}, {{\"month\", Int64.Type}}, {{\"service\", type text}}, {{\"category\", type text}}, {{\"series\", type text}}, {{\"metric_name\", type text}}, {{\"metric_value\", type number}}, {{\"metric_aux\", type number}}, {{\"status\", type text}}, {{\"detail\", type text}}}}, \"en-US\")
\t\t\t\tin
\t\t\t\t    #\"Tipo cambiado\"

\tannotation PBI_ResultType = Table
"""


def _model_tmdl() -> str:
    order = ",".join(json.dumps(d.table, ensure_ascii=False) for d in DASHBOARDS)
    refs = "\n".join(f"ref table {d.table}" for d in DASHBOARDS)
    return f"""model Model
\tculture: es-ES
\tdefaultPowerBIDataSourceVersion: powerBI_V3
\tsourceQueryCulture: es-ES
\tdataAccessOptions
\t\tlegacyRedirects
\t\treturnErrorValuesAsNull

annotation __PBI_TimeIntelligenceEnabled = 0

annotation PBI_ProTooling = [\"DevMode\"]

annotation PBI_QueryOrder = [{order}]

{refs}

ref cultureInfo es-ES
"""


def create_placeholders(export_dir: Path) -> None:
    export_dir.mkdir(parents=True, exist_ok=True)
    for index, dashboard in enumerate(DASHBOARDS, start=1):
        path = export_dir / f"{dashboard.table}.csv"
        if path.exists():
            continue
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_HEADER)
            writer.writeheader()
            for offset, service in enumerate(("yellow", "green", "fhvhv"), start=1):
                writer.writerow(
                    {
                        "date": f"2026-0{offset}-01",
                        "year": 2026,
                        "month": offset,
                        "service": service,
                        "category": f"Categoría {offset}",
                        "series": "Ejemplo",
                        "metric_name": dashboard.description,
                        "metric_value": index * 1000 + offset * 100,
                        "metric_aux": index * 10 + offset,
                        "status": "PLACEHOLDER",
                        "detail": "Se reemplaza al ejecutar el pipeline completo",
                    }
                )


def generate(
    root: Path,
    create_sample_data: bool = False,
    source_dir: str | None = None,
) -> None:
    project_root = root / "powerbi"
    report_root = project_root / "TLC_BigData.Report"
    model_root = project_root / "TLC_BigData.SemanticModel"
    export_dir = root / "exports" / "powerbi"
    model_source_dir = source_dir or str(export_dir.resolve())

    if create_sample_data:
        create_placeholders(export_dir)

    tables_dir = model_root / "definition" / "tables"
    if tables_dir.exists():
        shutil.rmtree(tables_dir)
    tables_dir.mkdir(parents=True, exist_ok=True)
    for stale in (model_root / "definition" / "relationships.tmdl",):
        if stale.exists():
            stale.unlink()

    (model_root / "definition" / "model.tmdl").write_text(
        _model_tmdl(), encoding="utf-8", newline="\n"
    )
    for dashboard in DASHBOARDS:
        table_path = tables_dir / f"{dashboard.table}.tmdl"
        table_path.write_text(
            _table_tmdl(dashboard, f"{model_source_dir}/{dashboard.table}.csv"),
            encoding="utf-8",
            newline="\n",
        )

    pages_root = report_root / "definition" / "pages"
    if pages_root.exists():
        shutil.rmtree(pages_root)
    pages_root.mkdir(parents=True, exist_ok=True)

    page_order: list[str] = []
    for dashboard in DASHBOARDS:
        page_name = f"tlc_{dashboard.code}_{_slug(dashboard.category)}"
        page_order.append(page_name)
        page_root = pages_root / page_name
        _dump(
            page_root / "page.json",
            {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json",
                "name": page_name,
                "displayName": dashboard.title,
                "displayOption": "FitToPage",
                "height": 720,
                "width": 1280,
                "annotations": [
                    {"name": "category", "value": dashboard.category},
                    {"name": "description", "value": dashboard.description},
                ],
            },
        )
        for visual_name, payload in _page_visuals(dashboard).items():
            _dump(page_root / "visuals" / visual_name / "visual.json", payload)

    _dump(
        pages_root / "pages.json",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.1.0/schema.json",
            "pageOrder": page_order,
            "activePageName": page_order[0],
        },
    )

    print(f"PBIP generado: {len(DASHBOARDS)} páginas, {len(DASHBOARDS) * 6} visuales")
    print(f"Contratos de datos: {export_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Raíz del repositorio",
    )
    parser.add_argument(
        "--create-placeholders",
        action="store_true",
        help="Crea CSV mínimos solo para validar la apertura del PBIP",
    )
    parser.add_argument(
        "--source-dir",
        help=(
            "Ruta absoluta que Power BI Desktop usará para leer los CSV. "
            "Es necesaria al generar dentro de Docker para apuntar al host Windows."
        ),
    )
    args = parser.parse_args()
    generate(
        args.root.resolve(),
        create_sample_data=args.create_placeholders,
        source_dir=args.source_dir,
    )


if __name__ == "__main__":
    main()
