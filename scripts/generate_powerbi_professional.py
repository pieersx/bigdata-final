#!/usr/bin/env python3
# ruff: noqa
"""Genera el PBIP profesional: constelacion Gold, dimensiones, medidas y slicers."""

from __future__ import annotations

import csv
import json
import shutil
import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / "data" / "gold"
MODEL = ROOT / "powerbi" / "TLC_BigData.SemanticModel" / "definition"
REPORT = ROOT / "powerbi" / "TLC_BigData.Report" / "definition"
DIM_EXPORT = ROOT / "exports" / "powerbi" / "dimensions"
VISUAL_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.10.0/schema.json"


@dataclass(frozen=True)
class Fact:
    gold: str
    name: str
    partition: str = ""
    month_start: bool = False


FACTS = (
    Fact("descriptive_daily_demand", "Fact_DemandaDiaria", "year"),
    Fact("descriptive_hourly_profile", "Fact_PerfilHorario", "year"),
    Fact("descriptive_service_financials", "Fact_FinanzasServicio", "year", True),
    Fact("diagnostic_route_performance", "Fact_RendimientoRutas", "year_month", True),
    Fact("diagnostic_tip_factors", "Fact_FactoresPropina", "year_month", True),
    Fact("diagnostic_daily_anomalies", "Fact_AnomaliasDiarias", "year_month"),
    Fact("model_timeseries_daily", "Fact_PronosticoDemanda"),
    Fact("model_segmentation_zones", "Fact_ZonasSegmentadas"),
    Fact("model_classification_demand", "Fact_ClasificacionDemanda"),
    Fact("model_timeseries_metrics", "Fact_MetricasPronostico"),
    Fact("model_segmentation_profiles", "Fact_PerfilesSegmentacion"),
    Fact("model_segmentation_metrics", "Fact_MetricasSegmentacion"),
    Fact("model_classification_confusion", "Fact_MatrizConfusion"),
    Fact("model_classification_metrics", "Fact_MetricasClasificacion"),
    Fact("model_metrics", "Fact_MetricasModelos"),
)


MEASURES: dict[str, tuple[tuple[str, str, str], ...]] = {
    "Fact_DemandaDiaria": (
        ("D1 Viajes Totales", "SUM(Fact_DemandaDiaria[trip_count])", "#,0"),
        ("D1 Ingresos Totales", "SUM(Fact_DemandaDiaria[total_revenue])", "$#,0.00"),
        ("D1 Ingreso por Viaje", "DIVIDE([D1 Ingresos Totales], [D1 Viajes Totales])", "$#,0.00"),
        ("D1 Distancia Promedio", "DIVIDE(SUM(Fact_DemandaDiaria[distance_miles]), [D1 Viajes Totales])", "0.00 \"mi\""),
    ),
    "Fact_PerfilHorario": (
        ("D2 Viajes Totales", "SUM(Fact_PerfilHorario[trip_count])", "#,0"),
        ("D2 Duracion Promedio Ponderada", "DIVIDE(SUM(Fact_PerfilHorario[duration_minutes]), [D2 Viajes Totales])", "0.00 \"min\""),
        ("D2 Ingresos Totales", "SUM(Fact_PerfilHorario[total_revenue])", "$#,0.00"),
    ),
    "Fact_FinanzasServicio": (
        ("D3 Ingresos Totales", "SUM(Fact_FinanzasServicio[total_revenue])", "$#,0.00"),
        ("D3 Propinas Totales", "SUM(Fact_FinanzasServicio[tip_revenue])", "$#,0.00"),
        ("D3 Tasa Propina Ponderada", "DIVIDE(SUM(Fact_FinanzasServicio[tip_revenue]), SUM(Fact_FinanzasServicio[fare_revenue]))", "0.00%"),
        ("D3 Viajes con Propina %", "DIVIDE(SUM(Fact_FinanzasServicio[tipped_trips]), SUM(Fact_FinanzasServicio[trip_count]))", "0.00%"),
    ),
    "Fact_AnomaliasDiarias": (
        ("D4 Anomalias Detectadas", "CALCULATE(COUNTROWS(Fact_AnomaliasDiarias), Fact_AnomaliasDiarias[is_anomaly] = TRUE())", "#,0"),
        ("D4 Porcentaje Anomalias", "DIVIDE([D4 Anomalias Detectadas], COUNTROWS(Fact_AnomaliasDiarias))", "0.00%"),
        ("D4 ZScore Demanda Max", "MAXX(Fact_AnomaliasDiarias, ABS(Fact_AnomaliasDiarias[demand_zscore]))", "0.00"),
    ),
    "Fact_RendimientoRutas": (
        ("D5 Viajes Rutas", "SUM(Fact_RendimientoRutas[trip_count])", "#,0"),
        ("D5 Duracion Ponderada", "DIVIDE(SUMX(Fact_RendimientoRutas, Fact_RendimientoRutas[avg_trip_duration_minutes] * Fact_RendimientoRutas[trip_count]), [D5 Viajes Rutas])", "0.00 \"min\""),
        ("D5 Velocidad Ponderada", "DIVIDE(SUMX(Fact_RendimientoRutas, Fact_RendimientoRutas[avg_speed_mph] * Fact_RendimientoRutas[trip_count]), [D5 Viajes Rutas])", "0.00 \"mph\""),
    ),
    "Fact_FactoresPropina": (
        ("D6 Tasa Propina Ponderada", "DIVIDE(SUM(Fact_FactoresPropina[tip_revenue]), SUM(Fact_FactoresPropina[fare_revenue]))", "0.00%"),
        ("D6 Viajes con Propina %", "DIVIDE(SUM(Fact_FactoresPropina[tipped_trips]), SUM(Fact_FactoresPropina[trip_count]))", "0.00%"),
        ("D6 Viajes Analizados", "SUM(Fact_FactoresPropina[trip_count])", "#,0"),
    ),
    "Fact_PronosticoDemanda": (
        ("D7 Viajes Pronosticados", "SUM(Fact_PronosticoDemanda[forecast_trips])", "#,0"),
        ("D7 Limite Inferior 95", "SUM(Fact_PronosticoDemanda[forecast_lower_95])", "#,0"),
        ("D7 Limite Superior 95", "SUM(Fact_PronosticoDemanda[forecast_upper_95])", "#,0"),
    ),
    "Fact_ZonasSegmentadas": (
        ("D8 Zonas Segmentadas", "DISTINCTCOUNT(Fact_ZonasSegmentadas[pickup_location_id])", "#,0"),
        ("D8 Viajes Segmentados", "SUM(Fact_ZonasSegmentadas[total_trips])", "#,0"),
        ("D8 Ingreso por Viaje", "DIVIDE(SUM(Fact_ZonasSegmentadas[total_revenue]), SUM(Fact_ZonasSegmentadas[total_trips]))", "$#,0.00"),
    ),
    "Fact_ClasificacionDemanda": (
        ("D9 Casos Evaluados", "COUNTROWS(Fact_ClasificacionDemanda)", "#,0"),
        ("D9 Aciertos", "SUMX(Fact_ClasificacionDemanda, IF(Fact_ClasificacionDemanda[actual_high_demand] = Fact_ClasificacionDemanda[predicted_high_demand], 1, 0))", "#,0"),
        ("D9 Accuracy Calculada", "DIVIDE([D9 Aciertos], [D9 Casos Evaluados])", "0.00%"),
        ("D9 Probabilidad Promedio", "AVERAGE(Fact_ClasificacionDemanda[probability_high_demand])", "0.00%"),
    ),
    "Fact_MetricasPronostico": (
        ("D7 WMAPE", "DIVIDE(CALCULATE(MAX(Fact_MetricasPronostico[metric_value]), Fact_MetricasPronostico[metric_name] = \"wmape_percent\"), 100)", "0.00%"),
        ("D7 RMSE", "CALCULATE(MAX(Fact_MetricasPronostico[metric_value]), Fact_MetricasPronostico[metric_name] = \"rmse\")", "#,0.00"),
        ("D7 R2", "CALCULATE(MAX(Fact_MetricasPronostico[metric_value]), Fact_MetricasPronostico[metric_name] = \"r2\")", "0.0000"),
    ),
    "Fact_MetricasSegmentacion": (
        ("D8 Silhouette", "CALCULATE(MAX(Fact_MetricasSegmentacion[metric_value]), Fact_MetricasSegmentacion[metric_name] = \"silhouette\")", "0.0000"),
    ),
    "Fact_MetricasClasificacion": (
        ("D9 AUC", "CALCULATE(MAX(Fact_MetricasClasificacion[metric_value]), Fact_MetricasClasificacion[metric_name] = \"auc_roc\")", "0.0000"),
        ("D9 F1", "CALCULATE(MAX(Fact_MetricasClasificacion[metric_value]), Fact_MetricasClasificacion[metric_name] = \"f1\")", "0.00%"),
    ),
    "Fact_MatrizConfusion": (("D9 Matriz Casos", "SUM(Fact_MatrizConfusion[count])", "#,0"),),
}


PAGES = (
    ("tlc_01_descriptivo", "01 Resumen ejecutivo", "Fact_DemandaDiaria", "D1 Viajes Totales", "D1 Ingresos Totales", "pickup_borough", "pickup_date", "service", (("DimFecha", "Año"), ("DimServicio", "Servicio"))),
    ("tlc_02_descriptivo", "02 Demanda temporal", "Fact_PerfilHorario", "D2 Viajes Totales", "D2 Duracion Promedio Ponderada", "pickup_hour", "pickup_date", "service", (("DimFecha", "Año"), ("DimServicio", "Servicio"), ("DimHora", "Hora"))),
    ("tlc_03_descriptivo", "03 Ingresos y tarifas", "Fact_FinanzasServicio", "D3 Ingresos Totales", "D3 Tasa Propina Ponderada", "payment_type", "month_start", "service", (("DimFecha", "Año"), ("DimServicio", "Servicio"), ("DimPago", "TipoPago"))),
    ("tlc_04_diagnostico", "04 Causas del cambio", "Fact_AnomaliasDiarias", "D4 Anomalias Detectadas", "D4 Porcentaje Anomalias", "anomaly_direction", "pickup_date", "service", (("DimFecha", "Año"), ("DimServicio", "Servicio"), ("Fact_AnomaliasDiarias", "anomaly_direction"))),
    ("tlc_05_diagnostico", "05 Rutas y congestión", "Fact_RendimientoRutas", "D5 Viajes Rutas", "D5 Duracion Ponderada", "pickup_zone", "month_start", "service", (("DimFecha", "Año"), ("DimServicio", "Servicio"), ("DimZonaOrigen", "Borough"), ("DimZonaDestino", "Borough"))),
    ("tlc_06_diagnostico", "06 Propinas y anomalías", "Fact_FactoresPropina", "D6 Tasa Propina Ponderada", "D6 Viajes con Propina %", "pickup_borough", "month_start", "service", (("DimFecha", "Año"), ("DimServicio", "Servicio"), ("DimPago", "TipoPago"), ("DimHora", "Hora"))),
    ("tlc_07_predictivo", "07 Pronóstico de demanda", "Fact_PronosticoDemanda", "D7 Viajes Pronosticados", "D7 Limite Superior 95", "service", "forecast_date", "service", (("DimFecha", "Año"), ("DimServicio", "Servicio"), ("Fact_PronosticoDemanda", "horizon_day"))),
    ("tlc_08_predictivo", "08 Segmentación de zonas", "Fact_ZonasSegmentadas", "D8 Zonas Segmentadas", "D8 Ingreso por Viaje", "segment_label", "pickup_location_id", "borough", (("DimCluster", "Segmento"), ("DimZonaOrigen", "Borough"))),
    ("tlc_09_predictivo", "09 Clasificación de alta demanda", "Fact_ClasificacionDemanda", "D9 Accuracy Calculada", "D9 Casos Evaluados", "dataset_split", "prediction_date", "predicted_high_demand", (("DimFecha", "Año"), ("Fact_ClasificacionDemanda", "dataset_split"), ("Fact_ClasificacionDemanda", "predicted_high_demand"))),
    ("tlc_10_auditoria", "10 Control y auditoría", "D10_Auditoria", "A10 Eventos", "A10 Eventos OK", "Categoría", "Fecha", "Estado", (("D10_Auditoria", "Estado"), ("D10_Auditoria", "Servicio"))),
)


def dump(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_dimensions() -> None:
    DIM_EXPORT.mkdir(parents=True, exist_ok=True)
    with (DIM_EXPORT / "DimFecha.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(("Fecha", "Año", "MesNumero", "Mes", "Trimestre", "AñoMes", "AñoMesOrden", "DiaSemanaNumero", "DiaSemana", "EsFinSemana"))
        d = date(2023, 1, 1)
        names = ("lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo")
        months = ("enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre")
        while d <= date(2026, 12, 31):
            w.writerow((d.isoformat(), d.year, d.month, months[d.month-1], f"T{(d.month-1)//3+1}", f"{d.year}-{d.month:02d}", d.year*100+d.month, d.weekday()+1, names[d.weekday()], d.weekday() >= 5)); d += timedelta(days=1)
    services = (("yellow", "Yellow Taxi"), ("green", "Green Taxi"), ("fhv", "For-Hire Vehicle"), ("fhvhv", "High Volume FHV"))
    with (DIM_EXPORT / "DimServicio.csv").open("w", newline="", encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(("Servicio", "ServicioNombre")); w.writerows(services)
    with (DIM_EXPORT / "DimPago.csv").open("w", newline="", encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(("TipoPago", "PagoNombre")); w.writerows(((1,"Tarjeta"),(2,"Efectivo"),(3,"Sin cargo"),(4,"Disputa"),(5,"Desconocido"),(6,"Viaje anulado")))
    with (DIM_EXPORT / "DimHora.csv").open("w", newline="", encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(("Hora", "FranjaHoraria", "EsHoraPico")); w.writerows((h, "Madrugada" if h<6 else "Mañana" if h<12 else "Tarde" if h<18 else "Noche", (7<=h<=9 or 16<=h<=19)) for h in range(24))
    lookup = ROOT / "data" / "bronze" / "reference" / "taxi_zone_lookup.csv"
    for dim in ("DimZonaOrigen", "DimZonaDestino"):
        shutil.copyfile(lookup, DIM_EXPORT / f"{dim}.csv")
    with (DIM_EXPORT / "DimCluster.csv").open("w", newline="", encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(("SegmentoID", "Segmento")); w.writerows(((0,"Segmento 0"),(1,"Segmento 1"),(2,"Segmento 2"),(3,"Segmento 3")))


def arrow_type(field) -> tuple[str, str | None]:
    value = str(field.type)
    if value.startswith("date"):
        return "dateTime", "Short Date"
    if value.startswith("timestamp"):
        return "dateTime", "General Date"
    if value.startswith("int") or value.startswith("uint"):
        return "int64", "0"
    if value in {"double", "float"}:
        return "double", "#,0.00"
    if value == "bool":
        return "boolean", None
    return "string", None


def fact_schema(fact: Fact):
    sample = next((GOLD / fact.gold).rglob("*.parquet"))
    fields = list(pq.ParquetFile(sample).schema_arrow)
    names = {f.name for f in fields}
    extras = []
    if fact.partition in {"year", "year_month"} and "pickup_year" not in names:
        extras.append(("pickup_year", "int64", "0"))
    if fact.partition == "year_month" and "pickup_month" not in names:
        extras.append(("pickup_month", "int64", "0"))
    if fact.month_start:
        extras.append(("month_start", "dateTime", "Short Date"))
    return [(f.name, *arrow_type(f)) for f in fields] + extras


def parquet_m(fact: Fact) -> str:
    path = str((GOLD / fact.gold).resolve()).replace("\\", "/")
    additions = "t"
    if fact.partition in {"year", "year_month"}:
        additions = 'Table.AddColumn(t, "pickup_year", each y, Int64.Type)'
    if fact.partition == "year_month":
        additions = f'Table.AddColumn({additions}, "pickup_month", each m, Int64.Type)'
    inner = f"let p=Text.Replace([Folder Path], \"\\\\\", \"/\"), y=try Number.FromText(Text.BeforeDelimiter(Text.AfterDelimiter(p, \"pickup_year=\"), \"/\")) otherwise null, m=try Number.FromText(Text.BeforeDelimiter(Text.AfterDelimiter(p, \"pickup_month=\"), \"/\")) otherwise null, t=Parquet.Document([Content]) in {additions}"
    final = "Table.Combine(ConDatos[Data])"
    if fact.month_start:
        final = f'Table.AddColumn(Table.Combine(ConDatos[Data]), "month_start", each try #date([pickup_year], [pickup_month], 1) otherwise null, type date)'
    return f'let\n    Origen = Folder.Files("{path}"),\n    Parquet = Table.SelectRows(Origen, each [Extension] = ".parquet"),\n    ConDatos = Table.AddColumn(Parquet, "Data", each {inner}),\n    Combinado = {final}\nin\n    Combinado'


def table_tmdl(fact: Fact) -> str:
    lines = [f"table {fact.name}"]
    for name, dax, fmt in MEASURES.get(fact.name, ()):
        lines += [f"\n\tmeasure '{name}' = {dax}", f"\t\tformatString: {fmt}", "\t\tdisplayFolder: Medidas"]
    for name, dtype, fmt in fact_schema(fact):
        lines += [f"\n\tcolumn '{name}'" if " " in name else f"\n\tcolumn {name}", f"\t\tdataType: {dtype}"]
        if fmt: lines.append(f"\t\tformatString: {fmt}")
        lines += ["\t\tsummarizeBy: none", f"\t\tsourceColumn: {name}"]
    source = parquet_m(fact).replace("\n", "\n\t\t\t\t")
    lines += [f"\n\tpartition {fact.name} = m", "\t\tmode: import", "\t\tsource =", f"\t\t\t\t{source}", "", "\tannotation PBI_ResultType = Table", ""]
    return "\n".join(lines)


DIMENSIONS = {
    "DimFecha": (("Fecha","dateTime","date"),("Año","int64","int"),("MesNumero","int64","int"),("Mes","string","text"),("Trimestre","string","text"),("AñoMes","string","text"),("AñoMesOrden","int64","int"),("DiaSemanaNumero","int64","int"),("DiaSemana","string","text"),("EsFinSemana","boolean","logical")),
    "DimServicio": (("Servicio","string","text"),("ServicioNombre","string","text")),
    "DimPago": (("TipoPago","int64","int"),("PagoNombre","string","text")),
    "DimHora": (("Hora","int64","int"),("FranjaHoraria","string","text"),("EsHoraPico","boolean","logical")),
    "DimZonaOrigen": (("LocationID","int64","int"),("Borough","string","text"),("Zone","string","text"),("service_zone","string","text")),
    "DimZonaDestino": (("LocationID","int64","int"),("Borough","string","text"),("Zone","string","text"),("service_zone","string","text")),
    "DimCluster": (("SegmentoID","int64","int"),("Segmento","string","text")),
}


def dim_tmdl(name: str) -> str:
    cols = DIMENSIONS[name]
    path = str((DIM_EXPORT / f"{name}.csv").resolve()).replace("\\", "/")
    count = len(cols)
    transforms = ", ".join(f'{{"{c}", type date}}' if t=="date" else f'{{"{c}", Int64.Type}}' if t=="int" else f'{{"{c}", type logical}}' if t=="logical" else f'{{"{c}", type text}}' for c,_,t in cols)
    lines=[f"table {name}"]
    for c,dtype,_ in cols:
        lines += [f"\n\tcolumn '{c}'", f"\t\tdataType: {dtype}", "\t\tsummarizeBy: none", f"\t\tsourceColumn: {c}"]
    m=f'let\n    Origen = Csv.Document(File.Contents("{path}"),[Delimiter=",", Columns={count}, Encoding=65001, QuoteStyle=QuoteStyle.Csv]),\n    Encabezados = Table.PromoteHeaders(Origen, [PromoteAllScalars=true]),\n    Tipos = Table.TransformColumnTypes(Encabezados,{{{transforms}}}, "es-ES")\nin\n    Tipos'
    lines += [f"\n\tpartition {name} = m", "\t\tmode: import", "\t\tsource =", "\t\t\t\t"+m.replace("\n","\n\t\t\t\t"), "", "\tannotation PBI_ResultType = Table", ""]
    return "\n".join(lines)


def audit_tmdl() -> str:
    old = ROOT / "powerbi" / "TLC_BigData.SemanticModel" / "definition" / "tables" / "D10_Auditoria.tmdl"
    if old.exists():
        content=old.read_text(encoding="utf-8")
        content=content.replace("measure '10 Total valor' = SUM([Valor])", "measure 'A10 Eventos' = SUM([Valor])").replace("measure '10 Promedio auxiliar' = AVERAGE([Valor auxiliar])", "measure 'A10 Eventos OK' = CALCULATE(SUM([Valor]), D10_Auditoria[Estado] = \"PASSED\" || D10_Auditoria[Estado] = \"OK\")")
        return content
    raise FileNotFoundError(old)


def relationships_tmdl() -> str:
    rels=[]
    def add(fact,col,dim,dcol):
        rels.extend((f"relationship {uuid.uuid4()}", f"\tfromColumn: {fact}.{col}", f"\ttoColumn: {dim}.'{dcol}'", "",))
    for fact,col in (("Fact_DemandaDiaria","pickup_date"),("Fact_PerfilHorario","pickup_date"),("Fact_AnomaliasDiarias","pickup_date"),("Fact_PronosticoDemanda","forecast_date"),("Fact_ClasificacionDemanda","prediction_date"),("Fact_FinanzasServicio","month_start"),("Fact_RendimientoRutas","month_start"),("Fact_FactoresPropina","month_start")): add(fact,col,"DimFecha","Fecha")
    for fact in ("Fact_DemandaDiaria","Fact_PerfilHorario","Fact_FinanzasServicio","Fact_RendimientoRutas","Fact_FactoresPropina","Fact_AnomaliasDiarias","Fact_PronosticoDemanda"): add(fact,"service","DimServicio","Servicio")
    for fact in ("Fact_DemandaDiaria","Fact_PerfilHorario","Fact_RendimientoRutas","Fact_AnomaliasDiarias","Fact_ZonasSegmentadas","Fact_ClasificacionDemanda"): add(fact,"pickup_location_id","DimZonaOrigen","LocationID")
    add("Fact_RendimientoRutas","dropoff_location_id","DimZonaDestino","LocationID")
    for fact in ("Fact_FinanzasServicio","Fact_FactoresPropina"): add(fact,"payment_type","DimPago","TipoPago")
    for fact in ("Fact_PerfilHorario","Fact_FactoresPropina"): add(fact,"pickup_hour","DimHora","Hora")
    add("Fact_ZonasSegmentadas","segment_id","DimCluster","SegmentoID"); add("Fact_PerfilesSegmentacion","segment_id","DimCluster","SegmentoID")
    return "\n".join(rels)


def field(entity: str, prop: str, measure: bool=False):
    kind="Measure" if measure else "Column"
    return {kind:{"Expression":{"SourceRef":{"Entity":entity}},"Property":prop}}


def projection(value, query, native): return {"field":value,"queryRef":query,"nativeQueryRef":native}
def base(name,x,y,w,h,z): return {"$schema":VISUAL_SCHEMA,"name":name,"position":{"x":x,"y":y,"z":z,"height":h,"width":w,"tabOrder":z}}


def card(name, entity, measure, x, y, z):
    v=base(name,x,y,230,115,z); f=field(entity,measure,True)
    v["visual"]={"visualType":"cardVisual","query":{"queryState":{"Data":{"projections":[projection(f,f"{entity}.{measure}",measure)]}}},"drillFilterOtherVisuals":True}
    return v


def chart(name, vtype, entity, category, measure, x,y,w,h,z,series=None, ascending=False):
    v=base(name,x,y,w,h,z); c=field(entity,category); m=field(entity,measure,True)
    state={"Category":{"projections":[projection(c,f"{entity}.{category}",category)]},"Y":{"projections":[projection(m,f"{entity}.{measure}",measure)]}}
    if series:
        s=field(entity,series); state["Series"]={"projections":[projection(s,f"{entity}.{series}",series)]}
    v["visual"]={"visualType":vtype,"query":{"queryState":state,"sortDefinition":{"sort":[{"field":c if ascending else m,"direction":"Ascending" if ascending else "Descending"}],"isDefaultSort":True}},"drillFilterOtherVisuals":True}
    return v


def slicer(name, entity, prop, x,y,z):
    v=base(name,x,y,185,72,z); f=field(entity,prop)
    v["visual"]={"visualType":"slicer","query":{"queryState":{"Values":{"projections":[projection(f,f"{entity}.{prop}",prop)]}}},"drillFilterOtherVisuals":True}
    return v


def build_report():
    pages=REPORT/"pages"
    if pages.exists(): shutil.rmtree(pages)
    pages.mkdir(parents=True)
    order=[]
    for page_name,title,entity,kpi1,kpi2,category,date_col,series,slicers in PAGES:
        order.append(page_name); root=pages/page_name
        dump(root/"page.json",{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json","name":page_name,"displayName":title,"displayOption":"FitToPage","height":720,"width":1280})
        visuals={"kpi1":card("kpi1",entity,kpi1,20,95,0),"kpi2":card("kpi2",entity,kpi2,260,95,1),"categorias":chart("categorias","clusteredBarChart",entity,category,kpi1,20,225,600,220,2),"tendencia":chart("tendencia","lineChart",entity,date_col,kpi1,640,225,620,220,3,series,True),"comparacion":chart("comparacion","clusteredColumnChart",entity,series,kpi2,20,465,600,225,4),"detalle":chart("detalle","donutChart",entity,category,kpi1,640,465,620,225,5)}
        predictive = {
            "tlc_07_predictivo": ("Fact_MetricasPronostico", "D7 WMAPE"),
            "tlc_08_predictivo": ("Fact_MetricasSegmentacion", "D8 Silhouette"),
            "tlc_09_predictivo": ("Fact_MetricasClasificacion", "D9 AUC"),
        }
        if page_name in predictive:
            metric_entity, metric_measure = predictive[page_name]
            visuals["kpi_modelo"] = card("kpi_modelo", metric_entity, metric_measure, 500, 95, 6)
        for i,(se,sp) in enumerate(slicers): visuals[f"slicer_{i}"]=slicer(f"slicer_{i}",se,sp,20+i*195,15,10+i)
        for n,v in visuals.items(): dump(root/"visuals"/n/"visual.json",v)
    dump(pages/"pages.json",{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.1.0/schema.json","pageOrder":order,"activePageName":order[0]})


def main() -> None:
    write_dimensions()
    tables=MODEL/"tables"
    audit=audit_tmdl()
    if tables.exists(): shutil.rmtree(tables)
    tables.mkdir(parents=True)
    for fact in FACTS: (tables/f"{fact.name}.tmdl").write_text(table_tmdl(fact),encoding="utf-8",newline="\n")
    for dim in DIMENSIONS: (tables/f"{dim}.tmdl").write_text(dim_tmdl(dim),encoding="utf-8",newline="\n")
    (tables/"D10_Auditoria.tmdl").write_text(audit,encoding="utf-8",newline="\n")
    names=[f.name for f in FACTS]+list(DIMENSIONS)+["D10_Auditoria"]
    refs="\n".join(f"ref table {n}" for n in names)
    order=",".join(json.dumps(n) for n in names)
    model=f'model Model\n\tculture: es-ES\n\tdefaultPowerBIDataSourceVersion: powerBI_V3\n\tsourceQueryCulture: es-ES\n\tdataAccessOptions\n\t\tlegacyRedirects\n\t\treturnErrorValuesAsNull\n\nannotation __PBI_TimeIntelligenceEnabled = 0\nannotation PBI_ProTooling = ["DevMode"]\nannotation PBI_QueryOrder = [{order}]\n\n{refs}\n\nref cultureInfo es-ES\n'
    (MODEL/"model.tmdl").write_text(model,encoding="utf-8",newline="\n")
    (MODEL/"relationships.tmdl").write_text(relationships_tmdl(),encoding="utf-8",newline="\n")
    build_report()
    print(f"PBIP profesional generado: {len(FACTS)} hechos Gold, {len(DIMENSIONS)} dimensiones, 10 páginas")


if __name__ == "__main__": main()
