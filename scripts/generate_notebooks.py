from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import nbformat as nbf

ROOT = Path(__file__).parents[1]
OUT = ROOT / "notebooks"
OUT.mkdir(exist_ok=True)


def md(text: str):
    return nbf.v4.new_markdown_cell(dedent(text).strip())


def code(text: str):
    return nbf.v4.new_code_cell(dedent(text).strip())


def common_setup(title: str):
    return [
        md(f"""
        # {title}

        ## Objetivo

        Este notebook sirve como guion técnico y visual para la exposición. Todas las
        comprobaciones usan los artefactos completos del proyecto; las vistas de pocas filas
        son únicamente para mostrar el esquema, no son el conjunto usado por el pipeline.

        **QUÉ EXPLICAR:** primero diga qué problema resuelve esta etapa, después muestre la
        evidencia y termine conectándola con la siguiente capa.
        """),
        md("""
        ## Preparación

        Ejecútelo desde el contenedor `spark` con la raíz `/workspace`. La celda también
        encuentra el repositorio cuando el notebook se abre desde la carpeta local.
        """),
        code("""
        from pathlib import Path
        import json

        # Encontrar la raíz permite usar el mismo notebook en Docker o en el repositorio local.
        candidates = [Path.cwd(), Path.cwd().parent, Path('/workspace')]
        ROOT = next(
            path for path in candidates
            if (path / 'config' / 'pipeline.yaml').exists()
        )
        DATA = ROOT / 'data'
        EXPORTS = ROOT / 'exports'
        print(f'Raíz del proyecto: {ROOT}')
        """),
        md("## Pasos"),
    ]


def save(name: str, cells: list):
    notebook = nbf.v4.new_notebook(
        cells=cells,
        metadata={
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.11"},
        },
    )
    nbf.write(notebook, OUT / name)


cells = common_setup("00 — Ingesta automática del año 2026")
cells += [
    md("""
    ### 1. Alcance seguro de la ejecución

    Bronze y Silver reciben `--years 2026`. Gold se recalcula desde Silver completo para que
    los dashboards mantengan 2023–2026. No se usa `--force`, por lo que los archivos válidos
    ya descargados se omiten de forma idempotente.

    **QUÉ EXPLICAR:** el año corriente se descubre en la página oficial; no se codifican meses.
    """),
    code("""
    # Modo seguro: esta celda solo muestra los comandos de la exposición.
    # Cambie a True únicamente cuando quiera ejecutar de verdad dentro del contenedor Spark.
    EJECUTAR = False
    comandos = [
        ('tlc-pipeline catalog --years 2026 --catalog-output '
         '/workspace/data/bronze/_catalog_2026.json'),
        ('tlc-pipeline ingest --years 2026 --catalog-input '
         '/workspace/data/bronze/_catalog_2026.json'),
        'tlc-pipeline silver --years 2026',
        'tlc-pipeline gold',
        'tlc-pipeline models',
        'tlc-pipeline audit-export',
        'tlc-pipeline powerbi',
        'tlc-pipeline verify --powerbi-path /workspace/powerbi',
    ]
    print('\\n'.join(comandos))

    if EJECUTAR:
        import subprocess
        for comando in comandos:
            print(f'\\n>>> {comando}')
            subprocess.run(comando, shell=True, check=True, cwd=ROOT)
    """),
    md("### 2. Evidencia de archivos 2026 ya publicados"),
    code("""
    import re
    from collections import Counter

    # El catálogo conserva lo descubierto; contar por servicio demuestra el alcance real.
    catalog_path = DATA / 'bronze' / '_catalog.json'
    catalog = json.loads(catalog_path.read_text(encoding='utf-8'))
    entries = catalog.get('files', catalog) if isinstance(catalog, dict) else catalog
    current = [
        item for item in entries
        if int(item['year']) == 2026 and item.get('available', True)
    ]
    print('Archivos 2026 disponibles:', len(current))
    print('Por servicio:', dict(Counter(item['service'] for item in current)))
    print('Meses:', sorted({int(item['month']) for item in current}))
    """),
    md("## Comprobaciones"),
    code("""
    # El comando público para la defensa tiene modo Demo por defecto y modo Execute explícito.
    script = ROOT / 'scripts' / 'run_2026_only.ps1'
    assert script.exists()
    text = script.read_text(encoding='utf-8')
    assert '--years 2026' in text and 'tlc-pipeline verify' in text
    print('Automatización 2026 lista:', script)
    """),
    md("""
    ## Siguiente paso

    Ejecute `./scripts/run_2026_only.ps1` para ensayar sin cambios. Durante la exposición use
    `./scripts/run_2026_only.ps1 -Mode Execute`. Después muestre Bronze.
    """),
]
save("00_ingesta_automatica_2026.ipynb", cells)


cells = common_setup("01 — Capa Bronze: datos originales e integridad")
cells += [
    md("""
    ### 1. Inventario y trazabilidad

    Bronze guarda cada Parquet tal como lo publica TLC, más su sidecar de metadatos, SHA-256
    y catálogo. Es una capa inmutable y reejecutable.

    **QUÉ EXPLICAR:** Bronze responde “qué recibí, de dónde, cuándo y con qué huella”.
    """),
    code("""
    from collections import Counter
    import re

    bronze = DATA / 'bronze'
    files = list(bronze.rglob('*_tripdata_*.parquet'))
    years = Counter(int(re.search(r'_(20\\d{2})-', p.name).group(1)) for p in files)
    sidecars = list(bronze.rglob('*.metadata.json'))
    print('Archivos Parquet originales:', len(files))
    print('Archivos por año:', dict(sorted(years.items())))
    print('Sidecars:', len(sidecars))
    print('Tamaño Bronze (GB):', round(sum(p.stat().st_size for p in files) / 1e9, 2))
    """),
    md("### 2. Ejemplo de sidecar sin modificar el dato"),
    code("""
    # Mostrar un sidecar es más claro que abrir un Parquet binario durante la defensa.
    example = json.loads(sidecars[0].read_text(encoding='utf-8'))
    print(json.dumps(example, indent=2, ensure_ascii=False)[:2500])
    """),
    md("## Comprobaciones"),
    code("""
    report = json.loads((EXPORTS / 'verification_report.json').read_text(encoding='utf-8'))
    bronze_check = next(c for c in report['checks'] if c['name'].startswith('bronze_'))
    assert bronze_check['passed'] and bronze_check['details']['historical_files'] == 144
    assert bronze_check['details']['checksums_recomputed'] is True
    print(bronze_check['details'])
    """),
    md("""
    ## Siguiente paso

    Bronze no corrige datos. La capa Silver aplica el esquema canónico, calidad, zonas y
    cuarentena manteniendo reconciliación fila a fila.
    """),
]
save("01_capa_bronze.ipynb", cells)


cells = common_setup("02 — Capa Silver: estandarización, calidad y cuarentena")
cells += [
    md("""
    ### 1. Reconciliación completa

    Silver unifica yellow, green, FHV y FHVHV. Cada registro fuente termina como válido o en
    cuarentena; nunca se pierde silenciosamente.

    **QUÉ EXPLICAR:** la igualdad clave es `fuente = válidas + cuarentena`.
    """),
    code("""
    report = json.loads((EXPORTS / 'verification_report.json').read_text(encoding='utf-8'))
    check = next(c for c in report['checks'] if c['name'] == 'silver_row_reconciliation')
    d = check['details']
    assert d['source_rows'] == d['valid_rows'] + d['quarantine_rows']
    print(json.dumps(d, indent=2))
    print('Diferencia:', d['source_rows'] - d['valid_rows'] - d['quarantine_rows'])
    """),
    md("### 2. Esquema PySpark y muestra explicativa"),
    code("""
    from pyspark.sql import SparkSession

    # Solo se muestran 5 filas; PySpark lee el mismo Silver completo usado por Gold.
    spark = (SparkSession.builder.master('local[*]')
             .appName('TLC-Exposicion-Silver').getOrCreate())
    silver_df = spark.read.option('basePath', str(DATA / 'silver')).parquet(str(DATA / 'silver'))
    print('Columnas canónicas:', len(silver_df.columns))
    silver_df.select('service', 'pickup_datetime', 'pickup_location_id',
                     'total_amount', 'dq_valid').show(5, truncate=False)
    """),
    md("## Comprobaciones"),
    code("""
    # Las 163 entradas reconciliadas prueban que se procesó el corpus publicado, no una muestra.
    assert check['passed'] and d['files_processed'] == 163
    assert d['historical_files_reconciled'] == 144
    print('Silver reconciliado al 100 % de los archivos disponibles.')
    spark.stop()
    """),
    md("""
    ## Siguiente paso

    Gold agrega Silver en granos útiles para decisiones y forma una constelación de hechos
    compartiendo dimensiones conformadas.
    """),
]
save("02_capa_silver.ipynb", cells)


cells = common_setup("03 — Capa Gold: constelación analítica")
cells += [
    md("""
    ### 1. Nueve tablas exigidas y seis salidas auxiliares

    Las nueve tablas base cubren 3 descriptivas, 3 diagnósticas y 3 predictivas. Las seis
    tablas adicionales contienen métricas, perfiles y matriz de confusión.

    **QUÉ EXPLICAR:** es una constelación (galaxy schema), porque varios hechos comparten
    fecha, hora, servicio, zona, pago y segmento.
    """),
    code("""
    import pyarrow.parquet as pq

    gold = DATA / 'gold'
    tables = sorted(path for path in gold.iterdir() if path.is_dir())

    # Leer todos los metadatos da el conteo exacto sin cargar millones de filas en RAM.
    counts = {}
    for table in tables:
        parquet_files = list(table.rglob('*.parquet'))
        counts[table.name] = sum(pq.ParquetFile(path).metadata.num_rows for path in parquet_files)
    for name, rows in counts.items():
        print(f'{name:42s} {rows:>12,}')
    """),
    md("### 2. Clasificación por propósito"),
    code("""
    base = {
        'descriptivo': [n for n in counts if n.startswith('descriptive_')],
        'diagnóstico': [n for n in counts if n.startswith('diagnostic_')],
        'predictivo': [n for n in counts if n in {
            'model_timeseries_daily', 'model_segmentation_zones',
            'model_classification_demand'}],
    }
    print(json.dumps(base, indent=2, ensure_ascii=False))
    assert [len(base[k]) for k in base] == [3, 3, 3]
    """),
    md("## Comprobaciones"),
    code("""
    report = json.loads((EXPORTS / 'verification_report.json').read_text(encoding='utf-8'))
    check = next(c for c in report['checks'] if c['name'] == 'nine_gold_tables')
    assert check['passed'] and check['details']['tables'] == 9
    assert len(counts) >= 15
    print('Gold base:', check['details'])
    """),
    md("""
    ## Siguiente paso

    Las tablas `model_*` alimentan GBT, K-Means y Random Forest; el siguiente notebook
    presenta sus métricas y artefactos persistidos.
    """),
]
save("03_capa_gold.ipynb", cells)


cells = common_setup("04 — Modelos predictivos: series, segmentación y clasificación")
cells += [
    md("""
    ### 1. Evidencia de modelos Spark ML persistidos

    Se entrenan GBT para pronóstico, K-Means para segmentar zonas y Random Forest para
    clasificar alta demanda. Todos usan semilla fija y separación temporal donde corresponde.

    **QUÉ EXPLICAR:** no basta una predicción; se muestran métrica, partición y artefacto.
    """),
    code("""
    model_root = ROOT / 'artifacts' / 'models'
    expected = ['time_series_forecast', 'zone_segmentation', 'high_demand_classifier']
    for name in expected:
        success = list((model_root / name).rglob('_SUCCESS'))
        print(name, 'persistido' if success else 'FALTA')
        assert success
    """),
    md("### 2. Métricas publicadas en Gold"),
    code("""
    import pyarrow.dataset as ds

    metric_tables = ['model_timeseries_metrics', 'model_segmentation_metrics',
                     'model_classification_metrics']
    for name in metric_tables:
        dataset = ds.dataset(DATA / 'gold' / name, format='parquet', partitioning='hive')
        frame = dataset.to_table().to_pandas()
        print(f'\\n{name}')
        print(frame.to_string(index=False))
    """),
    md("## Comprobaciones"),
    code("""
    report = json.loads((EXPORTS / 'verification_report.json').read_text(encoding='utf-8'))
    check = next(c for c in report['checks'] if c['name'] == 'three_predictive_models_and_metrics')
    assert check['passed'] and check['details']['models'] == 3
    print(check['details'])
    """),
    md("""
    ## Siguiente paso

    Muestre cómo cada ejecución y resultado queda registrado en MongoDB y en exportaciones
    CSV/JSON para el dashboard de auditoría.
    """),
]
save("04_modelos_predictivos.ipynb", cells)


cells = common_setup("05 — Flujo de auditoría: MongoDB, JSONL y reconciliación")
cells += [
    md("""
    ### 1. Eventos completos de auditoría

    La auditoría registra ejecuciones, archivos, calidad y modelos en MongoDB, y mantiene
    JSONL local más exportaciones CSV/JSON para recuperación y Power BI.

    **QUÉ EXPLICAR:** auditoría es un flujo transversal, no una cuarta capa de datos.
    """),
    code("""
    from collections import Counter

    events = json.loads((EXPORTS / 'audit_events.json').read_text(encoding='utf-8'))
    print('Eventos exportados:', len(events))
    print('Tipos:', dict(Counter(event['event_type'] for event in events)))
    print('Estados:', dict(Counter(event['status'] for event in events)))
    """),
    md("### 2. Comprobación opcional de MongoDB"),
    code("""
    import os
    from pymongo import MongoClient

    # Si Mongo está disponible, se muestran sus colecciones; el notebook sigue siendo portable.
    uri = os.getenv('MONGO_URI', 'mongodb://tlc:tlc_exam@mongo:27017/tlc_audit?authSource=admin')
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        client.admin.command('ping')
        db = client['tlc_audit']
        print({name: db[name].count_documents({}) for name in db.list_collection_names()})
        client.close()
    except Exception as exc:
        print('MongoDB no disponible; se validan exportaciones:', type(exc).__name__)
    """),
    md("## Comprobaciones"),
    code("""
    report = json.loads((EXPORTS / 'verification_report.json').read_text(encoding='utf-8'))
    check = next(c for c in report['checks'] if c['name'] == 'audit_flow_and_export')
    assert check['passed']
    assert check['details']['events'] == check['details']['exported_events']
    print(check['details'])
    """),
    md("""
    ## Siguiente paso

    El último notebook vincula Gold y auditoría con las diez páginas del modelo semántico
    de Power BI.
    """),
]
save("05_flujo_auditoria.ipynb", cells)


cells = common_setup("06 — Power BI: diez dashboards y modelo semántico")
cells += [
    md("""
    ### 1. Inventario de páginas y visuales

    El PBIP contiene 3 páginas descriptivas, 3 diagnósticas, 3 predictivas y 1 de auditoría.
    Los contratos provienen de Gold y auditoría; las medidas se calculan en el modelo semántico.

    **QUÉ EXPLICAR:** cada página responde una pregunta de decisión, no es solo una colección
    de gráficos.
    """),
    code("""
    pages_root = ROOT / 'powerbi' / 'TLC_BigData.Report' / 'definition' / 'pages'
    order = json.loads((pages_root / 'pages.json').read_text(encoding='utf-8'))['pageOrder']
    inventory = []
    for page_id in order:
        page_dir = pages_root / page_id
        page = json.loads((page_dir / 'page.json').read_text(encoding='utf-8'))
        visuals = list((page_dir / 'visuals').glob('*/visual.json'))
        inventory.append((page['displayName'], len(visuals)))
    for item in inventory:
        print(item)
    assert len(inventory) == 10
    """),
    md("### 2. Hechos, dimensiones y relaciones"),
    code("""
    semantic = ROOT / 'powerbi' / 'TLC_BigData.SemanticModel' / 'definition'
    table_files = list((semantic / 'tables').glob('*.tmdl'))
    facts = [p.stem for p in table_files if p.stem.startswith('Fact_')]
    dims = [p.stem for p in table_files if p.stem.startswith('Dim')]
    relation_text = (semantic / 'relationships.tmdl').read_text(encoding='utf-8')
    relationships = relation_text.count('relationship ')
    print('Hechos:', len(facts), facts)
    print('Dimensiones:', len(dims), dims)
    print('Relaciones:', relationships)
    """),
    md("## Comprobaciones"),
    code("""
    report = json.loads((EXPORTS / 'verification_report.json').read_text(encoding='utf-8'))
    check = next(c for c in report['checks'] if c['name'] == 'powerbi_ten_pages')
    definition_files = [
        p for p in (ROOT / 'powerbi').rglob('*')
        if p.is_file() and p.suffix.lower() in {'.json', '.tmdl', '.pbip', '.pbir', '.pbism'}
    ]
    all_text = '\\n'.join(
        p.read_text(encoding='utf-8', errors='ignore') for p in definition_files
    )
    assert check['passed'] and check['details']['pages'] == 10
    assert 'PLACEHOLDER' not in all_text
    assert len(facts) == 15 and len(dims) == 7
    print('Power BI validado:', check['details']['visuals'])
    """),
    md("""
    ## Siguiente paso

    Abra `powerbi/TLC_BigData.pbip`, actualice y recorra las páginas 01–10. Cierre mostrando
    la página 10 y el reporte de verificación sin fallos.
    """),
]
save("06_power_bi.ipynb", cells)

print(f"Se generaron 7 notebooks en {OUT}")
