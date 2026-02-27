# SmartCFOForecast

Un cliente del ramo de autopartes tiene un problema de inventario, el cual se estima en $970 MM MXN, siendo que sus ventas anuales son de $1,250 MM MXN.

## Objetivo del caso
1. Calcular el monto total de ingresos previstos 2026 con un join entre forecast (unidades) y ventas 2025 (precio SKU).
2. Construir un forecast 2026 alternativo con base en datos históricos 2025.
3. Seleccionar al menos 20 SKUs y calcular monto de inventario.
4. Clasificar SKUs por días de inventario (DIO) con criterios explícitos.
5. Definir estrategia de resurtido para 2026 considerando lead time de 120 días y meta de 400 días de inventario.
6. Señalar inconsistencias de información y cómo resolverlas.

## Estructura
- `src/download_data.py`: descarga archivos fuente (Google Drive / Google Sheets).
- `src/build_master.py`: integra ventas y forecast base, generando parquet en `data/processed/`.
- `src/analyze_forecast_inventory.py`: ejecuta el análisis de negocio completo y produce reporte final.
- `reports/analysis_summary.md`: salida de hallazgos ejecutivos.

## Setup
```bash
python -m pip install -r requirements.txt
```

## Ejecución end-to-end
```bash
python -m src.download_data
python -m src.build_master
python -m src.analyze_forecast_inventory
```

## Outputs esperados
- `data/processed/ventas_sku_mes.parquet`
- `data/processed/forecast_2026_anual.parquet`
- `data/processed/forecast_2026_ingresos.parquet`
- `data/processed/top20_inventory_strategy.csv`
- `reports/analysis_summary.md`

## Criterios analíticos implementados
- Forecast cliente: `unidades_2026 * precio_prom_2025`.
- Forecast alternativo: seasonal-naive (replica mes 2025 en 2026), adecuado cuando solo hay 12 meses históricos.
- Valor inventario: `existencia * costo_uni_proxy` (proxy basado en precio promedio 2025 por SKU).
- DIO: `existencia / (unidades_prom_mes / 30)`.
- Segmentación DIO:
  - Grupo A: `<180` días
  - Grupo B: `180-399` días
  - Grupo C: `400-699` días
  - Grupo D: `>=700` días
- Política sugerida:
  - `DIO >= 520`: congelar compras 6 meses
  - `400 <= DIO < 520`: monitorear, sin compra inmediata
  - `DIO < 400`: plan de resurtido inmediato

## Nota
Si el archivo de inventario trae un layout distinto al esperado, se deben mapear columnas mínimas: `item/SKU` y `existencia`.
