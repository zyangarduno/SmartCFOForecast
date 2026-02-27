import os
from typing import List

import numpy as np
import pandas as pd

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
REPORTS_DIR = "reports"


# -----------------------------
# Helpers
# -----------------------------
def canon_item(x) -> str:
    s = str(x).strip()
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except Exception:
        pass
    return s


def _find_col(df: pd.DataFrame, candidates: List[str]) -> str:
    normalized = {str(c).strip().lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in normalized:
            return normalized[c.lower()]
    raise KeyError(f"No se encontró ninguna columna candidata: {candidates}")


def dio_bucket(days: float) -> str:
    if pd.isna(days):
        return "Sin dato"
    if days < 180:
        return "A (<180)"
    if days < 400:
        return "B (180-399)"
    if days < 700:
        return "C (400-699)"
    return "D (>=700)"


# -----------------------------
# Load data
# -----------------------------
def load_sales_monthly() -> pd.DataFrame:
    path = os.path.join(PROCESSED_DIR, "ventas_sku_mes.parquet")
    df = pd.read_parquet(path)
    df["item"] = df["item"].map(canon_item)
    df["mes"] = pd.to_datetime(df["mes"]).dt.to_period("M").dt.to_timestamp()
    return df


def load_client_forecast() -> pd.DataFrame:
    path = os.path.join(PROCESSED_DIR, "forecast_2026_ingresos.parquet")
    df = pd.read_parquet(path)
    df["item"] = df["item"].map(canon_item)
    return df


def load_inventory() -> pd.DataFrame:
    inv = pd.read_excel(os.path.join(RAW_DIR, "inventario.xlsx"))
    inv.columns = [str(c).strip() for c in inv.columns]

    item_col = _find_col(inv, ["item", "sku", "codigo", "código"])
    exist_col = _find_col(inv, ["existencia", "existencias", "stock", "inventario"])

    inv = inv.rename(columns={item_col: "item", exist_col: "existencia"})
    inv["item"] = inv["item"].map(canon_item)
    inv["existencia"] = pd.to_numeric(inv["existencia"], errors="coerce").fillna(0)

    if "mes" in {c.lower() for c in inv.columns}:
        real_mes_col = [c for c in inv.columns if c.lower() == "mes"][0]
        inv["mes"] = pd.to_datetime(inv[real_mes_col], errors="coerce").dt.to_period("M").dt.to_timestamp()
        inv = inv.sort_values("mes").groupby("item", as_index=False).tail(1)
    else:
        inv["mes"] = pd.NaT

    return inv[["item", "mes", "existencia"]]


# -----------------------------
# Analysis
# -----------------------------
def improved_forecast_2026(ventas_sku_mes: pd.DataFrame) -> pd.DataFrame:
    # Con solo 12 meses de histórico, aplicamos seasonal-naive simple:
    # cada mes de 2026 = valor del mismo mes de 2025.
    v25 = ventas_sku_mes.copy()
    v25["mes_2026"] = v25["mes"].apply(lambda d: d.replace(year=2026))

    forecast_m = v25[["item", "mes_2026", "piezas", "dinero"]].rename(
        columns={"mes_2026": "mes", "piezas": "unidades_modelo", "dinero": "ingresos_modelo"}
    )

    forecast_y = (
        forecast_m.groupby("item", as_index=False)
        .agg(unidades_2026_modelo=("unidades_modelo", "sum"), ingresos_2026_modelo=("ingresos_modelo", "sum"))
    )
    return forecast_m, forecast_y


def inventory_valuation(
    ventas_sku_mes: pd.DataFrame, inv_last: pd.DataFrame, n_skus: int = 20
) -> pd.DataFrame:
    cost_proxy = (
        ventas_sku_mes.groupby("item", as_index=False)
        .agg(piezas_2025=("piezas", "sum"), dinero_2025=("dinero", "sum"))
    )
    cost_proxy["costo_uni_proxy"] = np.where(
        cost_proxy["piezas_2025"] > 0,
        cost_proxy["dinero_2025"] / cost_proxy["piezas_2025"],
        np.nan,
    )

    top = inv_last.merge(cost_proxy[["item", "costo_uni_proxy"]], on="item", how="left")
    top["inventario_mxn"] = top["existencia"] * top["costo_uni_proxy"]
    top = top.sort_values("inventario_mxn", ascending=False).head(n_skus).copy()
    return top


def classify_dio(top_inv: pd.DataFrame, ventas_sku_mes: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        ventas_sku_mes.groupby("item", as_index=False)
        .agg(unidades_prom_mes=("piezas", "mean"), ingresos_prom_mes=("dinero", "mean"))
    )

    out = top_inv.merge(monthly, on="item", how="left")
    out["consumo_dia"] = out["unidades_prom_mes"] / 30.0
    out["dio_days"] = np.where(out["consumo_dia"] > 0, out["existencia"] / out["consumo_dia"], np.nan)
    out["dio_cluster"] = out["dio_days"].apply(dio_bucket)
    return out


def reorder_strategy(top_class: pd.DataFrame) -> pd.DataFrame:
    out = top_class.copy()
    out["meta_dio"] = 400.0
    out["lead_time_dias"] = 120.0
    out["accion_inmediata"] = np.where(
        out["dio_days"] >= 520,
        "Congelar compras (6 meses)",
        np.where(out["dio_days"] >= 400, "Monitorear y no comprar ahora", "Plan de resurtido inmediato"),
    )
    out["dias_hasta_resurtir"] = np.where(out["dio_days"] > 400, out["dio_days"] - 400, 0)
    return out


def build_markdown_report(
    client_fc: pd.DataFrame,
    model_fc_y: pd.DataFrame,
    top_strategy: pd.DataFrame,
    out_path: str,
) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    cliente_ingresos = client_fc["ingresos_2026_cliente"].sum()
    cliente_sin_precio = client_fc["precio_prom_2025"].isna().mean()
    modelo_ingresos = model_fc_y["ingresos_2026_modelo"].sum()

    by_cluster = (
        top_strategy.groupby("dio_cluster", as_index=False)
        .agg(skus=("item", "nunique"), inventario_mxn=("inventario_mxn", "sum"))
        .sort_values("dio_cluster")
    )

    lines: List[str] = []
    lines.append("# Resumen ejecutivo de forecast e inventario\n")
    lines.append("## 1) Ingresos 2026 proyectados por forecast del cliente")
    lines.append(f"- Ingresos totales: **${cliente_ingresos:,.2f} MXN**")
    lines.append(f"- SKUs sin precio promedio 2025: **{cliente_sin_precio:.2%}**\n")

    lines.append("## 2) Nuevo forecast 2026 (seasonal-naive con histórico 2025)")
    lines.append(f"- Ingresos totales del modelo: **${modelo_ingresos:,.2f} MXN**")
    lines.append("- Criterio: con un solo año de histórico, se replica estacionalidad mensual de 2025 en 2026.\n")

    lines.append("## 3) Top 20 SKUs por monto de inventario")
    lines.append(top_strategy[["item", "existencia", "inventario_mxn"]].head(20).to_markdown(index=False))
    lines.append("")

    lines.append("## 4) Clasificación por días de inventario (DIO)")
    lines.append(by_cluster.to_markdown(index=False))
    lines.append("")

    lines.append("## 5) Estrategia inmediata (lead time 120 días, meta 400 días)")
    lines.append(
        top_strategy[["item", "dio_days", "accion_inmediata", "dias_hasta_resurtir"]]
        .sort_values("dio_days", ascending=False)
        .to_markdown(index=False)
    )
    lines.append("")

    lines.append("## 6) Inconsistencias y tratamiento propuesto")
    lines.append("- Si faltan precios 2025 para SKUs del forecast, usar mediana por familia/categoría o último precio válido.")
    lines.append("- Si inventario no trae mes, asumir snapshot de corte y documentar la fecha de extracción.")
    lines.append("- Si hay SKUs con cero consumo, marcar como inventario inmovilizado y excluir de DIO estándar.")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    ventas_sku_mes = load_sales_monthly()
    client_fc = load_client_forecast()
    inv_last = load_inventory()

    _, model_fc_y = improved_forecast_2026(ventas_sku_mes)
    top20_inv = inventory_valuation(ventas_sku_mes, inv_last, n_skus=20)
    top20_class = classify_dio(top20_inv, ventas_sku_mes)
    top20_strategy = reorder_strategy(top20_class)

    out_csv = os.path.join(PROCESSED_DIR, "top20_inventory_strategy.csv")
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    top20_strategy.to_csv(out_csv, index=False)

    out_md = os.path.join(REPORTS_DIR, "analysis_summary.md")
    build_markdown_report(client_fc, model_fc_y, top20_strategy, out_md)

    print("OK ✅ análisis generado")
    print(f"- {out_csv}")
    print(f"- {out_md}")


if __name__ == "__main__":
    main()
