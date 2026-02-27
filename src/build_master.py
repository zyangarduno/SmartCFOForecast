import os, glob
import pandas as pd
import numpy as np

RAW_DIR = "data/raw"
OUT_DIR = "data/processed"
os.makedirs(OUT_DIR, exist_ok=True)

def canon_item(x):
    s = str(x).strip()
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except:
        pass
    return s

def canon_mes(x):
    return pd.to_datetime(x, errors="coerce").to_period("M").to_timestamp()

def load_sales():
    paths = sorted(glob.glob(os.path.join(RAW_DIR, "ventas_2025_*.csv")))
    df = pd.concat([pd.read_csv(p).assign(source_file=os.path.basename(p)) for p in paths], ignore_index=True)

    # Normaliza fecha/mes y item
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df["mes"] = df["fecha"].dt.to_period("M").dt.to_timestamp()
    df["item"] = df["item"].apply(canon_item)

    # Agrega por SKU-mes
    ventas_sku_mes = (df.groupby(["item","mes"], as_index=False)
                        .agg(piezas=("piezas","sum"),
                             dinero=("dinero","sum"),
                             precio_prom_2025=("precio","mean")))

    return ventas_sku_mes, df

def load_forecast():
    fc = pd.read_excel(os.path.join(RAW_DIR, "forecast_2026.xlsx"))
    fc.columns = fc.columns.astype(str).str.strip()

    # Detecta columnas de meses (vienen como datetime string)
    month_cols = [c for c in fc.columns if str(c).startswith("2025-") or str(c).startswith("2026-")]
    if len(month_cols) == 0:
        # fallback: cualquier columna que sea datetime-like
        month_cols = [c for c in fc.columns if "00:00:00" in str(c)]

    fc["SKU"] = fc["SKU"].apply(canon_item)

    fc_long = fc.melt(id_vars=["SKU"], value_vars=month_cols, var_name="mes", value_name="unidades")
    fc_long["mes"] = pd.to_datetime(fc_long["mes"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    fc_long["unidades"] = (fc_long["unidades"].astype(str).str.replace(",","", regex=False))
    fc_long["unidades"] = pd.to_numeric(fc_long["unidades"], errors="coerce").fillna(0)

    # Si el cliente dejó 2025 pero es forecast 2026, forzamos año 2026 conservando mes
    fc_long["mes"] = fc_long["mes"].apply(lambda d: d.replace(year=2026) if pd.notna(d) else d)

    forecast_anual = (fc_long.groupby(["SKU"], as_index=False)
                        .agg(unidades_2026=("unidades","sum")))

    forecast_anual.rename(columns={"SKU":"item"}, inplace=True)
    forecast_anual["item"] = forecast_anual["item"].apply(canon_item)

    return fc_long, forecast_anual

def load_inventory():
    inv = pd.read_excel(os.path.join(RAW_DIR, "inventario.xlsx"))
    # Aquí asumo que YA lo tienes en formato largo/pivot como antes.
    # Si tu archivo sigue “raro”, lo convertimos después.
    return inv

def main():
    ventas_sku_mes, df_raw = load_sales()
    fc_long, forecast_anual = load_forecast()

    # Precio promedio 2025 por SKU (para el join con forecast anual)
    precio_sku_2025 = (ventas_sku_mes.groupby("item", as_index=False)
                        .agg(precio_prom_2025=("precio_prom_2025","mean")))

    forecast_join = forecast_anual.merge(precio_sku_2025, on="item", how="left")
    forecast_join["ingresos_2026_cliente"] = forecast_join["unidades_2026"] * forecast_join["precio_prom_2025"]

    # Guardar outputs
    ventas_sku_mes.to_parquet(os.path.join(OUT_DIR, "ventas_sku_mes.parquet"), index=False)
    forecast_anual.to_parquet(os.path.join(OUT_DIR, "forecast_2026_anual.parquet"), index=False)
    forecast_join.to_parquet(os.path.join(OUT_DIR, "forecast_2026_ingresos.parquet"), index=False)

    print("OK ✅ archivos creados en data/processed/")
    print("Ingresos 2026 cliente:", f"{forecast_join['ingresos_2026_cliente'].sum():,.2f}")
    print("SKUs sin precio:", f"{forecast_join['precio_prom_2025'].isna().mean():.2%}")

if __name__ == "__main__":
    main()
