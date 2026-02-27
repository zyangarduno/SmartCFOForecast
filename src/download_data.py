import os
import re
import gdown
from src.config import RAW_DIR, FORECAST_SHEET_URL, INVENTARIO_SHEET_URL, SALES_FILES

def extract_file_id(s: str) -> str:
    s = str(s).strip()
    if re.fullmatch(r"[A-Za-z0-9_-]{10,}", s) and "/" not in s:
        return s
    m = re.search(r"/file/d/([A-Za-z0-9_-]+)", s)
    if m:
        return m.group(1)
    m = re.search(r"[?&]id=([A-Za-z0-9_-]+)", s)
    if m:
        return m.group(1)
    raise ValueError(f"No pude extraer file_id de: {s}")

def gsheet_export(url: str, fmt: str = "xlsx") -> str:
    m = re.search(r"/spreadsheets/d/([A-Za-z0-9_-]+)", url)
    if not m:
        raise ValueError(f"No parece Google Sheets URL: {url}")
    sid = m.group(1)
    return f"https://docs.google.com/spreadsheets/d/{sid}/export?format={fmt}"

def download_drive_file(file_id: str, out_path: str) -> None:
    url = f"https://drive.google.com/uc?id={extract_file_id(file_id)}"
    gdown.download(url, out_path, quiet=False)

def download_sheet(sheet_url: str, out_path: str) -> None:
    url = gsheet_export(sheet_url, fmt="xlsx")
    gdown.download(url, out_path, quiet=False)

def main():
    os.makedirs(RAW_DIR, exist_ok=True)

    # Sheets
    download_sheet(FORECAST_SHEET_URL, os.path.join(RAW_DIR, "forecast_2026.xlsx"))
    download_sheet(INVENTARIO_SHEET_URL, os.path.join(RAW_DIR, "inventario.xlsx"))

    # Sales CSVs
    for fname, fid in SALES_FILES.items():
        download_drive_file(fid, os.path.join(RAW_DIR, fname))

if __name__ == "__main__":
    main()
