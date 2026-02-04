import zipfile
from pathlib import Path

import requests
from tqdm import tqdm
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

DOWNLOADS_DIR = Path(r"C:\Users\rajasekhar.palleti\Downloads\ne_data")
DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

NE_COUNTRIES_URL = "https://naturalearth.s3.amazonaws.com/50m_cultural/ne_50m_admin_0_countries.zip"

_INDIA_GEOM = None  # cache


def _download_zip(url: str, zip_path: Path, extract_dir: Path) -> None:
    extract_dir.mkdir(parents=True, exist_ok=True)
    if any(extract_dir.glob("*.shp")):
        return

    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length") or 0)
        with open(zip_path, "wb") as f, tqdm(
            total=total, unit="B", unit_scale=True, desc=f"Downloading {zip_path.name}"
        ) as pbar:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_dir)


def _load_india_geom():
    """Load India polygon geometry using Shapely 2.x union_all()."""
    global _INDIA_GEOM
    if _INDIA_GEOM is not None:
        return _INDIA_GEOM

    countries_zip = DOWNLOADS_DIR / "ne_50m_admin_0_countries.zip"
    countries_dir = DOWNLOADS_DIR / "ne_50m_admin_0_countries"
    _download_zip(NE_COUNTRIES_URL, countries_zip, countries_dir)

    shp_files = list(countries_dir.glob("*.shp"))
    if not shp_files:
        raise RuntimeError("Admin 0 countries shapefile missing after download.")

    gdf = gpd.read_file(shp_files[0])
    gdf = gdf.set_crs("EPSG:4326") if gdf.crs is None else gdf.to_crs("EPSG:4326")

    for col in ("ADM0_A3", "ISO_A3", "GU_A3", "WB_A3", "SOVEREIGNT", "ADMIN", "NAME"):
        if col in gdf.columns:
            if col in ("ADM0_A3", "ISO_A3", "GU_A3", "WB_A3"):
                tmp = gdf[gdf[col].str.upper() == "IND"]
            else:
                tmp = gdf[gdf[col].str.contains("India", case=False, na=False)]
            if not tmp.empty:
                _INDIA_GEOM = tmp.union_all()
                return _INDIA_GEOM

    raise RuntimeError("India polygon not found in shapefile.")


def tag_india_check_file(input_excel: str | Path) -> Path:
    input_path = Path(input_excel)
    output_path = input_path.with_name(input_path.stem + "_is_outside_india.xlsx")

    print("📂 Reading file:", input_path)
    df = pd.read_excel(input_path)

    if "latitude" not in df.columns or "longitude" not in df.columns:
        raise KeyError("Input file must contain 'latitude' and 'longitude' columns")

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    india_geom = _load_india_geom()

    print("🗺️ Creating geometry points...")
    geometries = [
        Point(lon, lat) if pd.notna(lat) and pd.notna(lon) else None
        for lat, lon in zip(df["latitude"], df["longitude"])
    ]
    gdf = gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:4326")

    df["is_outside_india"] = "INVALID"

    valid_mask = gdf["geometry"].notna()

    inside_mask = gdf.loc[valid_mask, "geometry"].within(india_geom) | \
                  gdf.loc[valid_mask, "geometry"].touches(india_geom)

    df.loc[valid_mask & inside_mask, "is_outside_india"] = "NO"
    df.loc[valid_mask & ~inside_mask, "is_outside_india"] = "YES"

    print("💾 Writing output to:", output_path)
    df.to_excel(output_path, index=False)
    print("✅ Done.")
    return output_path


if __name__ == "__main__":
    # Example usage:
    tag_india_check_file(r"C:\Users\rajasekhar.palleti\Downloads\ASSETDETAILS mdlz.xlsx")
    # pass