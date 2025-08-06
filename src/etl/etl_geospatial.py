import os
import logging
import geopandas as gpd
import requests
import zipfile
import io

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)

# URL real do shapefile deve ser colocada aqui
URL_SHAPE_BAIRROS = "https://example.com/shapefile_bairros_paranagua.zip"

def baixar_descompactar_shapefile(url: str, dir_destino: str):
    logging.info(f"Baixando shapefile: {url}")
    resp = requests.get(url)
    if resp.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            z.extractall(dir_destino)
        logging.info(f"Shapefile extraído em: {dir_destino}")
    else:
        logging.error(f"Falha ao baixar shapefile: status {resp.status_code}")
        raise RuntimeError(f"Falha ao baixar shapefile: status {resp.status_code}")

def main():
    output_dir_raw = os.path.join("data", "raw", "shapefiles")
    output_dir_proc = os.path.join("data", "processed")
    os.makedirs(output_dir_raw, exist_ok=True)
    os.makedirs(output_dir_proc, exist_ok=True)

    baixar_descompactar_shapefile(URL_SHAPE_BAIRROS, output_dir_raw)

    # Localiza o arquivo shapefile extraído (.shp)
    shapefile_path = None
    for file in os.listdir(output_dir_raw):
        if file.endswith(".shp"):
            shapefile_path = os.path.join(output_dir_raw, file)
            break

    if shapefile_path is None:
        logging.error("Arquivo .shp não encontrado após extração.")
        raise FileNotFoundError("Arquivo .shp não encontrado.")

    logging.info(f"Lendo shapefile: {shapefile_path}")
    gdf = gpd.read_file(shapefile_path)

    out_gpkg = os.path.join(output_dir_proc, "bairros_paranagua.gpkg")
    gdf.to_file(out_gpkg, driver="GPKG")
    logging.info(f"Shapefile convertido e salvo como GeoPackage: {out_gpkg}")

if __name__ == "__main__":
    main()
