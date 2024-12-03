import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.geometry import Polygon, mapping
import json

def extract_shop_bounds():
    gdf: GeoDataFrame = gpd.read_file("../geo_data/staging/DubaiMallSimplified.kml", driver="KML")
    gdf = gdf.to_crs(epsg=32633)
    shops = []

    for _, row in gdf.iterrows():
        coord_count = len(row.geometry.exterior.coords)
        area_m2 = row.geometry.area
        boundary = Polygon(row.geometry)
        shops.append({"area": {"name": row["Name"], "num coordinates": coord_count, "area_m2": f"{area_m2:.2f} m2", "boundary": mapping(boundary)}})

    with open("../geo_data/processed/DubaiMallShops.json", "w") as file:
        json.dump(shops, file)

def extract_zone_bounds():
    gdf: GeoDataFrame = gpd.read_file("../geo_data/staging/Zones.kml", driver="KML")
    gdf = gdf.to_crs(epsg=32633)
    zones = []

    for _, row in gdf.iterrows():
        area_m2 = row.geometry.area
        boundary = Polygon(row.geometry)
        zones.append({"zone": {"name": row["Name"], "area_m2": f"{area_m2:.2f} m2", "boundary": mapping(boundary)}})

    with open("../geo_data/processed/DubaiMallZones.json", "w") as file:
        json.dump(zones, file)

extract_shop_bounds()
extract_zone_bounds()