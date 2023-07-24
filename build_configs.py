import os
import json
def main(path_folder=None):
    if path_folder is None:
        path_folder = os.getcwd()
    args = {
    "path_folder": path_folder,
    "path_folder_street_segment_shapefile": os.path.join(path_folder,"data","raw","raw_orig_geodata","raw_orig_geodata_street_segment") ,
    "path_folder_landuse_shapefile": os.path.join(path_folder,"data","raw","raw_orig_geodata","landuse")}
    args.update({
    "path_file_traffic_volume_precompressed": os.path.join(path_folder,"data","raw","raw_orig_data","traffic_volume.csv") ,
    "path_file_traffic_volume": os.path.join(path_folder,"data","raw","raw_orig_data","traffic_volume.parquet"),
    "path_file_street_segment_shapefile": os.path.join(args["path_folder_street_segment_shapefile"], "lion.shp") ,
    "path_file_landuse_shapefile": os.path.join(args["path_folder_landuse_shapefile"], "MapPLUTO.shp") ,
    "path_file_landuse_geojson": os.path.join(path_folder, "static","landuse.geojson") ,
    "path_file_street_segment_geojson": os.path.join(path_folder,"static","street_segment.geojson") ,
    "path_file_street_segment_landuse_traffic_volume": os.path.join(path_folder,"data","temp","street_segment_landuse_traffic_volume.csv") ,
    "path_file_model_pipeline_pickle": os.path.join(path_folder,"data","final","multinomreg.pkl"),
    "path_file_dbt_profile_yaml": os.path.join(path_folder, "src", "features", "dbtnyc","profiles.yml"),
    })

    configs = args

    file_name_configs = "configs.json"
    path_file_configs = os.path.join(path_folder, file_name_configs)
    try:
        with open(path_file_configs, "w") as f:
            json.dump(configs, f)
            f.close()
    except:
        pass
    return configs



