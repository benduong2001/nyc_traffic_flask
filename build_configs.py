import os
import json
def main(path_folder=None):
    if path_folder is None:
        path_folder = os.getcwd()
    configs = {}
    args = configs
    args["path_folder"] = path_folder
    args["path_folder_street_segment_shapefile"] = os.path.join(args["path_folder"],"data","raw","raw_orig_geodata","raw_orig_geodata_street_segment") # TODO: can be taken uplevel, thus avoiding needing to update args with self.args = args;
    args["path_folder_landuse_shapefile"] = os.path.join(args["path_folder"],"data","raw","raw_orig_geodata","landuse"); # TODO: can be taken uplevel, thus avoiding needing to update args with self.args = args;
    args["path_file_traffic_volume_precompressed"] = os.path.join(args["path_folder"],"data","raw","raw_orig_data","traffic_volume.csv") # TODO: can be taken uplevel, thus avoiding needing to update args with self.args = args;
    args["path_file_traffic_volume"] = os.path.join(args["path_folder"],"data","raw","raw_orig_data","traffic_volume.parquet"); # TODO: can be taken uplevel, thus avoiding needing to update args with self.args = args;
    args["path_file_street_segment_shapefile"] = os.path.join(args["path_folder_street_segment_shapefile"], "lion.shp") # TODO: can be taken uplevel,
    args["path_file_landuse_shapefile"] = os.path.join(args["path_folder_landuse_shapefile"], "MapPLUTO.shp") # TODO: can be taken uplevel,
    args["path_file_landuse_geojson"] = os.path.join(path_folder,"display","static","landuse.geojson") # TODO: can be taken uplevel,
    args["path_file_street_segment_geojson"] = os.path.join(path_folder,"display","static","street_segment.geojson") # TODO: can be taken uplevel,
    args["path_file_street_segment_landuse_traffic_volume"] = os.path.join(args["path_folder"],"data","temp","street_segment_landuse_traffic_volume.csv") # TODO: can be taken uplevel,
    args["path_file_model_pipeline_pickle"] = os.path.join(args["path_folder"],"data","final","multinomreg.pkl")
    
    file_name_configs = "configs.json"
    path_file_configs = os.path.join(path_folder, file_name_configs)
    with open(path_file_configs, "w") as f:
        json.dump(configs, f)
        f.close()
    return configs



