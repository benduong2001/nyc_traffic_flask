import pandas as pd
import numpy as np
from scipy.stats import zscore
from shapely.geometry import Point
import pickle
import os
import sys
sys.path.append(".")
import build_configs

class Input_Reader:
    def __init__(self, args):
        self.args = args;
    def set_form(self, form):
        self.form = form
    def get_street_segments_near_point(
        self,
        temp_point,
        temp_buffer_radius=500,
        ):
        import geopandas as gpd
        from src.features.data_clean import Temp_Data_Preparation_Builder
        self.tdpb = Temp_Data_Preparation_Builder(self.args)
        #temp_point = Point(longitude, latitude)
        path_file_street_segment_shapefile = self.args["path_file_street_segment_shapefile"]
        temp_point_gdf = gpd.GeoDataFrame({'geometry': [temp_point]}, crs=4326)
        # since the original street segments AND the mappluto landuse shapefiles were both done in CRS 2263, both the point and street segment buffer need to be 2263
        temp_point_gdf = self.tdpb.project_gdf(temp_point_gdf, 2263)
        temp_point_gdf["geometry"] = temp_point_gdf["geometry"].buffer(temp_buffer_radius)
        street_segments_near_point_gdf = gpd.read_file(path_file_street_segment_shapefile,mask=temp_point_gdf['geometry'])
        street_segments_near_point_gdf = self.tdpb.project_gdf(street_segments_near_point_gdf, 2263)
        return street_segments_near_point_gdf
    
    def get_selected_street_segment_geoenriched_gdf(
        self,
        selected_street_segment_gdf,
        ):
        import geopandas as gpd
        from src.features.data_clean import Temp_Data_Preparation_Builder
        self.tdpb = Temp_Data_Preparation_Builder(self.args)
        
        path_file_landuse_shapefile = self.args["path_file_landuse_shapefile"]
        print(selected_street_segment_gdf.crs)

        selected_street_segment_buffer_gdf = self.tdpb.street_segment_buffers(selected_street_segment_gdf, 500)
        landuse_gdf = self.tdpb.load_landuse_gdf(path_file_landuse_shapefile,street_segment_buffers_gdf=selected_street_segment_buffer_gdf)
        #landuse_projected_gdf = self.tdpb.project_gdf(landuse_gdf, 2263) #self.tdpb.crs)
        landuse_cleaned_gdf = self.tdpb.landuse_cleaned(landuse_gdf) #landuse_projected_gdf)
        print(selected_street_segment_gdf.crs)
        print(landuse_cleaned_gdf.crs)
        selected_street_segment_buffer_landuses_gdf = self.tdpb.street_segment_buffer_landuses(landuse_cleaned_gdf,selected_street_segment_buffer_gdf,)
        print(list(selected_street_segment_buffer_landuses_gdf.shape))
        print(list(selected_street_segment_buffer_landuses_gdf.columns),)
        selected_street_segment_buffer_landuses_pivoted_df = self.tdpb.street_segment_buffer_landuses_pivoted(selected_street_segment_buffer_landuses_gdf)
        full_pivoted_df = pd.DataFrame()
        for column_name in self.tdpb.landuse_values:
            full_pivoted_df["LandUse_" + column_name] = 0
        for column_name in list(selected_street_segment_buffer_landuses_pivoted_df.columns):
            full_pivoted_df[column_name] = selected_street_segment_buffer_landuses_pivoted_df[column_name]        
        print(list(selected_street_segment_buffer_landuses_pivoted_df.columns))
        selected_street_segment_geoenriched_gdf = self.tdpb.street_segment_geoenriched(selected_street_segment_gdf, full_pivoted_df)
        return selected_street_segment_geoenriched_gdf
    def get_version(self, form):
        return 2
    def build_traffic_inputs(self):
        version = self.get_version(self.form)
        traffic_inputs = self.form
        if version == 0:
        # 0 just coords
            longitude = traffic_inputs["longitude"]
            latitude = traffic_inputs["latitude"]
            selected_point = Point(longitude, latitude)
            street_segments_near_point_gdf = self.get_street_segments_near_point(selected_point, 50)
            print(street_segments_near_point_gdf.crs)
            if (street_segments_near_point_gdf.shape[0]) == 0:
                return ""
            else:
                # get closest street segment in the small search space
                polyline_index = street_segments_near_point_gdf.distance(selected_point).sort_values().index[0]
                selected_street_segment_id = street_segments_near_point_gdf.loc[polyline_index, ["Segment_ID"]].values[0]
                # getting by id
                selected_street_segment_gdf = street_segments_near_point_gdf[street_segments_near_point_gdf["Segment_ID"]==selected_street_segment_id]
            selected_street_segment_geoenriched_gdf = self.get_selected_street_segment_geoenriched_gdf(selected_street_segment_gdf)
            traffic_inputs = self.form
            print(selected_street_segment_geoenriched_gdf.shape)
            traffic_inputs.update(selected_street_segment_geoenriched_gdf.loc[0].to_dict())
        # 1 coords to segment id
        elif version == 1:
            longitude = traffic_inputs["streetSegmentCentroidLongitude"]
            latitude = traffic_inputs["streetSegmentCentroidLatitude"]
            selected_street_segment_id = traffic_inputs["Segment_ID"]
            street_segment_centroid_point = Point(longitude, latitude)
            street_segments_near_point_gdf = self.get_street_segments_near_point(street_segment_centroid_point, 5)
            selected_street_segment_gdf = street_segments_near_point_gdf[street_segments_near_point_gdf["Segment_ID"]==selected_street_segment_id]
            selected_street_segment_geoenriched_gdf = self.get_selected_street_segment_geoenriched_gdf(selected_street_segment_gdf)
            traffic_inputs.update(selected_street_segment_geoenriched_gdf.loc[0].to_dict())
        # 2
        else:
            pass
        print(traffic_inputs)
        traffic_inputs.pop("Segment_ID")
        traffic_inputs.pop("streetSegmentCentroidLongitude")
        traffic_inputs.pop("streetSegmentCentroidLatitude")
        traffic_inputs.pop("longitude")
        traffic_inputs.pop("latitude")


        return traffic_inputs
    
def make_prediction(form):
    path_folder = "./"
    print(os.listdir(path_folder))
    args = build_configs.main()
    input_reader_object = Input_Reader(args)
    input_reader_object.set_form(form)
    traffic_inputs = input_reader_object.build_traffic_inputs()
    with open(args["path_file_model_pipeline_pickle"], 'rb') as f:
        model_pipeline = pickle.load(f)
        f.close()

    result = model_pipeline.predict(pd.DataFrame(traffic_inputs, index=[0])) 

    return result

 
