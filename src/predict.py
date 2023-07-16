import geopandas as gpd
import pandas as pd
import numpy as np
from scipy.stats import zscore
from shapely.geometry import Point
import pickle
import os
import sys
sys.path.append(".")
import build_configs
from src.features.data_clean import Temp_Data_Preparation_Builder

class Input_Reader:
    def __init__(self, args):
        self.args = args;
        self.tdpb = Temp_Data_Preparation_Builder(self.args)
    def set_form(self, form):
        self.form = form
    def get_street_segments_near_point(
        self,
        temp_point,
        temp_buffer_radius=500,
        ):
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
        traffic_inputs["LandUse_NULL"] = traffic_inputs["LandUse_00"]
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
        return traffic_inputs
    
def make_prediction(form):
    args = build_configs.main("../")
    input_reader_object = Input_Reader(args)
    input_reader_object.set_form(form)
    traffic_inputs = input_reader_object.build_traffic_inputs()
    with open(args["path_file_model_pipeline_pickle"], 'rb') as f:
        model_pipeline = pickle.load(f)
        f.close()

    result = model_pipeline.predict(pd.DataFrame(traffic_inputs, index=[0])) 

    return result
    
    
        
        
##class Prediction:
##    pass
##
##def make_prediction(hour, is_weekend,x_coord, y_coord):
##    print("DTC")
##    print(x_coord, y_coord)
##    path_folder = ""#os.getcwd()
##    #path_folder = "/content/drive/MyDrive/dsprojects/dsproject_nyc_traffic" # parent of current src folder
##
##    path_folder_landuse_geodata = path_folder + "../data/raw/raw_orig_geodata/raw_orig_geodata_landuse/MapPLUTO.shp"
##    path_folder_street_segment_geodata = path_folder + "../data/raw/raw_orig_geodata/raw_orig_geodata_street_segment/lion.shp"
##
##    orig_point = Point(x_coord, y_coord)
##    temp_point_gdf = gpd.GeoDataFrame({'geometry': [orig_point]}, crs=4326)
##    new_point = temp_point_gdf.to_crs({'init':'epsg:2263'})
##    new_x_coord = new_point.iloc[0]["geometry"].centroid.x  
##    new_y_coord = new_point.iloc[0]["geometry"].centroid.y
##    point = Point(new_x_coord,new_y_coord)
##
##    input_point_buffer = new_point.copy()
##    BUFFER_RADIUS = 500
##    input_point_buffer['geometry'] = input_point_buffer['geometry'].buffer(BUFFER_RADIUS)
##
##    # street_segment coords, polyline, or id
##
##    street_segment_geodata_column_names = ['Segment_ID', 'geometry']
##    street_segment_geodata_orig = gpd.read_file(path_folder_street_segment_geodata,
##                                                include_fields = street_segment_geodata_column_names,
##                                                mask=input_point_buffer['geometry']
##                                                )
##    if len(street_segment_geodata_orig)==0:
##        return "ERROR OUT OF BOUNDS"
##    street_segment_geodata = street_segment_geodata_orig
##    print("Read Street Segments Data")
##
##
##    street_segment_orig_categorical_column_names = ['Snow_Prior', 'RW_TYPE', 'Status', 'CurveFlag', 'FeatureTyp', 'SegmentTyp', 'NonPed',  'TrafDir','LocStatus','BikeLane','ROW_Type']
##    street_segment_orig_numerical_column_names = ['StreetWidt', 'SegCount', 'Number_Tra', 'Number_Par', 'Number_Tot', 'SHAPE_Leng'] 
##
##    NULL_PLACEHOLDER = "NULL"
##    street_segment_geodata[street_segment_orig_categorical_column_names] = street_segment_geodata[street_segment_orig_categorical_column_names].astype(str).fillna(NULL_PLACEHOLDER)
##
##    non_streets_filter = street_segment_geodata_orig["RW_TYPE"].isin(["1","2","3","4","9","11","13",NULL_PLACEHOLDER])
##    non_streets_filter &= street_segment_geodata_orig["FeatureTyp"].isin(["F", "A", "W", "9", "8","2","5"])==False
##    non_streets_filter &= street_segment_geodata_orig["TrafDir"].isin(["P",NULL_PLACEHOLDER])==False # not Pedestrian only nor non street feature
##    non_streets_filter &= street_segment_geodata_orig["Status"].isin(["2", NULL_PLACEHOLDER]) # constructed
##    #non_streets_filter &= street_segment_geodata_orig["ROW_Type"].isin([NULL_PLACEHOLDER]) # not subway
##
##    street_segment_geodata = street_segment_geodata_orig[non_streets_filter]
##
##    # trimming down the uncertain relevance with just the bad columns (accoridng to this subset)
##    # null is tolereable for categ, not for numer
##    # very skew is tolerable for numer, not for categ
##
##    street_segment_curr_numerical_column_names = ['Number_Tra'] 
##
##
##    street_segment_geodata[street_segment_curr_numerical_column_names] = street_segment_geodata[street_segment_curr_numerical_column_names].astype(float)
##
##    # getting by coords
##    # X, Y
##    #'''
##    # orig_point = Point(-73.97273825353946, 40.60055508842727) for 25210
##    # reproject
##
##    #orig_point = Point(x_coord, y_coord)
##    #temp_point_gdf = gpd.GeoDataFrame({'geometry': [orig_point]}, crs=4326)
##    #new_point = temp_point_gdf.to_crs({'init':'epsg:2263'})
##    new_x_coord = new_point.iloc[0]["geometry"].centroid.x  
##    new_y_coord = new_point.iloc[0]["geometry"].centroid.y
##    point = Point(new_x_coord,new_y_coord)
##    
##    polyline_index = street_segment_geodata.distance(point).sort_values().index[0]
##    selected_street_segment_id = street_segment_geodata.loc[polyline_index, ["Segment_ID"]].values[0]#'''
##    # getting by id
##    street_segment_geodata_selection = street_segment_geodata[street_segment_geodata["Segment_ID"]==selected_street_segment_id]
##    print("\t Found Closest Polyline")
##    print("\t Street is:", street_segment_geodata_selection["Street"].iloc[0])
##
##
##    hour_input = int(hour)
##
##    street_segment_geodata_selection = street_segment_geodata_selection.to_crs({'init':'epsg:2263'})
##    BUFFER_RADIUS = 150
##    street_segment_geodata_selection['geometry'] = street_segment_geodata_selection['geometry'].buffer(BUFFER_RADIUS)
##    print("\t Buffered Polyline")
##
##    landuse_geodata_column_names = ['geometry'] + ['LandUse']
##    landuse_geodata_orig = gpd.read_file(path_folder_landuse_geodata, 
##                                         include_fields=landuse_geodata_column_names,
##                                         mask=street_segment_geodata_selection['geometry']
##                                         )
##
##    # trimming down the uncertain relevance with just the bad columns (accoridng to this subset)
##    # null is tolereable for categ, not for numer
##    # very skew is tolerable for numer, not for categ
##
##    landuse_curr_categorical_column_names = ['LandUse']#+['BldgClass']
##    # if a categ is too skewed, info is useless. esp if the top categ is null.
##
##    landuse_geodata = landuse_geodata_orig
##
##    NULL_PLACEHOLDER = "NULL"
##    landuse_geodata[landuse_curr_categorical_column_names] = landuse_geodata[landuse_curr_categorical_column_names].fillna(NULL_PLACEHOLDER)
##    landuse_geodata[landuse_curr_categorical_column_names] = landuse_geodata[landuse_curr_categorical_column_names].astype(str)
##
##
##
##    landuse_street_segment_geodata_clip = gpd.clip(landuse_geodata, street_segment_geodata_selection['geometry'])
##    print("\t Clipped Buffer")
##    landuse_street_segment_geodata = gpd.sjoin(landuse_street_segment_geodata_clip, street_segment_geodata_selection)
##    print("\t Spatial Join Buffer")
##
##
##
##
##
##    landuse_street_segment_geodata_concat_df_list = []
##    landuse_street_segment_geodata_concat_df_list += [landuse_street_segment_geodata[["Segment_ID"]]]
##    landuse_street_segment_geodata_concat_df_list += [landuse_street_segment_geodata[street_segment_curr_numerical_column_names]]
##
##    ohe_dict = dict()
##    for preohe_column_name in landuse_curr_categorical_column_names:
##        temp_ohe_df = pd.get_dummies(landuse_street_segment_geodata[preohe_column_name], prefix=preohe_column_name)
##        ohe_dict[preohe_column_name] = list(temp_ohe_df.columns)
##        landuse_street_segment_geodata_concat_df_list += [temp_ohe_df]
##
##    landuse_street_segment_geodata_concat_df = pd.concat(landuse_street_segment_geodata_concat_df_list, axis=1)
##
##    landuse_street_segment_data_agg_dict = dict()
##    for column_name in (street_segment_curr_numerical_column_names): landuse_street_segment_data_agg_dict[column_name] = np.mean;
##    ohe_dict_values = (sum(list(ohe_dict.values()),[]))
##    for column_name in (ohe_dict_values): landuse_street_segment_data_agg_dict[column_name] = np.sum;
##    landuse_street_segment_data = (landuse_street_segment_geodata_concat_df.groupby(["Segment_ID"], as_index=False).agg(landuse_street_segment_data_agg_dict))
##    
##    for ohe_dict_key in list(ohe_dict.keys()): 
##        ohe_dict_key_values = ohe_dict[ohe_dict_key]
##        temp_anti_zero_div_adjustor = (landuse_street_segment_data[ohe_dict_key_values].sum(axis=1)==0).astype(int)
##        landuse_street_segment_data.loc[:,ohe_dict_key_values] = landuse_street_segment_data.loc[:,ohe_dict_key_values].div(landuse_street_segment_data[ohe_dict_key_values].sum(axis=1)+temp_anti_zero_div_adjustor, axis=0)
##
##
##    # This pads the input vector in case some landuse columns never appear
##    observed_ohe_dict_keys = sum(list(ohe_dict.values()),[])
##    saved_ohe_dict_keys  = ['LandUse_01', 'LandUse_02', 'LandUse_03', 'LandUse_04', 'LandUse_05', 'LandUse_06',
##             'LandUse_07', 'LandUse_08', 'LandUse_09', 'LandUse_10', 'LandUse_11', 'LandUse_NULL']
##    
##    unobserved_ohe_dict_keys = list(set(saved_ohe_dict_keys).difference(set(observed_ohe_dict_keys)))
##    unseen_ohe_columns = pd.Series(index=unobserved_ohe_dict_keys,data=np.zeros(len(unobserved_ohe_dict_keys))).to_frame().T
##    #cols += ['StreetWidt'] # 87
##    #cols += ['is_weekend'] # 90
##
##    hours_columns = pd.DataFrame(columns=['Hour'], data=[hour_input])
##
##    
##    input_vector = pd.concat([landuse_street_segment_data,unseen_ohe_columns,hours_columns],axis=1)
##
##    input_vector_column_names = []
##    input_vector_column_names += ['LandUse_01', 'LandUse_02', 'LandUse_03', 'LandUse_04', 'LandUse_05', 'LandUse_06',
##             'LandUse_07', 'LandUse_08', 'LandUse_09', 'LandUse_10', 'LandUse_11', 'LandUse_NULL']
##    input_vector_column_names += ['Hour']
##    input_vector_column_names += ['Number_Tra']
##    
##    
##
##    input_vector = input_vector[input_vector_column_names]
##    print(list(input_vector.columns))
##    print(input_vector.shape)
##    print(input_vector)
##
##    with open(path_folder +'../data/final/dtc.pkl', 'rb') as f:
##        model_dtc = pickle.load(f)
##        f.close()
##
##    result = model_dtc.predict(input_vector) 
##
##    return result
##
