# -*- coding: utf-8 -*-
"""predict.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1yFp7zHXKTddLSvwv1UIvVSoJ6fFnVMqH
"""

#!pip install pandas fiona shapely pyproj rtree
#!pip install geopandas
#!apt-get install -y libspatialindex-dev
#!pip install rtree
#import warnings
#warnings.simplefilter(action='ignore', category=FutureWarning)
import geopandas as gpd
import pandas as pd
import numpy as np
from scipy.stats import zscore
from shapely.geometry import Point
import pickle
import os

#print(gpd.__version__)
#print(pd.__version__)
#print(scipy.__version__)
#print(np.__version__)
#print(rtree.__version__)
#print(fiona.__version__)
#print(fiona.__version__)

# Load the Drive helper and mount
#from google.colab import drive
#mounted_path_folder = '/content/drive'
#drive.mount(mounted_path_folder, force_remount=True)

def make_prediction(hour, is_weekend,x_coord, y_coord):
    print(x_coord, y_coord)
    path_folder = ""#os.getcwd()
    #path_folder = "/content/drive/MyDrive/dsprojects/dsproject_nyc_traffic" # parent of current src folder

    path_folder_landuse_geodata = path_folder + "../data/raw/raw_orig_geodata/raw_orig_geodata_landuse/MapPLUTO.shp"
    path_folder_street_segment_geodata = path_folder + "../data/raw/raw_orig_geodata/raw_orig_geodata_street_segment/lion.shp"

    saved_zscorer = pd.read_csv(path_folder + "../data/temp/saved_zscorer.csv", index_col="column_name")




    with open(path_folder +'../data/temp/ohe_dict.pkl', 'rb') as f:
        saved_ohe_dict = pickle.load(f)
        f.close()
    print("relpath works")


    orig_point = Point(x_coord, y_coord)
    temp_point_gdf = gpd.GeoDataFrame({'geometry': [orig_point]}, crs=4326)
    new_point = temp_point_gdf.to_crs({'init':'epsg:2263'})
    new_x_coord = new_point.iloc[0]["geometry"].centroid.x  
    new_y_coord = new_point.iloc[0]["geometry"].centroid.y
    point = Point(new_x_coord,new_y_coord)

    input_point_buffer = new_point.copy()
    BUFFER_RADIUS = 500
    input_point_buffer['geometry'] = input_point_buffer['geometry'].buffer(BUFFER_RADIUS)

    # street_segment coords, polyline, or id

    street_segment_geodata_column_names = ['Segment_ID', 'geometry']
    street_segment_geodata_orig = gpd.read_file(path_folder_street_segment_geodata,
                                                include_fields = street_segment_geodata_column_names,
                                                mask=input_point_buffer['geometry']
                                                )
    street_segment_geodata = street_segment_geodata_orig
    print("Read Street Segments Data")


    street_segment_orig_categorical_column_names = ['Snow_Prior', 'RW_TYPE', 'Status', 'CurveFlag', 'FeatureTyp', 'SegmentTyp', 'NonPed',  'TrafDir','LocStatus','BikeLane','ROW_Type']
    street_segment_orig_numerical_column_names = ['StreetWidt', 'SegCount', 'Number_Tra', 'Number_Par', 'Number_Tot', 'SHAPE_Leng'] 

    NULL_PLACEHOLDER = "NULL"
    street_segment_geodata[street_segment_orig_categorical_column_names] = street_segment_geodata[street_segment_orig_categorical_column_names].astype(str).fillna(NULL_PLACEHOLDER)

    non_streets_filter = street_segment_geodata_orig["RW_TYPE"].isin(["1","2","3","4","9","11","13",NULL_PLACEHOLDER])
    non_streets_filter &= street_segment_geodata_orig["FeatureTyp"].isin(["F", "A", "W", "9", "8","2","5"])==False
    non_streets_filter &= street_segment_geodata_orig["TrafDir"].isin(["P",NULL_PLACEHOLDER])==False # not Pedestrian only nor non street feature
    non_streets_filter &= street_segment_geodata_orig["Status"].isin(["2", NULL_PLACEHOLDER]) # constructed
    #non_streets_filter &= street_segment_geodata_orig["ROW_Type"].isin([NULL_PLACEHOLDER]) # not subway

    street_segment_geodata = street_segment_geodata_orig[non_streets_filter]

    # trimming down the uncertain relevance with just the bad columns (accoridng to this subset)
    # null is tolereable for categ, not for numer
    # very skew is tolerable for numer, not for categ

    street_segment_curr_categorical_column_names = ['Snow_Prior', 'RW_TYPE', 'Status', 'CurveFlag', 'FeatureTyp', 'SegmentTyp', 'NonPed']
    # if a categ is too skewed, info is useless. esp if the top categ is null.
    street_segment_curr_numerical_column_names = ['StreetWidt', 'SegCount', 'Number_Tra', 'SHAPE_Leng'] 


    street_segment_geodata[street_segment_curr_numerical_column_names] = street_segment_geodata[street_segment_curr_numerical_column_names].astype(float)
    street_segment_geodata[street_segment_curr_numerical_column_names] = street_segment_geodata[street_segment_curr_numerical_column_names].fillna(street_segment_geodata[street_segment_curr_numerical_column_names].median(axis=0))
    street_segment_geodata[street_segment_curr_numerical_column_names] = street_segment_geodata[street_segment_curr_numerical_column_names].apply(np.log1p)

    # getting by coords
    # X, Y
    #'''
    # orig_point = Point(-73.97273825353946, 40.60055508842727) for 25210
    # reproject

    #orig_point = Point(x_coord, y_coord)
    #temp_point_gdf = gpd.GeoDataFrame({'geometry': [orig_point]}, crs=4326)
    #new_point = temp_point_gdf.to_crs({'init':'epsg:2263'})
    new_x_coord = new_point.iloc[0]["geometry"].centroid.x  
    new_y_coord = new_point.iloc[0]["geometry"].centroid.y
    point = Point(new_x_coord,new_y_coord)
    
    polyline_index = street_segment_geodata.distance(point).sort_values().index[0]
    selected_street_segment_id = street_segment_geodata.loc[polyline_index, ["Segment_ID"]].values[0]#'''
    # getting by id
    street_segment_geodata_selection = street_segment_geodata[street_segment_geodata["Segment_ID"]==selected_street_segment_id]
    print("\t Found Closest Polyline")
    print("\t Street is:", street_segment_geodata_selection["Street"].iloc[0])


    is_weekend = int(is_weekend)
    hour_input = int(hour)

    street_segment_geodata[street_segment_curr_numerical_column_names] = (
        street_segment_geodata[street_segment_curr_numerical_column_names].sub(saved_zscorer.loc[street_segment_curr_numerical_column_names, "mean"], axis=1)
    )
    street_segment_geodata[street_segment_curr_numerical_column_names] = (
        street_segment_geodata[street_segment_curr_numerical_column_names].div(saved_zscorer.loc[street_segment_curr_numerical_column_names, "std"], axis=1)
    )

    street_segment_geodata[street_segment_curr_numerical_column_names].mean(axis=0)

    street_segment_geodata_selection = street_segment_geodata_selection.to_crs({'init':'epsg:2263'})
    BUFFER_RADIUS = 150
    street_segment_geodata_selection['geometry'] = street_segment_geodata_selection['geometry'].buffer(BUFFER_RADIUS)
    print("\t Buffered Polyline")

    landuse_geodata_column_names = ['geometry'] + ['LandUse']
    landuse_geodata_orig = gpd.read_file(path_folder_landuse_geodata, 
                                         include_fields=landuse_geodata_column_names,
                                         mask=street_segment_geodata_selection['geometry']
                                         )

    landuse_orig_categorical_column_names = ['Borough', 'ZoneDist1', 'ZoneDist2', 'ZoneDist3', 'ZoneDist4', 'Overlay1', 'Overlay2', 
                                        'SPDist1', 'SPDist2', 'SPDist3', 'LtdHeight', 'SplitZone', 'BldgClass', 'LandUse', 
                                        'OwnerType', 'Ext', 'ProxCode', 'IrrLotCode', 'LotType', 'BsmtCode']
    landuse_orig_numerical_column_names = ['Easements', 'LotArea', 'BldgArea', 'ComArea', 'ResArea', 'OfficeArea', 'RetailArea', 'GarageArea', 'StrgeArea', 'FactryArea', 
                                      'OtherArea', 'NumBldgs', 'NumFloors', 'UnitsRes', 'UnitsTotal', 'LotFront', 'LotDepth', 'BldgFront', 'BldgDepth', 'AssessLand',
                                      'AssessTot', 'ExemptTot', 'YearBuilt', 'BuiltFAR', 'ResidFAR', 'CommFAR', 'FacilFAR']#+[ 'Shape_Area']

    # trimming down the uncertain relevance with just the bad columns (accoridng to this subset)
    # null is tolereable for categ, not for numer
    # very skew is tolerable for numer, not for categ

    landuse_curr_categorical_column_names = ['Borough','SplitZone', 'LandUse', 'Ext', 'ProxCode', 'IrrLotCode', 'LotType']#+['BldgClass']
    # if a categ is too skewed, info is useless. esp if the top categ is null.
    landuse_curr_numerical_column_names = []
    landuse_curr_numerical_column_names += ['Easements'] 
    landuse_curr_numerical_column_names += ['BldgArea', 'ComArea', 'ResArea', 'OfficeArea', 'RetailArea', 'GarageArea', 'StrgeArea', 'FactryArea', 'OtherArea']
    landuse_curr_numerical_column_names += ['NumBldgs']
    landuse_curr_numerical_column_names += ['NumFloors']
    landuse_curr_numerical_column_names += ['LotArea',  'LotFront', 'LotDepth']
    landuse_curr_numerical_column_names += ['BldgFront', 'BldgDepth']
    landuse_curr_numerical_column_names += ['AssessLand','AssessTot', 'ExemptTot']
    landuse_curr_numerical_column_names += ['BuiltFAR', 'ResidFAR', 'CommFAR', 'FacilFAR']
    landuse_curr_numerical_column_names += ['UnitsRes']

    landuse_geodata = landuse_geodata_orig
    landuse_geodata[landuse_curr_numerical_column_names] = landuse_geodata[landuse_curr_numerical_column_names].astype(float)

    NULL_PLACEHOLDER = "NULL"
    landuse_geodata[landuse_curr_categorical_column_names] = landuse_geodata[landuse_curr_categorical_column_names].fillna(NULL_PLACEHOLDER)
    landuse_geodata[landuse_curr_categorical_column_names] = landuse_geodata[landuse_curr_categorical_column_names].astype(str)

    landuse_geodata['UnitsRes'] = landuse_geodata['UnitsRes']/landuse_geodata['UnitsTotal']

    temp_anti_zero_div_adjustor = (landuse_geodata["BldgArea"]==0).astype(int)
    landuse_geodata[['ComArea', 'ResArea', 'OfficeArea', 'RetailArea', 'GarageArea', 'StrgeArea', 'FactryArea', 'OtherArea']] = ((
        landuse_geodata[['ComArea', 'ResArea', 'OfficeArea', 'RetailArea', 'GarageArea', 'StrgeArea', 'FactryArea', 'OtherArea']]).fillna(0)
    ).div(landuse_geodata["BldgArea"] + temp_anti_zero_div_adjustor, axis=0).fillna(0)

    landuse_geodata[landuse_curr_numerical_column_names] = (
        landuse_geodata[landuse_curr_numerical_column_names].sub(saved_zscorer.loc[landuse_curr_numerical_column_names, "mean"], axis=1)
    )
    landuse_geodata[landuse_curr_numerical_column_names] = (
        landuse_geodata[landuse_curr_numerical_column_names].div(saved_zscorer.loc[landuse_curr_numerical_column_names, "std"], axis=1)
    )

    landuse_street_segment_geodata_clip = gpd.clip(landuse_geodata, street_segment_geodata_selection['geometry'])
    print("\t Clipped Buffer")
    landuse_street_segment_geodata = gpd.sjoin(landuse_street_segment_geodata_clip, street_segment_geodata_selection)
    print("\t Spatial Join Buffer")

    landuse_street_segment_geodata_concat_df_list = []
    landuse_street_segment_geodata_concat_df_list += [landuse_street_segment_geodata[["Segment_ID"]]]
    landuse_street_segment_geodata_concat_df_list += [landuse_street_segment_geodata[street_segment_curr_numerical_column_names + landuse_curr_numerical_column_names]]

    ohe_dict = dict()
    for preohe_column_name in landuse_curr_categorical_column_names:
        temp_ohe_df = pd.get_dummies(landuse_street_segment_geodata[preohe_column_name], prefix=preohe_column_name)
        ohe_dict[preohe_column_name] = list(temp_ohe_df.columns)
        landuse_street_segment_geodata_concat_df_list += [temp_ohe_df]
    for preohe_column_name in street_segment_curr_categorical_column_names:
        temp_ohe_df = pd.get_dummies(landuse_street_segment_geodata[preohe_column_name], prefix=preohe_column_name)
        ohe_dict[preohe_column_name] = list(temp_ohe_df.columns)
        landuse_street_segment_geodata_concat_df_list += [temp_ohe_df]

    landuse_street_segment_geodata_concat_df = pd.concat(landuse_street_segment_geodata_concat_df_list, axis=1)

    landuse_street_segment_data_agg_dict = dict()
    for column_name in (street_segment_curr_numerical_column_names + landuse_curr_numerical_column_names): landuse_street_segment_data_agg_dict[column_name] = np.mean;
    ohe_dict_values = (sum(list(ohe_dict.values()),[]))
    for column_name in (ohe_dict_values): landuse_street_segment_data_agg_dict[column_name] = np.sum;

    landuse_street_segment_data = (landuse_street_segment_geodata_concat_df.groupby(["Segment_ID"], as_index=False).agg(landuse_street_segment_data_agg_dict))


    for ohe_dict_key in list(ohe_dict.keys()): 
        ohe_dict_key_values = ohe_dict[ohe_dict_key]
        temp_anti_zero_div_adjustor = (landuse_street_segment_data[ohe_dict_key_values].sum(axis=1)==0).astype(int)
        landuse_street_segment_data.loc[:,ohe_dict_key_values] = landuse_street_segment_data.loc[:,ohe_dict_key_values].div(landuse_street_segment_data[ohe_dict_key_values].sum(axis=1)+temp_anti_zero_div_adjustor, axis=0)

    saved_ohe_dict_keys = sum(list(saved_ohe_dict.values()),[])
    observed_ohe_dict_keys = sum(list(ohe_dict.values()),[])
    unobserved_ohe_dict_keys = list(set(saved_ohe_dict_keys).difference(set(observed_ohe_dict_keys)))

    unseen_ohe_columns = pd.Series(index=unobserved_ohe_dict_keys,data=np.zeros(len(unobserved_ohe_dict_keys))).to_frame().T


    hours_vector = np.zeros(24)
    hours_vector[hour_input] = 1
    hours_column_names = [ 'Hour_0','Hour_1','Hour_2','Hour_3','Hour_4','Hour_5','Hour_6','Hour_7','Hour_8','Hour_9','Hour_10','Hour_11',
            'Hour_12','Hour_13','Hour_14','Hour_15','Hour_16','Hour_17','Hour_18','Hour_19','Hour_20','Hour_21','Hour_22','Hour_23']
    hours_columns = pd.Series(index=hours_column_names,data=hours_vector).to_frame().T

    time_columns = pd.Series(index=['is_weekend'],data=[is_weekend]).to_frame().T

    input_vector = pd.concat([landuse_street_segment_data,unseen_ohe_columns,hours_columns,time_columns],axis=0)

    cols = [ 'Hour_0','Hour_1','Hour_2','Hour_3','Hour_4','Hour_5','Hour_6','Hour_7','Hour_8','Hour_9','Hour_10','Hour_11',
            'Hour_12','Hour_13','Hour_14','Hour_15','Hour_16','Hour_17','Hour_18','Hour_19','Hour_20','Hour_21','Hour_22','Hour_23',
    ]
    cols += ['LandUse_01', 'LandUse_02', 'LandUse_03', 'LandUse_04', 'LandUse_05', 'LandUse_06',
             'LandUse_07', 'LandUse_08', 'LandUse_09', 'LandUse_10', 'LandUse_11', 'LandUse_NULL']
    cols += ['Number_Tra'] # 83
    cols += ['StreetWidt'] # 87
    cols += ['is_weekend'] # 90
    input_vector = input_vector[cols]

    with open(path_folder +'../data/final/dtr.pkl', 'rb') as f:
        model_dtr = pickle.load(f)
        f.close()

    result = model_dtr.predict(input_vector)

    result = np.expm1(np.mean(np.array([result])))

    return result
