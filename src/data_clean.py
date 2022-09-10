import geopandas as gpd
import pandas as pd
import numpy as np
from scipy.stats import zscore
import pickle
import os


path_folder = "" #os.getcwd()

path_folder_traffic_volume_data = path_folder + "../data/raw/raw_orig_data/Traffic_Volume_Counts__2014-2019_.csv"
path_folder_landuse_geodata = path_folder + "../data/raw/raw_orig_geodata/raw_orig_geodata_landuse/MapPLUTO.shp"
path_folder_street_segment_geodata = path_folder + "../data/raw/raw_orig_geodata/raw_orig_geodata_street_segment/lion.shp"

# read in the street segment data
street_segment_geodata_column_names = ['Segment_ID', 'geometry']
street_segment_geodata_orig = gpd.read_file(path_folder_street_segment_geodata,
                                            include_fields = street_segment_geodata_column_names
                                            )

street_segment_orig_categorical_column_names = ['Snow_Prior', 'RW_TYPE', 'Status', 'CurveFlag', 'FeatureTyp', 'SegmentTyp', 'NonPed',  'TrafDir','LocStatus','BikeLane','ROW_Type']
street_segment_orig_numerical_column_names = ['StreetWidt', 'SegCount', 'Number_Tra', 'Number_Par', 'Number_Tot', 'SHAPE_Leng'] 

street_segment_geodata = street_segment_geodata_orig.copy()
NULL_PLACEHOLDER = "NULL"
street_segment_geodata[street_segment_orig_categorical_column_names] = street_segment_geodata[street_segment_orig_categorical_column_names].fillna(NULL_PLACEHOLDER).astype(str)

# https://archive.nyu.edu/bitstream/2451/34565/3/lion_metadata.pdf
# Filtering the street segment data  because it also contains non-vehicular polylines (hiking trails, ferry routes, etc.)
non_streets_filter = street_segment_geodata["RW_TYPE"].isin(["1","2","3","4","9","11","13",NULL_PLACEHOLDER])
non_streets_filter &= street_segment_geodata["FeatureTyp"].isin(["F", "A", "W", "9", "8","2","5"])==False
non_streets_filter &= street_segment_geodata["TrafDir"].isin(["P",NULL_PLACEHOLDER])==False # not Pedestrian only nor non street feature
non_streets_filter &= street_segment_geodata["Status"].isin(["2", NULL_PLACEHOLDER]) # constructed
#non_streets_filter &= street_segment_geodata["ROW_Type"].isin([NULL_PLACEHOLDER]) # not subway
street_segment_geodata = street_segment_geodata[non_streets_filter]



# valid_street_segment_geodata = street_segment_geodata[["Segment_ID", "geometry"]]
# valid_street_segment_geodata.to_file(path_folder +'/display/input_street_segments.shp')


street_segment_curr_numerical_column_names = ['StreetWidt', 'Number_Tra']

# some of the numerical columns that were chosen are actually string datatypes, so it must be converted to numerical type
street_segment_geodata[street_segment_curr_numerical_column_names] = street_segment_geodata[street_segment_curr_numerical_column_names].astype(float)





# Reading the traffic count data, and extracting unique street segments to filter down the street segment data. 

traffic_volume_data_column_names = ['Segment ID']
traffic_volume_data_orig = pd.read_csv(path_folder_traffic_volume_data, 
                                       #usecols=traffic_volume_data_column_names
                                       )
street_segment_id_foreign_key_column_name = "Segment_ID"
unique_traffic_volume_data_street_segments = traffic_volume_data_orig[["Segment ID"]]
unique_traffic_volume_data_street_segments = unique_traffic_volume_data_street_segments.drop_duplicates()
unique_traffic_volume_data_street_segments = unique_traffic_volume_data_street_segments.rename(columns={"Segment ID": street_segment_id_foreign_key_column_name})


traffic_street_segment_geodata = street_segment_geodata.merge(unique_traffic_volume_data_street_segments, on=[street_segment_id_foreign_key_column_name])

# Buffer 500ft-radius areas around each street segment
traffic_street_segment_geodata = traffic_street_segment_geodata.to_crs({'init':'epsg:2263'})
BUFFER_RADIUS = 150 # 150 meters, roughly 500 ft.
traffic_street_segment_geodata['geometry'] = traffic_street_segment_geodata['geometry'].buffer(BUFFER_RADIUS)

# Read LandUse data, but use the street segment buffer as a mask.
landuse_geodata_column_names = ['geometry'] + ['LandUse']
landuse_geodata_orig = gpd.read_file(path_folder_landuse_geodata, 
                                     include_fields=landuse_geodata_column_names,
                                     mask=traffic_street_segment_geodata['geometry']
                                     )
landuse_geodata = landuse_geodata_orig.copy()
# https://www1.nyc.gov/assets/planning/download/pdf/data-maps/open-data/PLUTODD.pdf?v=22v2
# Reading through the mappluto landuse data's dictionary, we separate the columns into categorical and numerical groups. But not all columns were kept:
# while columns with certain relevance as well as uncertain relevance were kept; columns with certain irrelevance were dropped.
# Mostly, record-keeping or metadata-oriented columns were the ones that were removed.
landuse_curr_categorical_column_names = ['LandUse']

NULL_PLACEHOLDER = "NULL"
landuse_geodata[landuse_curr_categorical_column_names] = landuse_geodata[landuse_curr_categorical_column_names].fillna(NULL_PLACEHOLDER)
landuse_geodata[landuse_curr_categorical_column_names] = landuse_geodata[landuse_curr_categorical_column_names].astype(str)

# clip the landuse polygons with the street segment buffer polygons, and spacial join 
landuse_street_segment_geodata_clip = gpd.clip(landuse_geodata, traffic_street_segment_geodata['geometry'])
landuse_street_segment_geodata = gpd.sjoin(landuse_street_segment_geodata_clip, traffic_street_segment_geodata)


landuse_street_segment_geodata_concat_df_list = []
landuse_street_segment_geodata_concat_df_list += [landuse_street_segment_geodata[["Segment_ID"]]]
landuse_street_segment_geodata_concat_df_list += [landuse_street_segment_geodata[street_segment_curr_numerical_column_names 
                                                                                 #+ landuse_curr_numerical_column_names
                                                                                 ]]
ohe_dict = dict()
for preohe_column_name in landuse_curr_categorical_column_names:
    temp_ohe_df = pd.get_dummies(landuse_street_segment_geodata[preohe_column_name], prefix=preohe_column_name)
    ohe_dict[preohe_column_name] = list(temp_ohe_df.columns)
    landuse_street_segment_geodata_concat_df_list += [temp_ohe_df]
landuse_street_segment_geodata_concat_df = pd.concat(landuse_street_segment_geodata_concat_df_list, axis=1)

landuse_street_segment_data_agg_dict = dict()
for column_name in (street_segment_curr_numerical_column_names 
                    #+ landuse_curr_numerical_column_names
                    ): landuse_street_segment_data_agg_dict[column_name] = np.mean;
ohe_dict_values = (sum(list(ohe_dict.values()),[]))
for column_name in (ohe_dict_values): landuse_street_segment_data_agg_dict[column_name] = np.sum;

landuse_street_segment_data = (landuse_street_segment_geodata_concat_df.groupby(["Segment_ID"], as_index=False).agg(landuse_street_segment_data_agg_dict))

for ohe_dict_key in list(ohe_dict.keys()): 
    ohe_dict_key_values = ohe_dict[ohe_dict_key]
    temp_anti_zero_div_adjustor = (landuse_street_segment_data[ohe_dict_key_values].sum(axis=1)==0).astype(int)
    landuse_street_segment_data.loc[:,ohe_dict_key_values] = landuse_street_segment_data.loc[:,ohe_dict_key_values].div(landuse_street_segment_data[ohe_dict_key_values].sum(axis=1)+temp_anti_zero_div_adjustor, axis=0)

hourly_traffic_volume_column_names = ['12:00-1:00 AM', '1:00-2:00AM', '2:00-3:00AM', '3:00-4:00AM',
       '4:00-5:00AM', '5:00-6:00AM', '6:00-7:00AM', '7:00-8:00AM',
       '8:00-9:00AM', '9:00-10:00AM', '10:00-11:00AM', '11:00-12:00PM',
       '12:00-1:00PM', '1:00-2:00PM', '2:00-3:00PM', '3:00-4:00PM',
       '4:00-5:00PM', '5:00-6:00PM', '6:00-7:00PM', '7:00-8:00PM',
       '8:00-9:00PM', '9:00-10:00PM', '10:00-11:00PM', '11:00-12:00AM']

#eda_hour_imputation = '''

# Superkey for traffic data should be segment id, date, hour,
# Minimum necessary column necessary are segment id, date, hour, and traffic
# string columns like Roadway Name, From, To were thus removed.
# and Direction was deemed unnecessary duplicator column, and thus collapsed with sum

segment_id_column_name = "Segment ID"
date_column_name = "Date"

temp_hour_column_name = "Hour"
temp_traffic_column_name = "Traffic"

# convert numerical columns to float to keep nans
# fill categoricals with null string
# convert numerical columns to float to keep nans
# fillna before melting and groupby

temp_df = traffic_volume_data_orig.rename(columns=dict(zip(hourly_traffic_volume_column_names, list(range(24)))))
temp_df = temp_df.melt([segment_id_column_name,date_column_name], list(range(24)),temp_hour_column_name,temp_traffic_column_name)
temp_df = temp_df.groupby([segment_id_column_name,date_column_name]+[temp_hour_column_name], as_index=False).agg({temp_traffic_column_name: np.sum})

# check just in case
date_column_name = "Date"
date_column = pd.to_datetime(temp_df[date_column_name])
temp_df[date_column_name] = date_column

temp_is_weekend_column_name = "is_weekend"
temp_is_weekend_column = (date_column.dt.weekday.isin([5,6])).astype(int)
temp_df[temp_is_weekend_column_name] = temp_is_weekend_column


traffic_volume_data = temp_df

traffic_landuse_street_segment_data = landuse_street_segment_data.merge(traffic_volume_data,left_on=["Segment_ID"], right_on=["Segment ID"])
print(temp_df["Traffic"].max())

traffic_landuse_street_segment_data.head()
final_data_columns = [
    'StreetWidt','is_weekend',
                      'Number_Tra', 'LandUse_01', 'LandUse_02',
       'LandUse_03', 'LandUse_04', 'LandUse_05', 'LandUse_06', 'LandUse_07',
       'LandUse_08', 'LandUse_09', 'LandUse_10', 'LandUse_11', 'LandUse_NULL', 'Hour', 'Traffic']
traffic_landuse_street_segment_data1 = traffic_landuse_street_segment_data[final_data_columns]

traffic_landuse_street_segment_data1.to_csv(path_folder + "../data/temp/traffic_landuse_street_segment_data.csv", index=False)

traffic_landuse_street_segment_data.columns

