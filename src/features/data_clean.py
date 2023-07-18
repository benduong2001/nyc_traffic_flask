import geopandas as gpd
import pandas as pd
import numpy as np
import polars
import pyarrow

import os
import shutil

##import plotly
##import seaborn as sns
import matplotlib.pyplot as plt
##
##import pickle
##import tqdm
##import flask
##import logging
##import datetime
##import joblib
##from shapely.geometry import Point
##
##import sodapy
##from sodapy import Socrata
import geojson
##import gdown
##from bs4 import BeautifulSoup
##import requests
##import zipfile
##import io
##import json
##import yaml
##import jinja2
##import dbt
##import airflow
##
##import boto3
##import sqlalchemy
##import psycopg
##
##import statsmodels
##from sklearn.decomposition import PCA
##from sklearn.ensemble import RandomForestClassifier 
##from sklearn.tree import DecisionTreeClassifier
##from sklearn.linear_model import LinearRegression, LogisticRegression
##from sklearn.tree import DecisionTreeRegressor
##from sklearn.preprocessing import StandardScaler
##from sklearn.preprocessing import OneHotEncoder
##from sklearn.preprocessing import FunctionTransformer
##from sklearn.compose import ColumnTransformer
##from sklearn.impute import SimpleImputer
##from sklearn.pipeline import Pipeline
##from sklearn.model_selection import train_test_split
##from sklearn.model_selection import cross_val_score
##from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
##from sklearn.metrics import classification_report
##from sklearn.metrics import confusion_matrix

# Approach 1: GeoPandas->Pandas->Polars & Polars (Fully Python) [Done]
# Approach 2: GeoPandas->Pandas & Regular SQL (Mixture)
# Approach 3: PostGIS & dbt (Fully SQL)

# All data already on Cloud: https://lumigo.io/learn/aws-lambda-python/ (use lambda)
# [No-Cld, No db]: online_data + prestored street segments -> clone locally (or docker image) -> approach 1 (no db)
class Temp_Data_Preparation_Builder:
    def __init__(self, args):
        self.args = args
        self.set_default_variables()
    def set_default_variables(self):
        self.street_segment_gdf_column_names = [
            'StreetWidt', 'Number_Tra',
            'RW_TYPE', "FeatureTyp",
            "TrafDir",  "Status","SHAPE_Leng",
                'Segment_ID', 'geometry',
                ]
        self.crs = 3857
        self.buffer_radius = 150
        self.landuse_gdf_column_names = ['LandUse','geometry'] #+ ['Borough','LandUse','NumFloors','geometry']
        self.null_placeholder = "NULL"
        self.landuse_values = [str.zfill(str(x),2) for x in range(1,12)] + [self.null_placeholder]
    def load_street_segments_gdf(
        self,
        path_file_street_segment_shapefile,
        street_segment_gdf_column_names=None,
        selected_point_buffer_gdf=None,
        ):

        if street_segment_gdf_column_names is None: street_segment_gdf_column_names = self.street_segment_gdf_column_names;
            
        street_segment_gdf = gpd.read_file(
            path_file_street_segment_shapefile,
            include_fields = street_segment_gdf_column_names,
            mask=selected_point_buffer_gdf
        )
        return street_segment_gdf[street_segment_gdf_column_names]

    def project_gdf(
        self,
        gdf,
        crs=None,
        ):
        if crs is None: crs = self.crs
        gdf = gdf.to_crs(crs)
        return gdf


    def street_segment_cleaned(
        self,
        street_segment_gdf,
        ):
        street_segment_categorical_column_names = ['RW_TYPE', 'Status','FeatureTyp',  'TrafDir',]
        # The ultimate goal for this street segment data:
        # Another goal is to have its categorical columns one-hot-encoded, and its numerical columns lognormalized
        # But first,

        # first off, imputation needs to be done. Before doing so, we need to pick out categorical and numerical columns, because these 2 groups have their own methods of imputation.
        # https://archive.nyu.edu/bitstream/2451/34565/3/lion_metadata.pdf
        # Reading through the street segment data's dictionary, we separate the columns into categorical and numerical groups. But not all columns were kept:
        # while columns with certain relevance as well as uncertain relevance were kept; columns with certain irrelevance were dropped.
        # Mostly, record-keeping or metadata-oriented columns were the ones that were removed.
        NULL_PLACEHOLDER = self.null_placeholder
        street_segment_gdf[street_segment_categorical_column_names] = street_segment_gdf[street_segment_categorical_column_names].fillna(NULL_PLACEHOLDER).astype(str)

        #street_segment_categorical_column_names = ['Snow_Prior', 'RW_TYPE', 'Status', 'CurveFlag', 'FeatureTyp', 'SegmentTyp', 'NonPed']
        street_segment_numerical_column_names = ['StreetWidt', 'Number_Tra'] #+['SegCount', 'SHAPE_Leng']
        # some of the numerical columns that were chosen are actually string datatypes, so it must be converted to numerical type
        street_segment_gdf[street_segment_numerical_column_names] = street_segment_gdf[street_segment_numerical_column_names].astype(float)

        streets_filter = (street_segment_gdf["Segment_ID"].isnull()==False)
        streets_filter &= (street_segment_gdf["StreetWidt"]>=8) # minimum street width is 8. minimum width of 2 lane road in NYC is 16; see https://www.dot.ny.gov/divisions/engineering/technical-services/hds-respository/Tab/NYSDOT_Local_Highway_Inventory_Manual_April_2018.pdf 
        streets_filter &= (street_segment_gdf["Number_Tra"]>=1) # minimum lane amount is 1
        streets_filter &= street_segment_gdf["RW_TYPE"].isin(["1","2","3","4","9","11","13",NULL_PLACEHOLDER])
        streets_filter &= street_segment_gdf["FeatureTyp"].isin(["F", "A", "W", "9", "8","2","5"])==False
        streets_filter &= street_segment_gdf["TrafDir"].isin(["P",NULL_PLACEHOLDER])==False # not Pedestrian only nor non street feature
        streets_filter &= street_segment_gdf["Status"].isin(["2", NULL_PLACEHOLDER]) # constructed
        #streets_filter &= street_segment_geodata["ROW_Type"].isin([NULL_PLACEHOLDER]) # not subway
        street_segment_gdf = street_segment_gdf[streets_filter]

        # Median will be the numerical column imputation value. In this case, only Number_Tra needs to be imputed; by choosing np.median as the imputer value, the pre-imputed and post-imputed look the least dissimilar. The mean can work too.
        # this is still the case even if numerical imputation was done before vehicular-street filtering.
        # street_segment_geodata['Number_Tra'] = street_segment_geodata['Number_Tra'].fillna(street_segment_geodata['Number_Tra'].median(axis=0))
        street_segment_cleaned_gdf = street_segment_gdf
        return street_segment_cleaned_gdf

    def save_gdf_to_geojson(
        self,
        gdf,
        path_file_geojson
        ):
        #path_folder = args["path_folder"]
        #path_file_geojson = os.path.join(path_folder,"display","static",file_name_geojson)
        
        gdf.to_file(path_file_geojson, driver="GeoJSON")
        #with open(path_file_geojson , 'r') as f:
        #    gj = geojson.load(f)
        #features = gj['features'][0]
        #assert len(gj['features']) == gdf.shape[0]

    def street_segment_buffers(
        self,
        street_segment_cleaned_gdf,
        buffer_radius=None,
        ):
        if buffer_radius is None: buffer_radius = self.buffer_radius;
        street_segment_buffers_gdf = street_segment_cleaned_gdf[["Segment_ID","geometry"]]
        street_segment_buffers_gdf['geometry'] = street_segment_buffers_gdf['geometry'].buffer(buffer_radius)
        return street_segment_buffers_gdf

    def load_landuse_gdf(
        self,
        path_file_landuse_shapefile,
        landuse_gdf_column_names=None,
        street_segment_buffers_gdf=None,
        ):
        if landuse_gdf_column_names is None: landuse_gdf_column_names = self.landuse_gdf_column_names;
        landuse_gdf = gpd.read_file(
            path_file_landuse_shapefile,
            include_fields = landuse_gdf_column_names,
            mask = street_segment_buffers_gdf,
        )
        return landuse_gdf[landuse_gdf_column_names]
    # Read LandUse data, but use the street segment buffer as a mask.

    def landuse_cleaned(
        self,
        landuse_gdf,
        ):
        # gdf
        landuse_cleaned_gdf = landuse_gdf
        return landuse_cleaned_gdf

    def street_segment_buffer_landuses(
        self,
        landuse_cleaned_gdf,
        street_segment_buffers_gdf,
        ):
        assert landuse_cleaned_gdf.crs == street_segment_buffers_gdf.crs
        
        street_segment_buffer_landuses_gdf = gpd.sjoin(landuse_cleaned_gdf, street_segment_buffers_gdf)
        return street_segment_buffer_landuses_gdf

    def street_segment_buffer_landuses_pivoted(
        self,
        street_segment_buffer_landuses_gdf,
        landuse_values=None,
        ):
        street_segment_buffer_landuses_df = street_segment_buffer_landuses_gdf[["Segment_ID","LandUse","geometry"]]
        if landuse_values is None: landuse_values = self.landuse_values; # ["01","02", ... "11"]
        street_segment_buffer_landuses_df.loc[~(street_segment_buffer_landuses_df["LandUse"].isin(landuse_values)), "LandUse"] = self.null_placeholder
        # fill all null or unknown or never-before-seen landuse categories with "NULL"

        street_segment_buffer_landuses_df["LandUse"] = "LandUse_" + street_segment_buffer_landuses_df["LandUse"]
        # Adds an identifable encoding prefix to the landuse columns
        
        street_segment_buffer_landuses_pivoted_df = (
            pd.pivot_table(street_segment_buffer_landuses_df,
                           index="Segment_ID",
                           columns="LandUse",
                           aggfunc="count")
            )
        # "Collapses" the landuses seen within each sreet segment's buffer by pivoting landuse. Now, the street segment id column (segment id) should now be unique
        print("street_segment_buffer_landuses_pivoted_df")
        print(street_segment_buffer_landuses_pivoted_df.shape)
        
        landuse_column_names = list(street_segment_buffer_landuses_pivoted_df.columns.droplevel(0))
        street_segment_buffer_landuses_pivoted_df.columns = landuse_column_names
        street_segment_buffer_landuses_pivoted_df.reset_index(inplace=True)
        street_segment_buffer_landuses_pivoted_df.fillna(0, inplace=True)
        # uninteresting operations to clean up the format of the new pivot dataframe (indexing-levels, nulls)
        
        temp_landuse_columns = street_segment_buffer_landuses_pivoted_df[landuse_column_names]
        temp_num_landuses = temp_landuse_columns.values.sum(axis=1,keepdims=True) # row-wise sum


        street_segment_buffer_landuses_pivoted_perc_df = temp_landuse_columns / temp_num_landuses
        # each row now adds up to 1

        street_segment_buffer_landuses_pivoted_df.loc[:,landuse_column_names] = street_segment_buffer_landuses_pivoted_perc_df.values
        print("street_segment_buffer_landuses_pivoted_df")
        print(street_segment_buffer_landuses_pivoted_df.shape)
        return street_segment_buffer_landuses_pivoted_df

    def street_segment_geoenriched(
        self,
        street_segment_cleaned_gdf,
        street_segment_buffer_landuses_pivoted_df,
        ):

        street_segment_cleaned_gdf = street_segment_cleaned_gdf[[
            "Segment_ID",
            "StreetWidt",
            "Number_Tra",
            "SHAPE_Leng",
            #"geometry",
            ]] 
        street_segment_geoenriched_gdf = (
            street_segment_cleaned_gdf
            ).merge(
                street_segment_buffer_landuses_pivoted_df,
                on=["Segment_ID"],
                how="left",
                )
        return street_segment_geoenriched_gdf


    def load_traffic_volume_df(
        self,
        path_file_traffic_volume,
        ):
        traffic_volume_df = polars.read_parquet(path_file_traffic_volume)
        # https://pola-rs.github.io/polars-book/user-guide/io/aws/#read
        # https://pola-rs.github.io/polars-book/user-guide/io/database/#read-from-a-database
        # read_parquet, read_csv, read_s3
        return traffic_volume_df    
        

    def traffic_volume_cleaned(
        self,
        traffic_volume_df,
        ):
        traffic_volume_df = traffic_volume_df.with_columns(
            ((polars.col("M")//3).alias("Season")),
            ((polars.date(polars.col("Yr"),polars.col("M"),polars.col("D"))).dt.weekday()).alias("DayOfWeek"),
            ((polars.date(polars.col("Yr"),polars.col("M"),polars.col("D"))).dt.weekday() >= 6).cast(polars.Int64, strict=False).alias("IsWeekend"),
            )

        traffic_volume_cleaned_df = traffic_volume_df.drop_nulls("SegmentID") # TODO: NaN and Null
        return traffic_volume_cleaned_df

    def traffic_volume_agg(
        self,
        traffic_volume_cleaned_df,
        ):
        traffic_volume_agg_season_weekend_hour_df = (
        traffic_volume_cleaned_df.groupby(["SegmentID","Season","IsWeekend","HH"]).agg(polars.col("Vol").mean())
            )
        traffic_volume_agg_df = traffic_volume_agg_season_weekend_hour_df
        return traffic_volume_agg_df

    def street_segment_landuse_traffic_volume(
        self,
        traffic_volume_agg_df,
        street_segment_geoenriched_gdf,
        ):
        street_segment_geoenriched_df = polars.from_pandas(street_segment_geoenriched_gdf)
        street_segment_geoenriched_df = street_segment_geoenriched_df.with_columns((polars.col("Segment_ID")).alias("SegmentID"))
        # street_segment_buffer_landuse_traffic_volume_agg_season_weekend_hour_df
        street_segment_landuse_traffic_volume_df = traffic_volume_agg_df.join(street_segment_geoenriched_df, on="SegmentID", how="left")
        return street_segment_landuse_traffic_volume_df

    def save_final_data(
        self,
        street_segment_landuse_traffic_volume_df,
        path_file_street_segment_landuse_traffic_volume,
        ):
        street_segment_landuse_traffic_volume_df.write_csv(path_file_street_segment_landuse_traffic_volume)

    def data_clean(
        self,
        args=None,
        ):
        if args is None: args = self.args;

        path_folder = args["path_folder"]

        ##args["path_file_street_segment_shapefile"] = os.path.join(args["path_folder_street_segment_shapefile"], "lion.shp") # TODO: can be taken uplevel,
        ##args["path_file_landuse_shapefile"] = os.path.join(args["path_folder_landuse_shapefile"], "MapPLUTO.shp") # TODO: can be taken uplevel,
        print("loading street segments")

        path_file_street_segment_shapefile = args["path_file_street_segment_shapefile"]
        street_segment_gdf = self.load_street_segments_gdf(path_file_street_segment_shapefile)
        street_segment_projected_gdf = self.project_gdf(street_segment_gdf, self.crs)
        street_segment_cleaned_gdf = self.street_segment_cleaned(street_segment_projected_gdf)
        print(type(street_segment_cleaned_gdf), street_segment_cleaned_gdf.shape)

        print("saving street segments to geojson")
        path_file_street_segment_geojson = args["path_file_street_segment_geojson"] # os.path.join(path_folder,"display","static","street_segment.geojson")
        #args["path_file_street_segment_geojson"] = path_file_street_segment_geojson # TODO: can be taken uplevel,
        #self.save_gdf_to_geojson(street_segment_cleaned_gdf[["Segment_ID","Number_Tra","StreetWidt","SHAPE_Leng","geometry"]],path_file_street_segment_geojson)

        print("buffering street segments")
        street_segment_buffers_gdf = self.street_segment_buffers(street_segment_cleaned_gdf, self.buffer_radius)

        print("loading landuses")
        path_file_landuse_shapefile = args["path_file_landuse_shapefile"]
        landuse_gdf = self.load_landuse_gdf(path_file_landuse_shapefile)
        landuse_projected_gdf = self.project_gdf(landuse_gdf, self.crs)
        landuse_cleaned_gdf = self.landuse_cleaned(landuse_projected_gdf)
        print(type(landuse_cleaned_gdf), landuse_cleaned_gdf.shape)

        print("saving landuses to geojson")
        path_file_landuse_geojson = args["path_file_landuse_geojson"] # os.path.join(path_folder,"display","static","landuse.geojson")
        #args["path_file_landuse_geojson"] = path_file_landuse_geojson # TODO: can be taken uplevel,
        #self.save_gdf_to_geojson(landuse_cleaned_gdf[['LandUse','geometry']],path_file_landuse_geojson)
        
        print("spatially joining landuses to street segment buffers")
        street_segment_buffer_landuses_gdf = self.street_segment_buffer_landuses(landuse_cleaned_gdf,street_segment_buffers_gdf,)
        print(street_segment_buffer_landuses_gdf.shape)
        street_segment_buffer_landuses_pivoted_df = self.street_segment_buffer_landuses_pivoted(street_segment_buffer_landuses_gdf)
        print("geoenriching street segments")
        street_segment_geoenriched_gdf = self.street_segment_geoenriched(street_segment_cleaned_gdf,street_segment_buffer_landuses_pivoted_df)
        print(type(street_segment_geoenriched_gdf), street_segment_geoenriched_gdf.shape)

        print("loading traffic volume")
        path_file_traffic_volume = args["path_file_traffic_volume"]
        traffic_volume_df = self.load_traffic_volume_df(path_file_traffic_volume,)
        traffic_volume_cleaned_df = self.traffic_volume_cleaned(traffic_volume_df,)

        print("aggregating traffic volume")
        traffic_volume_agg_df = self.traffic_volume_agg(traffic_volume_cleaned_df)
        print(traffic_volume_agg_df.shape)

        print("joining traffic volume with street segment buffer landuse")
        street_segment_landuse_traffic_volume_df = self.street_segment_landuse_traffic_volume(traffic_volume_agg_df, street_segment_geoenriched_gdf)
        print(street_segment_landuse_traffic_volume_df.shape)

        print("saving final results")
        path_file_street_segment_landuse_traffic_volume = args["path_file_street_segment_landuse_traffic_volume"] # os.path.join(path_folder,"data","temp","street_segment_landuse_traffic_volume.parquet")
        # args["path_file_street_segment_landuse_traffic_volume"] = path_file_street_segment_landuse_traffic_volume # TODO: can be taken uplevel,
        self.save_final_data(street_segment_landuse_traffic_volume_df, path_file_street_segment_landuse_traffic_volume)

        self.args = args

    def data_clean_optimized(
        self,
        args=None,
        ):
        if args is None: args = self.args;

        path_folder = args["path_folder"]

        ##args["path_file_street_segment_shapefile"] = os.path.join(args["path_folder_street_segment_shapefile"], "lion.shp") # TODO: can be taken uplevel,
        ##args["path_file_landuse_shapefile"] = os.path.join(args["path_folder_landuse_shapefile"], "MapPLUTO.shp") # TODO: can be taken uplevel,


        print("loading street segments")
        path_file_street_segment_shapefile = args["path_file_street_segment_shapefile"]
        street_segment_gdf = self.load_street_segments_gdf(path_file_street_segment_shapefile)
        street_segment_projected_gdf = self.project_gdf(street_segment_gdf, self.crs)
        street_segment_cleaned_gdf = self.street_segment_cleaned(street_segment_projected_gdf)
        print(type(street_segment_cleaned_gdf), street_segment_cleaned_gdf.shape)

        print("saving street segments to geojson")
        path_file_street_segment_geojson = args["path_file_street_segment_geojson"] # os.path.join(path_folder,"display","static","street_segment.geojson")
        #args["path_file_street_segment_geojson"] = path_file_street_segment_geojson # TODO: can be taken uplevel,
        self.save_gdf_to_geojson(street_segment_cleaned_gdf[["Segment_ID","Number_Tra","StreetWidt","SHAPE_Leng","geometry"]],path_file_street_segment_geojson)

        # In this optimized version of the data cleaning, we filter down the street segments even further by only getting those seen in the traffic volume dataset
        # It is important to NOT do this before saving the street segment's geojson, so that the geojson still has the full scope of input roads.
        print("loading traffic volume")
        path_file_traffic_volume = args["path_file_traffic_volume"]
        traffic_volume_df = self.load_traffic_volume_df(path_file_traffic_volume,)
        traffic_volume_cleaned_df = self.traffic_volume_cleaned(traffic_volume_df,)

        print("filtering street segments further to only include those seen in traffic volume dataset")
        unique_street_segment_df = traffic_volume_cleaned_df.unique(subset=["SegmentID"])[["SegmentID"]].to_pandas()
        street_segment_cleaned_gdf = street_segment_cleaned_gdf.merge(unique_street_segment_df, left_on="Segment_ID", right_on="SegmentID")
        print(type(street_segment_cleaned_gdf), street_segment_cleaned_gdf.shape)

        #fig, ax = plt.subplots()
        #street_segment_cleaned_gdf.plot(ax=ax)
        #plt.show()
        

        print("buffering street segments")
        street_segment_buffers_gdf = self.street_segment_buffers(street_segment_cleaned_gdf, self.buffer_radius)

        print("loading landuses")
        path_file_landuse_shapefile = args["path_file_landuse_shapefile"]
        landuse_gdf = self.load_landuse_gdf(path_file_landuse_shapefile,street_segment_buffers_gdf=street_segment_buffers_gdf)
        landuse_projected_gdf = self.project_gdf(landuse_gdf, self.crs)
        landuse_cleaned_gdf = self.landuse_cleaned(landuse_projected_gdf)
        print(type(landuse_cleaned_gdf), landuse_cleaned_gdf.shape)

        print("saving landuses to geojson")
        path_file_landuse_geojson = args["path_file_landuse_geojson"] # os.path.join(path_folder,"display","static","landuse.geojson")
        #args["path_file_landuse_geojson"] = path_file_landuse_geojson # TODO: can be taken uplevel,
        self.save_gdf_to_geojson(landuse_cleaned_gdf[['LandUse','geometry']],path_file_landuse_geojson)

        
        print("spatially joining landuses to street segment buffers")
        street_segment_buffer_landuses_gdf = self.street_segment_buffer_landuses(landuse_cleaned_gdf,street_segment_buffers_gdf,)
        print(street_segment_buffer_landuses_gdf.shape)

        print("street_segment_buffer_landuses")
        street_segment_buffer_landuses_pivoted_df = self.street_segment_buffer_landuses_pivoted(street_segment_buffer_landuses_gdf)
        print(street_segment_buffer_landuses_pivoted_df.shape)
        
        print("geoenriching street segments")
        street_segment_geoenriched_gdf = self.street_segment_geoenriched(street_segment_cleaned_gdf,street_segment_buffer_landuses_pivoted_df)
        print(type(street_segment_geoenriched_gdf), street_segment_geoenriched_gdf.shape)




        print("aggregating traffic volume")
        traffic_volume_agg_df = self.traffic_volume_agg(traffic_volume_cleaned_df)
        print(traffic_volume_agg_df.shape)

        print("joining traffic volume with street segment buffer landuse")
        street_segment_landuse_traffic_volume_df = self.street_segment_landuse_traffic_volume(traffic_volume_agg_df, street_segment_geoenriched_gdf)
        print(street_segment_landuse_traffic_volume_df.shape)

        print("saving final results")
        path_file_street_segment_landuse_traffic_volume = args["path_file_street_segment_landuse_traffic_volume"] # os.path.join(path_folder,"data","temp","street_segment_landuse_traffic_volume.parquet")
        # args["path_file_street_segment_landuse_traffic_volume"] = path_file_street_segment_landuse_traffic_volume # TODO: can be taken uplevel,
        self.save_final_data(street_segment_landuse_traffic_volume_df, path_file_street_segment_landuse_traffic_volume)

        self.args = args

        
def main(args):
    temp_data_preparation_builder_object = Temp_Data_Preparation_Builder(args)
    #temp_data_preparation_builder_object.data_clean(args);
    temp_data_preparation_builder_object.data_clean_optimized(args);
    return temp_data_preparation_builder_object
    
"""
WITH
street_segment_orig AS (
SELECT
SS0.* FROM
street_segment_orig SS0
)

, street_segment_projected AS (
SELECT
SS0.*,
ST_Transform(SS0.geometry,3857) as geom
FROM street_segment_orig SS0
)

, landuse_projected AS (
SELECT
L0.*,
ST_Transform(L0.geometry,3857) as geom
FROM landuse_orig L0
)

, street_segment_cleaned AS (
SS0.Segment_ID
, SS0.SHAPE_Leng AS StreetLength
, SS0.StreetWidt::DOUBLE AS StreetWidth
, SS0.Number_Tra::INTEGER AS Number_Travel_Lanes
, COALESCE(SS0.FeatureTyp, 'NULL') AS FeatureType
, COALESCE(SS0.RW_TYPE, 'NULL') AS RW_TYPE
, COALESCE(SS0.TrafDir, 'NULL') AS TrafDir
, COALESCE(SS0.Status, 'NULL') AS Status
, SS0.geometry
FROM street_segment_projected SS0
WHERE
RW_Type IN (['1','2','3','4','9','11','13','NULL')
AND FeatureType NOT IN ('F', 'A', 'W', '9', '8','2','5')
AND (TrafDir <> 'P' OR TrafDir <> 'NULL')
AND (Status = '2' OR Status = 'NULL')
)

, street_segment_buffers AS (
SELECT
SS1.Segment_ID,
ST_BUFFER(SS1.geom, 150) AS geom
FROM street_segment_cleaned SS1
)

, street_segment_buffer_landuses AS (
SELECT
SSB0.Segment_ID,
L0.landuse
FROM street_segment_buffers AS SSB0
JOIN landuse AS L0
ON ST_Contains(L0.geom, SSB0.geom) OR ST_Intersects(L0.geom, SSB0.geom)
)

, street_segment_buffer_landuses_pivoted AS (
SELECT
SSBL0.Segment_ID
, SUM(CASE WHEN (SSBL0.LandUse = '01') THEN 1 ELSE 0 END) AS LandUse_01
, SUM(CASE WHEN (SSBL0.LandUse = '02') THEN 1 ELSE 0 END) AS LandUse_02
, SUM(CASE WHEN (SSBL0.LandUse = '03') THEN 1 ELSE 0 END) AS LandUse_03
, SUM(CASE WHEN (SSBL0.LandUse = '04') THEN 1 ELSE 0 END) AS LandUse_04
, SUM(CASE WHEN (SSBL0.LandUse = '05') THEN 1 ELSE 0 END) AS LandUse_05
, SUM(CASE WHEN (SSBL0.LandUse = '06') THEN 1 ELSE 0 END) AS LandUse_06
, SUM(CASE WHEN (SSBL0.LandUse = '07') THEN 1 ELSE 0 END) AS LandUse_07
, SUM(CASE WHEN (SSBL0.LandUse = '08') THEN 1 ELSE 0 END) AS LandUse_08
, SUM(CASE WHEN (SSBL0.LandUse = '09') THEN 1 ELSE 0 END) AS LandUse_09
, SUM(CASE WHEN (SSBL0.LandUse = '10') THEN 1 ELSE 0 END) AS LandUse_10
, SUM(CASE WHEN (SSBL0.LandUse = '11') THEN 1 ELSE 0 END) AS LandUse_11
, SUM(CASE WHEN (SSBL0.LandUse IS NULL) THEN 1 ELSE 0 END) AS LandUse_NULL
COUNT(*) AS tempNumLandUses
FROM street_segment_buffer_landuses SSBL0
GROUP BY SSBL0.Segment_ID
)

, street_segment_geoenriched AS (
SELECT
SS1.Segment_ID
, SS1.StreetWidth
, SS1.Number_Travel_Lanes
, SS1.StreetLength
, SSBLP0.LandUse_01/SSBLP0.tempNumLandUses AS LandUse_01
, SSBLP0.LandUse_02/SSBLP0.tempNumLandUses AS LandUse_02
, SSBLP0.LandUse_03/SSBLP0.tempNumLandUses AS LandUse_03
, SSBLP0.LandUse_04/SSBLP0.tempNumLandUses AS LandUse_04
, SSBLP0.LandUse_05/SSBLP0.tempNumLandUses AS LandUse_05
, SSBLP0.LandUse_06/SSBLP0.tempNumLandUses AS LandUse_06
, SSBLP0.LandUse_07/SSBLP0.tempNumLandUses AS LandUse_07
, SSBLP0.LandUse_08/SSBLP0.tempNumLandUses AS LandUse_08
, SSBLP0.LandUse_09/SSBLP0.tempNumLandUses AS LandUse_09
, SSBLP0.LandUse_10/SSBLP0.tempNumLandUses AS LandUse_10
, SSBLP0.LandUse_11/SSBLP0.tempNumLandUses AS LandUse_11
, SSBLP0.LandUse_NULL/SSBLP0.tempNumLandUses AS LandUse_NULL
FROM street_segment_cleaned SS1
LEFT JOIN
street_segment_buffer_landuses_pivoted SSBLP0
ON
SS1.Segment_ID = SSBLP0.Segment_ID
)

, traffic_volume_cleaned AS (
SELECT
TV0.*
, TV0.M // 3 AS Season
, EXTRACT(ISODOW FROM MAKE_DATE(TV0.Yr, TV0.M, TV0.D)) AS DayOfWeek
, (CASE WHEN (EXTRACT(ISODOW FROM MAKE_DATE(TV0.Yr, TV0.M, TV0.D)) < 6) THEN 0 ELSE 1 END) AS IsWeekend
FROM traffic_volume_orig TV0
)

, traffic_volume_agg_directions AS (
SELECT
TV0.SegmentID
, TV0.Yr
, TV0.M
, TV0.D
, TV0.HH
, TV0.MM
, SUM(TV0.Vol) AS Vol
FROM traffic_volume_cleaned TV0
GROUP BY
TV0.SegmentID
, TV0.Yr
, TV0.M
, TV0.D
, TV0.HH
, TV0.MM
)

, traffic_volume_agg_season_weekend_hour AS (
SELECT
TV0.SegmentID
, TV0.Season
, TV0.IsWeekend
, TV0.HH
, AVG(TV0.Vol) AS Vol
FROM traffic_volume_agg_directions TV0
GROUP BY
TV0.SegmentID
, TV0.Season
, TV0.IsWeekend
, TV0.HH
)

, final_data AS (
SELECT
SSGE0.*,
TVSWH0.Season,
TVSWH0.IsWeekend,
TVSWH0.HH AS Hour,
FROM traffic_volume_agg_season_weekend_hour TVSWH0
LEFT JOIN street_segment_geoenriched SSGE0
ON TVSWH0.SegmentID = SSGE0.Segment_ID
)

SELECT *
FROM final_data;
"""
