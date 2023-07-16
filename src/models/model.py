##import geopandas as gpd
import pandas as pd
import numpy as np
import polars
#import pyarrow

import os
#import shutil

##import plotly
#import seaborn as sns
#import matplotlib.pyplot as plt
##
import pickle
#import tqdm
##import flask
##import logging
##import datetime
##import joblib
##from shapely.geometry import Point
##
##import sodapy
##from sodapy import Socrata
##import geojson
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
#import statsmodels
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.preprocessing import StandardScaler, OneHotEncoder, FunctionTransformer
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, classification_report, confusion_matrix


class Temp_Model_Builder:
    def __init__(self, args):
        self.args = args
        self.seed = 1
    def load_input_data(self, path_file_street_segment_landuse_traffic_volume):
        df = pd.read_csv(path_file_street_segment_landuse_traffic_volume)
        df = df.dropna(subset=["Segment_ID"])
        df = df.fillna({"Number_Tra": 2}) # Number_Tra is the only column with nulls left. All roads should have at bare minimum 2 travel lanes. So this gets imputed by 2
        self.df = df
        return df
    
    def build_column_names(self):
        column_names = {
        "categorical_column_names": ["HH","Season"],
        "numerical_column_names": ["StreetWidt","Number_Tra"]+["LandUse_"+str.zfill(str(x),2) for x in range(1,12)]+["LandUse_NULL"],
        "boolean_column_names": ["IsWeekend"],
        "loggable_numerical_column_names": ["SHAPE_Leng"],
        }
        self.column_names = column_names
        self.input_column_names = sum(list(column_names.values()),[])
        self.output_column_name = "Vol"
    def build_X(self, X=None):
        if X is None:
            X = self.df[self.input_column_names]
        self.X = X
        return X
    def build_y(self, y=None):
        if y is None:
            tri_level = lambda x: [["Low","Medium"][x>=-1],"High"][x>=1]
            y = pd.Series(StandardScaler().fit_transform(self.df[[self.output_column_name]]
                                                         .apply(np.log1p)).T[0]).apply(tri_level)
        self.y = y
        return y
    def build_train_test(
        self,
        X=None,
        y=None,
    ):
        if X is None or y is None:
            X = self.X
            y = self.y
        np.random.seed(self.seed)
        X_train, X_test, y_train, y_test = train_test_split(X,y,stratify=y)
        self.X_train = X_train
        self.X_test = X_test
        self.y_train = y_train
        self.y_test = y_test
    def build_preprocessing_pipeline(self):
        column_names = self.column_names
        
        categorical_column_names = column_names["categorical_column_names"]
        numerical_column_names = column_names["numerical_column_names"]
        loggable_numerical_column_names = column_names["loggable_numerical_column_names"]
        boolean_column_names = column_names["boolean_column_names"]
        
        categorical_pipeline = Pipeline([
            #("imp", SimpleImputer(strategy="constant", fill_value=0)),
            ("one-hot", OneHotEncoder(sparse=True,handle_unknown="ignore")),
        ])

        numerical_pipeline = Pipeline([
            ("imp", SimpleImputer(strategy="mean")),
            ("normal", StandardScaler())
        ])

        transformer = FunctionTransformer(np.log1p)
        loggable_numerical_pipeline = Pipeline([
            ("imp", SimpleImputer(strategy="mean")),
            ("log1p", transformer),
            ("normal", StandardScaler()),
        ]
        )

        boolean_pipeline = Pipeline([
            ("imp", SimpleImputer(strategy="constant", fill_value=0)),
        ])

        ct0 = ColumnTransformer([
            ("categ",categorical_pipeline,categorical_column_names),
            ("numer",numerical_pipeline,numerical_column_names),
            ("numer_lognorm",loggable_numerical_pipeline,loggable_numerical_column_names),
            ("boolean", boolean_pipeline, boolean_column_names),
        ])
        preprocessing_pipeline = ct0
        self.preprocessing_pipeline = preprocessing_pipeline
        return preprocessing_pipeline        
    def build_model_pipeline(
        self, 
        model_choice=LogisticRegression(max_iter=500),
        preprocessing_pipeline=None,
        ):
        if preprocessing_pipeline is None: preprocessing_pipeline = self.preprocessing_pipeline;
        model_pipeline = Pipeline([
                ("preprocessing", preprocessing_pipeline),
                ("model", model_choice)
        ])
        self.model_pipeline = model_pipeline
        return model_pipeline
    def fit_model(
        self,
        model_pipeline=None,
        X=None,
        y=None,
    ):
        if model_pipeline is None: model_pipeline = self.model_pipeline;
        if X is None: X = self.X_train;
        if y is None: y = self.y_train;
        np.random.seed(self.seed)
        model_pipeline.fit(X, y)
        self.model_pipeline = model_pipeline        
        return model_pipeline
    def evaluate_model(
        self, 
        model_pipeline=None, 
        X=None,
        y=None,
        ):
        if model_pipeline is None: model_pipeline = self.model_pipeline;
        if X is None: X = self.X_test;
        if y is None: y = self.y_test;
        #predictions = model_pipeline.predict(X)
        return model_pipeline.score(X, y)
    def save_model(
        self, 
        path_file_model_pipeline_pickle,
        model_pipeline=None, 
    ):
        if model_pipeline is None: model_pipeline = self.model_pipeline;        
        with open(path_file_model_pipeline_pickle, 'wb') as f:
            pickle.dump(model_pipeline, f)
            f.close()
    def execute(self):
        self.build_column_names()
        self.build_X()
        self.build_y()
        self.build_train_test()
        self.build_preprocessing_pipeline()
        self.build_model_pipeline()
        self.fit_model()
        print(self.evaluate_model())
    
def main(args):
    path_file_street_segment_landuse_traffic_volume = args["path_file_street_segment_landuse_traffic_volume"]
    temp_model_builder_object = Temp_Model_Builder(args)
    temp_model_builder_object.load_input_data(path_file_street_segment_landuse_traffic_volume)
    
    temp_model_builder_object.execute()
    
    path_file_model_pipeline_pickle = args["path_file_model_pipeline_pickle"] # os.path.join(args["path_folder"],"data","final","multinomreg.pkl")
    temp_model_builder_object.save_model(path_file_model_pipeline_pickle)
    return temp_model_builder_object
