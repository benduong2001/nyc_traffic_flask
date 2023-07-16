import geopandas as gpd
import pandas as pd
import numpy as np
import pickle
import os
import logging
import zipfile
import io
import json
import shutil
import requests


import gdown
import sodapy
from sodapy import Socrata
import polars
import pyarrow
import geojson
import tqdm
from bs4 import BeautifulSoup

class Temp_Dataset_Builder:
    def __init__(self, args):
        self.args = args;

    def _collapse_nested_extracted_zip_folder(self, path_folder):
        # sometimes, extracting the zipped file with produce nested duplicates of the unzipped folder
        # i.e. my_folder_name.zip becomes "my_folder_name/my_folder_name/...my_folder_name/my_folder_name.shp"
        # this code below gets rid of the the nested duplicate parent subfolders.
        folder_name = os.path.split(path_folder)[-1]
        base_dir = path_folder
        curr_dir = base_dir
        while (folder_name in os.listdir(curr_dir)):
            curr_dir = os.path.join(curr_dir, folder_name)
        for file_name in os.listdir(curr_dir):
            shutil.move( os.path.join(curr_dir, file_name),  os.path.join(base_dir, file_name))
        os.rmdir(curr_dir)

    def _download_file_from_google_drive_share_link_to_file_path(self, url, path_file):
        gdown.download(url=url, output=path_file, quiet=False, fuzzy=True)        

    def _download_zipped_file_from_google_drive_share_link_to_folder_path(self, url, path_zipped_file, path_folder):
        args = self.args
        
        self._download_file_from_google_drive_share_link_to_file_path(url, path_zipped_file)
        with zipfile.ZipFile(path_zipped_file,"r") as f:
            f.extractall(path_folder)
            f.close()
        os.remove(path_zipped_file)
        self._collapse_nested_extracted_zip_folder(path_folder)

    def _download_extract_shapefile_href_to_folder(self, shapefile_download_href, path_folder_shapefile):
        r = requests.get(shapefile_download_href)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(path_folder_shapefile)

    def _download_extract_href_to_file(self, download_href, path_file):
        r = requests.get(download_href)
        with open(path_file, 'wb') as f:
            f.write(r.content)
            f.close()


    def _retrieve_records_from_api(self, client, data_set,):
        row_amount_info = client.get(data_set, select="COUNT(*)") # 27414481
        row_amount = int(row_amount_info[0]["COUNT"])
        #row_amount = 5700
        chunk_size = 1000
        chunk_amount = -((-row_amount) // (chunk_size))
        records = []
        for i in tqdm.tqdm(range(chunk_amount)):
            records.extend( client.get(data_set, offset=(i*chunk_size), limit=chunk_size))
        return records
        
            
    def etl_street_segment_gdrive(self, args=None):
        if args is None: args = self.args;
        path_folder_street_segment_shapefile = args["path_folder_street_segment_shapefile"]
        zipped_street_segment_shapefile_google_drive_url = args["access"]["gdrive_links"]["zipped_street_segment_shapefile_google_drive_url"]
        path_zipped_folder_street_segment_shapefile = path_folder_street_segment_shapefile + ".zip"
        self._download_zipped_file_from_google_drive_share_link_to_folder_path(
            zipped_street_segment_shapefile_google_drive_url,
            path_zipped_folder_street_segment_shapefile,
            path_folder_street_segment_shapefile,
        )
        pass
    def etl_street_segment(self, args=None):
        if args is None: args = self.args;
        # etl_street_segment_cloud
        # etl_street_segment_gdrive
        print("street_segment")
        # args["path_folder_street_segment_shapefile"] = os.path.join(args["path_folder"],"data","raw","raw_orig_geodata","raw_orig_geodata_street_segment"); self.args = args; # TODO: can be taken uplevel, thus avoiding needing to update args with self.args = args;

        path_folder_street_segment_shapefile = args["path_folder_street_segment_shapefile"]
        try:
            # assumes street segments data is already provided in the cloned repo in the form of a zipped folder / does not need to be retrieved from online
            path_zipped_folder_street_segment_shapefile = path_folder_street_segment_shapefile + ".zip"
            assert os.path.exists(path_zipped_folder_street_segment_shapefile)
            with zipfile.ZipFile(path_zipped_folder_street_segment_shapefile,"r") as f:
                f.extractall(path_folder_street_segment_shapefile)
                f.close()
                # os.remove(path_zipped_folder_street_segment_shapefile) # keep this line commented out; Don't remove the zipfile after extracting it to a new folder, so it can be reused when run_clear in run.py is executed
                self._collapse_nested_extracted_zip_folder(path_folder_street_segment_shapefile) 
        except:
            try:
                print("trying gdrive")
                self.etl_street_segment_gdrive(args)
            except:
                print("all failed")
                pass
        pass


    def etl_landuse_bs4(self, args=None):
        if args is None: args = self.args;
        path_folder_landuse_shapefile = args["path_folder_landuse_shapefile"]
        url_shapefile_location = "https://www.nyc.gov/site/planning/data-maps/open-data/dwn-pluto-mappluto.page"
        page = requests.get(url_shapefile_location)
        soup = BeautifulSoup(page.text, 'html.parser')
        #shapefile_download_href = get_landuse_shapefile_download_href(soup)
        shapefile_download_href = "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nyc_mappluto_23v1_2_shp.zip"
        self._download_extract_shapefile_href_to_folder(shapefile_download_href, path_folder_landuse_shapefile)
        pass
    def etl_landuse_gdrive(self, args=None):
        if args is None: args = self.args;
        path_folder_landuse_shapefile = args["path_folder_landuse_shapefile"]
        zipped_landuse_shapefile_google_drive_url = args["access"]["gdrive_links"]["zipped_landuse_shapefile_google_drive_url"]
        path_zipped_landuse_shapefile = path_folder_landuse_shapefile + ".zip"
        self._download_zipped_file_from_google_drive_share_link_to_folder_path(
            zipped_landuse_shapefile_google_drive_url,
            path_zipped_landuse_shapefile,
            path_folder_landuse_shapefile,
        )
        pass
    def etl_landuse(self, args=None):
        if args is None: args = self.args;
        # etl_landuse_bs4
        # etl_landuse_cloud
        # etl_landuse_gdrive
        print("landuse")
        # args["path_folder_landuse_shapefile"] = os.path.join(args["path_folder"],"data","raw","raw_orig_geodata","landuse"); self.args = args; # TODO: can be taken uplevel, thus avoiding needing to update args with self.args = args;
        try:
            print("trying bs4")
            self.etl_landuse_bs4(args)
        except:
            try:
                print("trying gdrive")
                self.etl_landuse_gdrive(args)
            except:
                print("all failed")
                pass
        pass

    def etl_traffic_volume_api(self, args=None):
        if args is None: args = self.args;
        #assert False
        access = args["access"]["socrata"]
        path_file_traffic_volume = args["path_file_traffic_volume_precompressed"]
        data_url ='data.cityofnewyork.us'
        data_set = '7ym2-wayt'
        client = Socrata(data_url,
                        access["apptoken"],
                        username=access["username"],
                        password=access["password"],
                        )
        records = self._retrieve_records_from_api(client, data_set)
        df = pd.DataFrame.from_records(records)
        records.to_csv(path_file_traffic_volume,index=False)

    def etl_traffic_volume_bs4(self, args=None):
        if args is None: args = self.args;
        path_file_traffic_volume = args["path_file_traffic_volume_precompressed"]
        url_file_location = "https://data.cityofnewyork.us/Transportation/Automated-Traffic-Volume-Counts/7ym2-wayt"
        page = requests.get(url_file_location)
        soup = BeautifulSoup(page.text, 'html.parser')
        #download_href = get_traffic_volume_file_download_href(soup)
        download_href = "https://data.cityofnewyork.us/api/views/7ym2-wayt/rows.csv?accessType=DOWNLOAD"
        self._download_extract_href_to_file(download_href, path_file_traffic_volume)
        pass
    def etl_traffic_volume_gdrive(self, args=None):
        if args is None: args = self.args;
        traffic_volume_google_drive_url = args["access"]["gdrive_links"]["traffic_volume_google_drive_url"]
        path_file_traffic_volume = args["path_file_traffic_volume_precompressed"]
        self._download_file_from_google_drive_share_link_to_file_path(traffic_volume_google_drive_url, path_file_traffic_volume)
        pass

    def etl_traffic_volume(self, args=None):
        if args is None: args = self.args;
        # etl_traffic_volume_api
        # etl_traffic_volume_bs4
        # etl_traffic_volume_cloud
        # etl_traffic_volume_gdrive
        print("traffic_volume")
        # args["path_file_traffic_volume"] = os.path.join(args["path_folder"],"data","raw","raw_orig_data","traffic_volume.csv") # TODO: can be taken uplevel, thus avoiding needing to update args with self.args = args;
        try:
            print("trying API")
            self.etl_traffic_volume_api(args)
        except:
            try:
                print("trying bs4")
                self.etl_traffic_volume_bs4(args)
            except:
                try:
                    print("trying gdrive")
                    self.etl_traffic_volume_gdrive(args)
                except:
                    print("all failed")
                    pass
        
        # replace the csv with a parquet for compression
        path_file_traffic_volume = args["path_file_traffic_volume_precompressed"]
        df = polars.read_csv(path_file_traffic_volume)
        os.remove(path_file_traffic_volume)
        path_file_traffic_volume = args["path_file_traffic_volume"] # os.path.join(args["path_folder"],"data","raw","raw_orig_data","traffic_volume.parquet")
        # args["path_file_traffic_volume"] = path_file_traffic_volume; self.args = args; # TODO: can be taken uplevel, thus avoiding needing to update args with self.args = args;
        df.write_parquet(path_file_traffic_volume)
        pass

    def etl(self, args=None):
        if args is None:
            args = self.args;
        self.etl_street_segment()
        self.etl_traffic_volume()
        self.etl_landuse()

def main(args):
    temp_dataset_builder_object = Temp_Dataset_Builder(args)
    temp_dataset_builder_object.etl(args)
    return temp_dataset_builder_object
