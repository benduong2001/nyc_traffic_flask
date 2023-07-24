import os
import json
import sys

# import src.data.etl as etl
# import src.features.data_clean as data_clean
# import src.models.model as model

class Temp_Project_Workflow:
    def __init__(self, args):
        self.args = args
    def _clear_folder(self, path_folder, exempt_files = []):
        for item in os.listdir(path_folder):
            if item in exempt_files:
                continue
            path_item = os.path.join(path_folder, item)
            if os.path.isdir(path_item):
                # is a subfolder
                path_subfolder = path_item
                self._clear_folder(path_subfolder, exempt_files)
                if len(os.listdir(path_subfolder)) == 0:
                    os.rmdir(path_subfolder) # delete the subfolder if it's empty, but don't if it still contains exempt files, 
            else:
                # is a file
                try:
                    path_file = path_item
                    os.remove(path_file)
                except:
                    #print("Failed to delete ",path_item)
                    pass
            
    def run_clear(self, args=None):
        if args is None: args = self.args;
        
        path_folder = args["path_folder"]
        exempt_files = ["stub.txt",".gitignore.txt"]
        file_name_saved_model = os.path.split(args["path_file_model_pipeline_pickle"])[1]
        # TODO: clear everything (including pickled models) in the data folder EXCEPT the un-deleted zipfile for the street segments
        self._clear_folder(os.path.join(path_folder, "data", ), exempt_files+["raw_orig_geodata_street_segment.zip",file_name_saved_model])
        # TODO: clear the geojsons in display/static folder
        self._clear_folder(os.path.join(path_folder,
                                        #"display",
                                        "static"), exempt_files)
        # TODO: clear the database in the dbt folder
        self._clear_folder(os.path.join(path_folder, "src", "features", "dbtnyc","data"), exempt_files)


        # reset the password to being blank in the dbt profiles yaml
        import yaml
        path_file_dbt_profile_yaml = args["path_file_dbt_profile_yaml"]
        with open(path_file_dbt_profile_yaml, "r") as f:
            dbt_profile_yaml = yaml.safe_load(f)
            f.close()
        dbt_profile_yaml["default"]["outputs"]["dev"]["password"] = "password"
        with open(path_file_dbt_profile_yaml, "w") as f:
            yaml.dump(dbt_profile_yaml, f)
            f.close()
        pass
        try:
            from src.data.pgsql_db import Temp_PostGIS_Database_Builder
            Temp_PostGIS_Database_Builder(args).remove_database()
        except:
            pass
    def run_data(self, args=None):
        if args is None: args = self.args;
        
        import src.data.etl as etl
        temp_dataset_builder_object = etl.main(args)
        #self.args = temp_dataset_builder_object.args;
        return temp_dataset_builder_object

    def run_features(self, args=None):
        if args is None: args = self.args;
        
        import src.features.data_clean as data_clean
        temp_dataset_preparation_object = data_clean.main(args)
        #self.args = temp_dataset_preparation_object.args;
        return temp_dataset_preparation_object

    def run_model(self, args=None):
        if args is None: args = self.args;
        
        import src.models.model as model
        temp_model_builder_object = model.main(args)
        #self.args = temp_model_builder_object.args;
        return temp_model_builder_object

    def run_setup(self, args=None):
        if args is None: args = self.args;
        
        self.run_clear(args);
        self.run_data(args);
        self.run_features(args);
        self.run_model(args);
        
    def run_app(self, args=None):
        if args is None: args = self.args;
        print("running app.py")
        try:
            import app
        except:
            from . import app

    def run_all(self, args=None):
        if args is None: args = self.args;
        
        self.run_setup(args)
        self.run_app(args)

    def run_setupdisplay(self,args=None):
        # if the cloned github repo already comes with the pickled model, there is no need to run setup; only the following steps are needed.
        # this whole function just builds the geojsons for the website html
        from src.data.etl import Temp_Dataset_Builder
        tdsb = Temp_Dataset_Builder(args)
        tdsb.etl_street_segment();
        tdsb.etl_landuse();

        from src.features.data_clean import Temp_Data_Preparation_Builder
        tdpb = Temp_Data_Preparation_Builder(args)
        path_file_street_segment_shapefile = args["path_file_street_segment_shapefile"]
        print("load street segments gdf")
        street_segment_gdf = tdpb.load_street_segments_gdf(path_file_street_segment_shapefile)
        print("project street segments gdf")
        street_segment_projected_gdf = tdpb.project_gdf(street_segment_gdf, tdpb.crs)
        print("clean street segments gdf")
        street_segment_cleaned_gdf = tdpb.street_segment_cleaned(street_segment_projected_gdf)
        path_file_street_segment_geojson = args["path_file_street_segment_geojson"]
        print("save street segments to geojson")
        tdpb.save_gdf_to_geojson(street_segment_cleaned_gdf[["Segment_ID","Number_Tra","StreetWidt","SHAPE_Leng","geometry"]],path_file_street_segment_geojson)
        
        path_file_landuse_shapefile = args["path_file_landuse_shapefile"]
        print("load landuse gdf")
        landuse_gdf = tdpb.load_landuse_gdf(path_file_landuse_shapefile)
        print("project landuse gdf")
        landuse_projected_gdf = tdpb.project_gdf(landuse_gdf, tdpb.crs)
        print("clean landuse gdf")
        landuse_cleaned_gdf = tdpb.landuse_cleaned(landuse_projected_gdf)
        path_file_landuse_geojson = args["path_file_landuse_geojson"]
        print("save landuse to geojson")
        tdpb.save_gdf_to_geojson(landuse_cleaned_gdf[['LandUse','geometry']],path_file_landuse_geojson)
        
    def run_postgresql(self, args=None):
        # create the postgis database for dbt
        if args is None: args = self.args;
        import src.data.pgsql_db as postgis_db
        temp_postgis_database_builder_object = postgis_db.main(args)
        return temp_postgis_database_builder_object
        
    def run_dbt(self, args=None):
        if args is None: args = self.args;
        import yaml

        # set the password in the dbt profiles yaml to the password in the access json (currently set to None for safety)
        pgsql_db_password = args["access"]["pgsql_db"]["password"]
        path_file_dbt_profile_yaml = args["path_file_dbt_profile_yaml"]
        with open(path_file_dbt_profile_yaml, "r") as f:
            dbt_profile_yaml = yaml.safe_load(f)
            f.close()
        dbt_profile_yaml["default"]["outputs"]["dev"]["password"] = pgsql_db_password
        with open(path_file_dbt_profile_yaml, "w") as f:
            yaml.dump(dbt_profile_yaml, f)
            f.close()

        #from src.data.pgsql_db import Temp_PostGIS_Database_Builder
        #tpgdbb = Temp_PostGIS_Database_Builder(args)
        # run dbt
    def dag(self, args=None):
        if args is None: args = self.args;

        from airflow import DAG
        from airflow.operators.bash_operator import BashOperator
        from airflow.operators.python_operator import PythonOperator
        from datetime import datetime

        from src.data.etl import Temp_Dataset_Builder
        from src.data.pgsql_db import Temp_PostGIS_Database_Builder
        from src.models.model import Temp_Model_Builder

        tdsb = Temp_Dataset_Builder(args)
        tpgdbb = Temp_PostGIS_Database_Builder(args)
        tmb = Temp_Model_Builder(args)

        default_args = {
            'owner': 'my_user',
            'start_date': datetime(2022, 1, 1),
            'retries': 1,
            #'retry_delay': datetime.timedelta(minutes=5)
        }

        dag = DAG('nyc_dag', default_args=default_args, schedule_interval=None)

        task_clear = PythonOperator(task_id="clear",python_callable=self.run_clear,dag=dag)
        task_data = PythonOperator(task_id="data",python_callable=tdsb.etl,dag=dag)
        task_postgresql = PythonOperator(task_id="postgresql",python_callable=tpgdbb.setup,dag=dag)
        task_dbt = PythonOperator(task_id="postgresql",python_callable=self.run_dbt,dag=dag)
        task_post_dbt = PythonOperator(task_id="postgresql",python_callable=tpgdbb.post_dbt_table_exports,dag=dag)
        task_model = PythonOperator(task_id="postgresql",python_callable=tmb.execute,dag=dag)

        task_clear >> task_data >> task_postgresql >> task_dbt >> task_post_dbt >> task_model
        
    def run(self, targets, args=None):
        if args is None: args = self.args;
        # 

        for target in targets:
            if target in ["clear"]:
                self.run_clear(args)
            if target in ["data"]:
                self.run_data(args)
            if target in ["features"]:
                self.run_features(args)
            if target in ["model"]:
                self.run_model(args)
            if target in ["setup"]:
                self.run_setup(args)
            if target in ["app"]:
                self.run_app(args)
            if target in ["all"]:
                self.run_all(args)
            if target in ["setupdisplay"]:
                self.run_setupdisplay(args)
            if target in ["dag"]:
                self.run_dag(args)
    
def main(targets):
    import build_configs;
    configs = build_configs.main(); # TODO, comment out; put access.json and build_jsons in gitignore

    path_folder = os.getcwd()
    file_name_configs = "configs.json"
    path_file_configs = os.path.join(path_folder, file_name_configs)
    with open(path_file_configs, "r") as f:
        configs = json.load(f)
        f.close()

    #print(configs)

    args = configs
    args["path_folder"] = path_folder

    import build_access; 
    file_name_access = "access.json"
    path_file_access = os.path.join(path_folder, file_name_access)
    if os.path.exists(path_file_access):
        pass
    else:
       access = build_access.main();

    file_name_access = "access.json"
    path_file_access = os.path.join(path_folder, file_name_access)
    with open(path_file_access, "r") as f:
        access = json.load(f)
        f.close()
    args["access"] = access
        
    temp_project_workflow = Temp_Project_Workflow(args)
    temp_project_workflow.run(targets)

if __name__ == "__main__":
    targets = sys.argv[1:]
    #targets = ["all"]
    main(targets)

