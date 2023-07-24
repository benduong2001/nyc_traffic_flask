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
        pass

    def run_data(self, args=None):
        if args is None: args = self.args;
        
        import src.data.etl as etl
        temp_dataset_builder_object = etl.main(args)
        #self.args = temp_dataset_builder_object.args;

    def run_features(self, args=None):
        if args is None: args = self.args;
        
        import src.features.data_clean as data_clean
        temp_dataset_preparation_object = data_clean.main(args)
        #self.args = temp_dataset_preparation_object.args;


    def run_model(self, args=None):
        if args is None: args = self.args;
        
        import src.models.model as model
        temp_model_builder_object = model.main(args)
        #self.args = temp_model_builder_object.args;

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
        
        
    def run_temppgsqldb(self, args=None):
        if args is None: args = self.args;
        



        # requires pip install psycopg2, psycopg2-binary, SQLAlchemy, sqlalchemy2, geoalchemy2,  geopandas, polars (or pandas), connectorx, adbc-driver-postgresql
        # get psycopg2-binary, not psycopg2 https://stackoverflow.com/questions/70330567/what-is-the-different-about-psycopg2-and-psycopg2-binary-python-package
        import psycopg2
        import sqlalchemy

        


        # Creating local postgresql database in python https://www.tutorialspoint.com/python_data_access/python_postgresql_create_database.htm

        args["access"]["pgsql_db"] = {"password": "R0mce+on1"}
        args["pgsql_db_name"] = "nyc_data"
        path_file_dbt_profile_yaml = args["path_file_dbt_profile_yaml"]
        with open(path_file_dbt_profile_yaml, "r") as f:
            
        pgsql_db_password = args["access"]["pgsql_db"]["password"]
        pgsql_db_name = args["pgsql_db_name"]
        pgsql_db_username = "postgres"
        pgsql_db_host = "localhost"
        pgsql_db_port = "5432"

        first_time = 0

        if first_time == 1:
            # https://gis.stackexchange.com/questions/188257/create-table-with-postgis-geometry-in-python
            conn = psycopg2.connect(user=pgsql_db_username, password=pgsql_db_password, host=pgsql_db_host, port= pgsql_db_port)
            conn.autocommit = True
            cursor = conn.cursor()
            sql_create_database = """CREATE database {0}""".format(pgsql_db_name);
            cursor.execute(sql_create_database)
            conn.close()

            print("created database")

            
            conn = psycopg2.connect(dbname=pgsql_db_name, user=pgsql_db_username, password=pgsql_db_password, host=pgsql_db_host, port= pgsql_db_port)
            conn.autocommit = True
            cursor = conn.cursor()
            sql_activate_postgis = """CREATE EXTENSION postgis;"""
            cursor.execute(sql_activate_postgis)
            conn.commit()
            cursor.close()
            conn.close()

            print("activate postgis")

        # read shp file straight to postgis. concerns are how the geometry column gets renamed as geom

        # https://mapscaping.com/loading-spatial-data-into-postgis/#:~:text=One%20common%20way%20to%20load,table%20in%20a%20PostgreSQL%20database.
    
        # read in geodataframes with geopandas, then rename geometry column to geom
        # https://gis.stackexchange.com/questions/188257/create-table-with-postgis-geometry-in-python
        import geopandas as gpd
        # https://gis.stackexchange.com/questions/239198/adding-geopandas-dataframe-to-postgis-table
        # https://stackoverflow.com/questions/65586873/geopandas-to-postgis-taking-hours
        first_time = 0

        if first_time == 1:
            street_segment_gdf = gpd.read_file(args["path_file_street_segment_shapefile"])
            print("read street segments gdf from shp file")
            #engine_url = "postgresql://username:password@localhost:5432/"+name_Database
            engine_url = f"postgresql://{pgsql_db_username}:{pgsql_db_password}@{pgsql_db_host}:{pgsql_db_port}/{pgsql_db_name}"
            engine = sqlalchemy.create_engine(engine_url)
            print("read street segments gdf to postgis")
            street_segment_gdf.to_postgis(con=engine, name='street_segments', if_exists = 'replace')

        first_time = 0

        if first_time == 1:
            print("test street segments postgis")
            conn = psycopg2.connect(dbname=pgsql_db_name, user=pgsql_db_username, password=pgsql_db_password, host=pgsql_db_host, port= pgsql_db_port)
            conn.autocommit = True
            cursor = conn.cursor()
            sql_test_postgis_upload = """SELECT * FROM street_segments LIMIT 5;"""
            cursor.execute(sql_test_postgis_upload)
            column_names = [desc[0] for desc in cursor.description]
            print("column names are:", column_names)
            cursor.close()
            conn.close()

            print("viewing geometries")
            conn = psycopg2.connect(dbname=pgsql_db_name, user=pgsql_db_username, password=pgsql_db_password, host=pgsql_db_host, port= pgsql_db_port)
            conn.autocommit = True
            cursor = conn.cursor()
            try:
                sql_test_postgis_geom = "SELECT geom FROM street_segments LIMIT 5;"
                cursor.execute(sql_test_postgis_geom)
                print(cursor.fetchall())
            except:
                try:
                    sql_test_postgis_geom = "SELECT geometry FROM street_segments LIMIT 5;"
                    cursor.execute(sql_test_postgis_geom)
                    print(cursor.fetchall())
                except:
                    pass
            cursor.close()
            conn.close()
        
        first_time = 0

        if first_time == 1:        
        
            landuse_gdf = gpd.read_file(args["path_file_landuse_shapefile"])
            print("read landuse gdf from shp file")
            #engine_url = "postgresql://username:password@localhost:5432/"+name_Database
            engine_url = f"postgresql://{pgsql_db_username}:{pgsql_db_password}@{pgsql_db_host}:{pgsql_db_port}/{pgsql_db_name}"
            engine = sqlalchemy.create_engine(engine_url)
            print("read landuse_gdf to postgis")
            landuse_gdf.to_postgis(con=engine, name='landuse', if_exists = 'replace')


        # Writing the (Non-geospatial) polars dataframe for traffic volume to database
        # Either open csv as polars on python then add to database; or run sql statement copying csv to database directly
        # requires pip install SQLAlchemy, connectorx, adbc-driver-postgresql, polars
        # https://pola-rs.github.io/polars-book/user-guide/io/database/#adbc_1
        first_time = 0
        if first_time == 1:
            import polars
            print("read traffic volume into polars from csv")
            path_file_traffic_volume = args["path_file_traffic_volume_precompressed"]
            df = polars.read_csv(path_file_traffic_volume)
            connection_uri = f"postgresql://{pgsql_db_username}:{pgsql_db_password}@{pgsql_db_host}:{pgsql_db_port}/{pgsql_db_name}"
            print("add non-geospatial traffic volume to postgis")
            df.write_database(table_name="traffic_volume",  connection_uri=connection_uri)
        first_time = 0
        if first_time == 1:
            print("add non-geospatial traffic volume to postgis via csv")
            conn = psycopg2.connect(dbname=pgsql_db_name, user=pgsql_db_username, password=pgsql_db_password, host=pgsql_db_host, port= pgsql_db_port)
            conn.autocommit = True
            cursor = conn.cursor()
            path_file_traffic_volume = args["path_file_traffic_volume_precompressed"]
            sql_copy_csv_to_postgis = f"""
            DROP TABLE IF EXISTS traffic_volume;
            CREATE TABLE traffic_volume(
            RequestID integer,
            Boro varchar(15),
            Yr integer,
            M integer,
            D integer,
            HH integer,
            MM integer,
            Vol integer,
            SegmentID integer,
            WktGeom varchar(5),
            street varchar(5),
            fromSt varchar(5),
            toSt varchar(5),
            Direction varchar(5)
            );
            COPY traffic_volume FROM '{path_file_traffic_volume}' DELIMITER ',' CSV HEADER;
            """
            cursor.execute(sql_copy_csv_to_postgis)
            print("execute non-geospatial traffic volume to postgis via csv")
            conn.commit()
            print("commit non-geospatial traffic volume to postgis via csv")
            cursor.close()
            conn.close()
        first_time = 0
        if first_time == 1:
            print("add non-geospatial traffic volume to postgis via csv")
            conn = psycopg2.connect(dbname=pgsql_db_name, user=pgsql_db_username, password=pgsql_db_password, host=pgsql_db_host, port= pgsql_db_port)
            conn.autocommit = True
            cursor = conn.cursor()
            path_file_traffic_volume = args["path_file_traffic_volume_precompressed"]
            sql_copy_csv_to_postgis = f"""
            DROP TABLE IF EXISTS traffic_volume;
            CREATE TABLE traffic_volume(
            RequestID integer,
            Boro varchar(15),
            Yr integer,
            M integer,
            D integer,
            HH integer,
            MM integer,
            Vol integer,
            SegmentID integer,
            WktGeom varchar(5),
            street varchar(5),
            fromSt varchar(5),
            toSt varchar(5),
            Direction varchar(5)
            );
            COPY traffic_volume FROM '{path_file_traffic_volume}' DELIMITER ',' CSV HEADER;
            """
            cursor.execute(sql_copy_csv_to_postgis)
            print("execute non-geospatial traffic volume to postgis via csv")
            conn.commit()
            print("commit non-geospatial traffic volume to postgis via csv")
            cursor.close()
            conn.close()
        first_time = 0
        if first_time == 1:
            print("add empty non-geospatial traffic volume to postgis")
            conn = psycopg2.connect(dbname=pgsql_db_name, user=pgsql_db_username, password=pgsql_db_password, host=pgsql_db_host, port= pgsql_db_port)
            conn.autocommit = True
            cursor = conn.cursor()
            sql_add_empty_table_to_postgis = """
            DROP TABLE IF EXISTS traffic_volume;
            CREATE TABLE traffic_volume(
            "RequestID" integer,
            "Boro" varchar,
            "Yr" integer,
            "M" integer,
            "D" integer,
            "HH" integer,
            "MM" integer,
            "Vol" integer,
            "SegmentID" integer,
            "WktGeom" varchar,
            "street" varchar,
            "fromSt" varchar,
            "toSt" varchar,
            "Direction" varchar
            );
            """
            cursor.execute(sql_add_empty_table_to_postgis)
            conn.commit()
            cursor.close()
            conn.close()  
            
            conn = psycopg2.connect(dbname=pgsql_db_name, user=pgsql_db_username, password=pgsql_db_password, host=pgsql_db_host, port= pgsql_db_port)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM traffic_volume;")
            column_names = [desc[0] for desc in cursor.description]
            print("column names are:", column_names)
            
            engine_url = f"postgresql://{pgsql_db_username}:{pgsql_db_password}@{pgsql_db_host}:{pgsql_db_port}/{pgsql_db_name}"
            engine = sqlalchemy.create_engine(engine_url)
            
            path_file_traffic_volume = args["path_file_traffic_volume_precompressed"]
            read_chunk_size = 10**5
            

            #sql_chunk_row_insertion = """
            #INSERT INTO traffic_volume ('RequestID', 'Boro', 'Yr', 'M', 'D', 'HH', 'MM', 'Vol', 'SegmentID', 'WktGeom', 'street', 'fromSt', 'toSt', 'Direction')
            #VALUES 
            #"""
            dtype_dict = {
            "RequestID": sqlalchemy.types.INTEGER,
            "Boro": sqlalchemy.types.VARCHAR(length=20),
            "Yr": sqlalchemy.types.INTEGER,
            "M": sqlalchemy.types.INTEGER,
            "D": sqlalchemy.types.INTEGER,
            "HH": sqlalchemy.types.INTEGER,
            "MM": sqlalchemy.types.INTEGER,
            "Vol": sqlalchemy.types.INTEGER,
            "SegmentID": sqlalchemy.types.INTEGER,
            "WktGeom": sqlalchemy.types.VARCHAR(length=20),
            "street": sqlalchemy.types.VARCHAR(length=20),
            "fromSt": sqlalchemy.types.VARCHAR(length=20),
            "toSt": sqlalchemy.types.VARCHAR(length=20),
            "Direction": sqlalchemy.types.VARCHAR(length=5),
                }
            import pandas as pd
            with pd.read_csv(path_file_traffic_volume, chunksize=read_chunk_size) as reader:
                for e, chunk in enumerate(reader):
                    print(e*read_chunk_size)
                    
                    chunk.to_sql(name="traffic_volume", con=engine,index=False,if_exists="append")#dtype=dtype_dict, method='multi')#,chunksize=write_chunk_size)
                    #inserted_values = ""
                    #for row in (chunk.itertuples(index=False, name=None)):
                    #    inserted_values = " " + inserted_values + (str(row) + ",")
                    #inserted_values[-1] = ";" # change the last character (a trailing comma) into a semicolon
                    #sql_chunk_row_insertion_temp = sql_chunk_row_insertion + " " + inserted_values
                    #cursor.execute(sql_chunk_row_insertion_temp)
                    #conn.commit()
                    

            
            cursor.close()
            conn.close()                    
                    
        
        first_time = 0
        if first_time == 1:
            print("test traffic_volume postgis")
            conn = psycopg2.connect(dbname=pgsql_db_name, user=pgsql_db_username, password=pgsql_db_password, host=pgsql_db_host, port= pgsql_db_port)
            conn.autocommit = True
            cursor = conn.cursor()
            sql_test_postgis_upload = """SELECT COUNT(*) FROM traffic_volume;"""
            cursor.execute(sql_test_postgis_upload)
            print(cursor.fetchall())
            sql_test_postgis_upload = """SELECT * FROM traffic_volume LIMIT 3;"""
            cursor.execute(sql_test_postgis_upload)
            print(cursor.fetchall())
            cursor.close()
            conn.close()     
        first_time = 1
        if first_time == 1:
            conn = psycopg2.connect(dbname=pgsql_db_name, user=pgsql_db_username, password=pgsql_db_password, host=pgsql_db_host, port= pgsql_db_port)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
            """)

            # Fetch all the table names
            table_names = cursor.fetchall()

            # Print the table names
            for table_name in table_names:
                print(table_name[0])

            cursor.execute("SELECT COUNT(*) FROM traffic_volume_cleaned;")
            print(cursor.fetchall())
            cursor.execute("SELECT COUNT(*) FROM landuse_cleaned;")            
            print(cursor.fetchall())
            cursor.execute("SELECT COUNT(*) FROM street_segment_cleaned;")            
            print(cursor.fetchall())

            cursor.close()
            conn.close()
        
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
            if target in ["temppgsqldb"]:
                self.run_temppgsqldb(args)
    
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

