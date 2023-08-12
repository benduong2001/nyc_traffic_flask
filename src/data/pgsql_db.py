import geopandas as gpd
import pandas as pd
import polars
import psycopg2
import sqlalchemy
import os

class Temp_PostGIS_Database_Builder:
    # requires pip install psycopg2, psycopg2-binary, SQLAlchemy, sqlalchemy2, geoalchemy2,  geopandas, polars (or pandas), connectorx, adbc-driver-postgresql
    # get psycopg2-binary, not psycopg2 https://stackoverflow.com/questions/70330567/what-is-the-different-about-psycopg2-and-psycopg2-binary-python-package
    # Creating local postgresql database in python https://www.tutorialspoint.com/python_data_access/python_postgresql_create_database.htm
    # https://gis.stackexchange.com/questions/188257/create-table-with-postgis-geometry-in-python
    # read shp file straight to postgis. concerns are how the geometry column gets renamed as geom

    # https://mapscaping.com/loading-spatial-data-into-postgis/#:~:text=One%20common%20way%20to%20load,table%20in%20a%20PostgreSQL%20database.
    
    # read in geodataframes with geopandas, then rename geometry column to geom
    # https://gis.stackexchange.com/questions/188257/create-table-with-postgis-geometry-in-python
    # https://gis.stackexchange.com/questions/239198/adding-geopandas-dataframe-to-postgis-table
    # https://stackoverflow.com/questions/65586873/geopandas-to-postgis-taking-hours

    # Writing the (Non-geospatial) polars dataframe for traffic volume to database
    # Either open csv as polars on python then add to database; or run sql statement copying csv to database directly
    # requires pip install SQLAlchemy, connectorx, adbc-driver-postgresql, polars
    # https://pola-rs.github.io/polars-book/user-guide/io/database/#adbc_1
    def __init__(self, args):
        self.args = args;
        self.set_credentials()
        self.get_engine_url()
    def set_credentials(self):
        self.pgsql_db_username = "postgres"
        self.pgsql_db_password = self.args["access"]["pgsql_db"]["password"]
        self.pgsql_db_host = "localhost"
        self.pgsql_db_port = "5432"
        self.pgsql_db_name = "nyc_data"
    def get_engine_url(self):
        engine_url = f"postgresql://{self.pgsql_db_username}:{self.pgsql_db_password}@{self.pgsql_db_host}:{self.pgsql_db_port}/{self.pgsql_db_name}"
        self.engine_url = engine_url
        return engine_url
    def get_connection_to_pgsql(self):
        conn = psycopg2.connect(user=self.pgsql_db_username, password=self.pgsql_db_password, host=self.pgsql_db_host, port=self.pgsql_db_port)
        return conn
    def get_connection_to_pgsql_db(self):
        conn = psycopg2.connect(dbname=self.pgsql_db_name, user=self.pgsql_db_username, password=self.pgsql_db_password, host=self.pgsql_db_host, port=self.pgsql_db_port)
        return conn
    def execute_sql_statement(self, sql_statement):
        conn = self.get_connection_to_pgsql_db()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql_statement)
        conn.commit()
        cursor.close()
        conn.close()
    def add_gdf_table_to_postgis_database(self, gdf, table_name):
        engine = sqlalchemy.create_engine(self.get_engine_url())
        print(f"added gdf to postgis db as new table named {table_name}")
        gdf.to_postgis(con=engine, name=table_name, if_exists = 'replace')


    def create_postgresql_database(self, args=None):
        if args is None: args = self.args;
        # https://gis.stackexchange.com/questions/188257/create-table-with-postgis-geometry-in-python
        conn = self.get_connection_to_pgsql()
        conn.autocommit = True
        cursor = conn.cursor()
        sql_create_database = """CREATE database {0}""".format(self.pgsql_db_name);
        cursor.execute(sql_create_database)
        conn.commit()
        cursor.close()
        conn.close()
        print("created database")
    def remove_postgresql_database(self, args=None):
        if args is None: args = self.args;
        # https://pythontic.com/database/postgresql/remove%20database
        conn = self.get_connection_to_pgsql()
        conn.autocommit = True
        cursor = conn.cursor()
        sql_remove_database = """REMOVE database {0}""".format(self.pgsql_db_name);
        cursor.execute(sql_remove_database)
        conn.commit()
        cursor.close()
        conn.close()
        print("removed database")
    def activate_postgis_extension(self, args=None):
        if args is None: args = self.args;
        sql_activate_postgis = """CREATE EXTENSION postgis;"""
        self.execute_sql_statement(sql_activate_postgis)
        print("activate postgis")
    def add_street_segments_gdf_to_postgis_database(self, args=None):
        if args is None: args = self.args;
        gdf = gpd.read_file(args["path_file_street_segment_shapefile"])
        table_name = "street_segments"
        self.add_gdf_table_to_postgis_database(gdf, table_name)
    def add_landuse_gdf_to_postgis_database(self, args=None):
        if args is None: args = self.args;
        gdf = gpd.read_file(args["path_file_landuse_shapefile"])
        table_name = "landuse"
        self.add_gdf_table_to_postgis_database(gdf, table_name)

    
    def add_traffic_volume_df_to_postgis_database_from_polars(self, args=None):
        # DOES NOT WORK; DO NOT USE THIS FUNCTION
        if args is None: args = self.args;
        print("read traffic volume into polars from csv")
        path_file_traffic_volume = args["path_file_traffic_volume_precompressed"]
        table_name = "traffic_volume"
        df = polars.read_csv(path_file_traffic_volume)
        print("add non-geospatial traffic volume to postgis")
        df.write_database(table_name=table_name,  connection_uri=self.get_engine_url())
    def get_sql_statement_create_empty_traffic_volume_table(self,table_name="traffic_volume"):
        if args is None: args = self.args;
        sql_create_empty_traffic_volume_table= """
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name}(
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
        return sql_create_empty_traffic_volume_table
    def add_traffic_volume_df_to_postgis_database_from_csv(self,args=None):
        # DOES NOT WORK; DO NOT USE THIS FUNCTION
        if args is None: args = self.args;
        path_file_traffic_volume = args["path_file_traffic_volume_precompressed"]
        table_name = "traffic_volume"
        sql_create_empty_traffic_volume_table = self.get_sql_statement_create_empty_traffic_volume_table(table_name=table_name)
        sql_create_empty_traffic_volume_table += f"COPY {table_name} FROM '{path_file_traffic_volume}' DELIMITER ',' CSV HEADER;"
        self.execute_sql_statement(sql_create_empty_traffic_volume_table)
        
    def add_traffic_volume_df_to_postgis_database_from_pandas_by_incremental_chunk_reading(self,args=None):
        if args is None: args = self.args;
        path_file_traffic_volume = args["path_file_traffic_volume_precompressed"]
        read_chunk_size = 10**5
        table_name = "traffic_volume"
        sql_create_empty_traffic_volume_table = self.get_sql_statement_create_empty_traffic_volume_table(table_name=table_name)
        self.execute_sql_statement(sql_create_empty_traffic_volume_table)
        engine = sqlalchemy.create_engine(self.get(engine_url))
        with pd.read_csv(path_file_traffic_volume, chunksize=read_chunk_size) as reader:
            for e, chunk in enumerate(reader):
                print(e*read_chunk_size)
                chunk.to_sql(name=table_name, con=engine,index=False,if_exists="append")
    def add_traffic_volume_df_to_postgis_database(self, args=None):
        if args is None: args = self.args;
        self.add_traffic_volume_df_to_postgis_database_from_pandas_by_incremental_chunk_reading()
    def setup(self, args=None):
        if args is None: args = self.args;
        self.create_postgresql_database()
        self.activate_postgis_extension()
        self.add_street_segments_gdf_to_postgis_database()
        self.add_landuse_gdf_to_postgis_database()
        self.add_traffic_volume_df_to_postgis_database()

    # post dbt functions (mainly for exporting tables to file):
    def export_landuse_postgis_to_geojson(self, args=None):
        # Only run this function after doing dbt run
        if args is None: args = self.args;
        # https://gis.stackexchange.com/questions/14514/exporting-feature-geojson-from-postgis
        # https://www.flother.is/til/postgis-geojson/
        # https://gis.stackexchange.com/questions/124413/how-to-export-thousands-of-features-from-postgis-to-a-single-geojson-file
        pass
    def export_street_segments_postgis_to_geojson(self, args=None):
        # Only run this function after doing dbt run
        if args is None: args = self.args;
        # https://gis.stackexchange.com/questions/14514/exporting-feature-geojson-from-postgis
        # https://www.flother.is/til/postgis-geojson/
        # https://gis.stackexchange.com/questions/124413/how-to-export-thousands-of-features-from-postgis-to-a-single-geojson-file
        pass
    def export_final_table_postgis_to_csv(self, args):
        # Only run this function after doing dbt run
        if args is None: args = self.args;
        # https://stackoverflow.com/questions/22776849/how-to-save-results-of-postgresql-to-csv-excel-file-using-psycopg2
        path_file_street_segment_landuse_traffic_volume = args["path_file_street_segment_landuse_traffic_volume"]
        table_name = "street_segment_landuse_traffic_volume"
        sql_statement = f"""SELECT * FROM {table_name}"""
        conn = self.get_connection_to_pgsql_db()
        cur = conn.cursor()
        outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)
        with open(path_file_street_segment_landuse_traffic_volume, 'w') as f:
            cur.copy_expert(outputquery, f)
        conn.close()
    def post_dbt_table_exports(self, args):
        # Only run this function after doing dbt run
        if args is None: args = self.args;
        self.export_street_segments_postgis_to_geojson();
        self.export_landuse_postgis_to_geojson();
        self.export_final_table_postgis_to_csv();

    

    # test functions; only for debugging
    def test_pgsql_fetchall(self, sql_test_statement):
        conn = self.get_connection_to_pgsql_db()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql_test_statement)
        output = cursor.fetchall()
        print(output)
        conn.commit()
        cursor.close()
        conn.close()
        return output
    def test_pgsql_fetchall_tables(self):
        sql_test_statement = """SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"""
        return self.test_pgsql_fetchall(sql_test_statement)
    def test_pgsql_fetchall_table_column_names(self, table_name):
        conn = self.get_connection_to_pgsql_db()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"""SELECT * FROM {table_name} LIMIT 1""")
        column_names = [desc[0] for desc in cursor.description]
        print(column_names)
        conn.commit()
        cursor.close()
        conn.close()
        return column_names      
    def test_pgsql_fetchall_table_row_count(self, table_name):
        sql_test_statement = f"""SELECT COUNT(*) FROM {table_name}"""
        return self.test_pgsql_fetchall(sql_test_statement) 
    def test_pgsql_fetchall_table_row(self, table_name, row_count=3, column_names="*"):
        sql_test_statement = f"""SELECT {column_names} FROM {table_name} LIMIT {row_count}"""
        return self.test_pgsql_fetchall(sql_test_statement)


def main(args):
    temp_postgis_database_builder_object = Temp_PostGIS_Database_Builder(args)
    temp_postgis_database_builder_object.setup(args)
    return temp_postgis_database_builder_object
