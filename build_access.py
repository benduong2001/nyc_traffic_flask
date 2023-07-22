import os
import json
def main(path_folder=None):
    if path_folder is None:
        path_folder = os.getcwd()

    access = {"socrata":
              {"apptoken": None
               ,
              "username": None
               ,
              "password": None
               ,
               },
              "gdrive_links":
              {"zipped_street_segment_shapefile_google_drive_url": None
               ,
               "zipped_landuse_shapefile_google_drive_url": None
               ,
               "traffic_volume_google_drive_url": None
              ,
              }}
    file_name_access = "access.json"
    path_file_access = os.path.join(path_folder, file_name_access)
    try:
        with open(path_file_access, "w") as f:
            json.dump(access, f)
            f.close()
    except:
        pass
    return access



