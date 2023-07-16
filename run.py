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
        # TODO: clear everything (including pickled models) in the data folder EXCEPT the un-deleted zipfile for the street segments
        self._clear_folder(os.path.join(path_folder, "data", ), exempt_files+["raw_orig_geodata_street_segment.zip"])
        # TODO: clear the geojsons in display/static folder
        self._clear_folder(os.path.join(path_folder, "display", "static"), exempt_files)
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
        
        import display.app as app

    def run_all(self, args=None):
        if args is None: args = self.args;
        
        self.run_setup(args)
        self.run_app(args)
        
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
def main(targets):
    import build_configs; build_configs.main(); # TODO, comment out; put access.json and build_jsons in gitignore
    import build_access; build_access.main(); # TODO, comment out; put access.json and build_jsons in gitignore

    path_folder = os.getcwd()
    file_name_configs = "configs.json"
    path_file_configs = os.path.join(path_folder, file_name_configs)
    with open(path_file_configs, "r") as f:
        configs = json.load(f)
        f.close()

    #print(configs)

    args = configs
    args["path_folder"] = path_folder

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

