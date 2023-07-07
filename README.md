# nyc_traffic_flask



https://github.com/benduong2001/nyc_traffic_flask/assets/62922098/ad5be968-6b06-47b2-816d-6cba730b8df8



# NYC Traffic Prediction 
* This project builds a Flask App with leaflet.js to make predictions of NYC Traffic using machine learning.
* This is a sub-project of this older kaggle project: https://github.com/benduong2001/ArcGIS_Project_nyc_traffic


### Setup

Get the conda environment up and running and run as follows
```
conda create --name geoenv
conda activate geoenv
conda install -c conda-forge scikit-learn=1.0.2
conda install -c conda-forge geopandas
conda install -c conda-forge werkzeug=2.0.3
conda install -c conda-forge flask=2.1.0
```

**Docker Image**: bensonduong/authry:latest

After forking the project, some specific steps needs to be manually done:
* **"/data/raw/raw_orig_geodata/"** should have 2 subfolders of GIS Data. One is already provided but in zipped form. The other is too big for Github (~1GB), and needs to be manually downloaded.
  * Go to https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-pluto-mappluto.page and download "MapPLUTO - Shoreline Clipped (Shapefile)". 
  * Extract and rename the folder as **"raw_orig_geodata_landuse"**, and put it in the folder **"/data/raw/raw_orig_geodata/"**
* Extract the already-zipped folder /data/raw/raw_orig_geodata/raw_orig_geodata_street_segment.zip
* So in the end, the folder **"/data/raw/raw_orig_geodata/"** should have 2 unzipped subfolders: **"/raw_orig_geodata_landuse/"** and **"/raw_orig_geodata_street_segment/"**

* We are now ready to run the flask app. In your conda env,
```
cd C:\Users\Benson\nyc_traffic_flask\display\
python app.py
```
Copy the output http link to browser to use the Flask app
