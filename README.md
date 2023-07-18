# nyc_traffic_flask





<!--https://github.com/benduong2001/nyc_traffic_flask/assets/62922098/1578a46e-bdea-4f19-803f-51bc74ceb048-->
![](assets/nyc_traffic_optimized4.gif)




# NYC Traffic Prediction 
* This project builds a Flask App with leaflet.js to make predictions of NYC Traffic using machine learning.
* This is a sub-project of this older kaggle project: https://github.com/benduong2001/ArcGIS_Project_nyc_traffic


### Setup with Conda

Get a new conda environment up and running and run as follows
```
conda create --name geoenv python=3.11
conda activate geoenv
conda install -c conda-forge scikit-learn=1.0.2
conda install -c conda-forge geopandas
conda install -c conda-forge werkzeug=2.0.3
conda install -c conda-forge flask=2.1.0
conda install -c conda-forge geojson -y
conda install -c conda-forge tqdm -y
conda install -c conda-forge gdown -y
conda install -c conda-forge statsmodels
conda install -c anaconda beautifulsoup4 -y
pip install sodapy
pip install polars
pip install pyarrow
```
Activate the environment; fork the project locally and run the run.py file with argument "setup" one time only
```
conda activate geoenv
git clone https://github.com/benduong2001/nyc_traffic_flask.git
cd C:/Users/Benson/nyc_traffic_flask/
python run.py setup
conda deactivate
```
After that, all that is needed is to run the app.py inside the display directory.
```
conda activate geoenv
cd C:/Users/Benson/nyc_traffic_flask/display
python app.py
```

### Setup with Docker

**Docker Image**: bensonduong/nyc_traffic:latest
