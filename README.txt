
conda create --name geoenv
conda activate geoenv
conda install -c conda-forge scikit-learn=1.0.2
conda install -c conda-forge geopandas
conda install -c conda-forge werkzeug=2.0.3
conda install -c conda-forge flask=2.1.0

cd C:\Users\Countlinard\nyc_traffic_flask\display\
python app.py
copy http link in the command output and open in browser
