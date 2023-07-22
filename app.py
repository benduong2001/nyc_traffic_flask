import os
from flask import Flask, request, render_template
import pandas as pd
import pickle
import joblib
import numpy as np

import sys
#sys.path.append("..")
from src.predict import make_prediction



# Declare a Flask app
app = Flask(__name__)

# Main function here
@app.route('/', methods=['GET', 'POST'])
def main():
    
    # If a form is submitted
    if request.method == "POST":
        
        ## # Unpickle classifier
        # model = joblib.load("../data/final/dtr.pkl")


        
        # Get values through input bars
        #print(request.form)
        
        traffic_inputs = request.form.to_dict()
        #time based features (works for all 3 versions of gdf)
        traffic_inputs["HH"] = int(traffic_inputs["Hour"].split(":")[0])
        traffic_inputs.pop("Hour")
        traffic_inputs["IsWeekend"] = int("IsWeekend" in traffic_inputs)
        for key in traffic_inputs:
            traffic_inputs[key] = float(traffic_inputs[key])
        traffic_inputs["LandUse_NULL"] = traffic_inputs["LandUse_00"]
        traffic_inputs.pop("LandUse_00")
        
        #print(traffic_inputs)        
        
        prediction_output = make_prediction(traffic_inputs)
        if prediction_output == "ERROR OUT OF BOUNDS":
            html_output_value = "ERROR: Re-select coordinates inside of NYC boundaries"
        else:
            html_output_value = "Predicted Traffic Volume Level: {0}".format(str(prediction_output))
            #make_prediction = 
    else:
        html_output_value = ""
        
    return render_template("website.html", output = html_output_value)

# Running the app
if __name__ == '__main__':
    with_host_port = 1;
    # if using dockerfile that already has geojsons stored, set with_host_port to 1
    if with_host_port == 1:
        port = int(os.environ.get('PORT', 5000))
        app.run(debug=True, host='0.0.0.0', port=port)
    else:
        app.run(debug = True)
