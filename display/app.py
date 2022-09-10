import os
print(os.path.dirname(os.path.abspath(__file__)))
from flask import Flask, request, render_template
import pandas as pd
import pickle
import joblib
import numpy as np

import sys
sys.path.append("..")
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
        hour = request.form.get("hour")
        is_weekend = request.form.get("is_weekend")
        latitude = request.form.get("latitude")
        longitude = request.form.get("longitude")
        
        ## # Put inputs to dataframe
        ## X = pd.DataFrame([[height, weight]], columns = ["Height", "Weight"])
        
        ## # Get prediction
        ## prediction = clf.predict(X)[0]
        hour = int(hour.split(":")[0])
        is_weekend = int(hour=="on")
        x_coord = float(longitude)
        y_coord = float(latitude)
        
        
        # prediction = str(hour) + " " + str(is_weekend) + " " + str(x_coord) + " " + str(y_coord)
        prediction_output = make_prediction(hour, is_weekend, x_coord, y_coord)
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
    app.run(debug = True)
