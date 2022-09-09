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
        

        with open('../data/temp/saved_zscore_traffic.pkl', 'rb') as f:
            saved_zscore_traffic = pickle.load(f)
            f.close()
        
        # prediction = str(hour) + " " + str(is_weekend) + " " + str(x_coord) + " " + str(y_coord)
        prediction = make_prediction(hour, is_weekend, x_coord, y_coord)
        quantity = int(prediction)
        relative_level = ((np.log1p(prediction) - saved_zscore_traffic["mean"])/saved_zscore_traffic["std"])
        prediction = "Predicted Traffic Volume: {0} cars, Relative Level: {1},".format(str(quantity),str(np.round(relative_level, 2)))
        #make_prediction = 
    else:
        prediction = ""
        
    return render_template("website.html", output = prediction)

# Running the app
if __name__ == '__main__':
    app.run(debug = True)
