# https://blog.logrocket.com/build-deploy-flask-app-using-docker/
# https://www.freecodecamp.org/news/how-to-dockerize-a-flask-app/

# start by pulling the python image
# FROM python:3.8-alpine
# RUN apk --no-cache add musl-dev linux-headers g++
## https://stackoverflow.com/questions/33421965/installing-numpy-on-docker-alpine
# RUN pip install --upgrade pip


# Just stay with UCSD's base image
ARG BASE_CONTAINER=ucsdets/datahub-base-notebook:2022.3-stable

FROM $BASE_CONTAINER

LABEL maintainer="UC San Diego ITS/ETS <ets-consult@ucsd.edu>"

# 2) change to root to install packages
USER root

RUN apt update

# RUN apt-get -y install aria2 nmap traceroute

# 3) install packages using notebook user
USER jovyan


# copy the requirements file into the image
COPY requirements.txt /app/requirements.txt

# switch working directory
WORKDIR /app

# install the dependencies and packages in the requirements file
#RUN pip install -r requirements.txt
RUN pip install --no-cache-dir scikit-learn==1.0.2 werkzeug==2.2.3 flask==2.2.4 geojson==3.0.1 numpy==1.23.2 pandas==2.0.0 shapely==1.8.4 requests==2.29.0 
#geopandas==0.11.1 matplotlib scipy bs4 seaborn gdown sodapy statsmodels polars pyarrow tqdm 

# copy every content from the local file to the image
# this means the geojsons!
COPY app.py /app/
COPY build_configs.py /app/
COPY configs.json /app/
COPY src/data/etl.py /app/src/data/
COPY src/features/data_clean.py /app/src/features/
COPY src/models/model.py /app/src/models/
COPY src/predict.py /app/src/

COPY templates/ /app/templates/
COPY static/ /app/static/
COPY data/final/ /app/data/final/

# configure the container to run in an executed manner
CMD [ "python", "app.py" ]

# docker build -t bensonduong/nyc_traffic_premade .
# docker push bensonduong/nyc_traffic_premade

# docker run -d -p 5000:5000 bensonduong/nyc_traffic_premade
# or 
# docker run -p 5000:5000 -d bensonduong/nyc_traffic_premade


# OLD VERSION
## 1) choose base container
## generally use the most recent tag

## base notebook, contains Jupyter and relevant tools
## See https://github.com/ucsd-ets/datahub-docker-stack/wiki/Stable-Tag 
## for a list of the most current containers we maintain
#ARG BASE_CONTAINER=ucsdets/datahub-base-notebook:2022.3-stable

#FROM $BASE_CONTAINER

#LABEL maintainer="UC San Diego ITS/ETS <ets-consult@ucsd.edu>"

## 2) change to root to install packages
#USER root

#RUN apt update

## RUN apt-get -y install aria2 nmap traceroute

## 3) install packages using notebook user
#USER jovyan

## RUN conda install -y babypandas geopandas

#RUN pip install --no-cache-dir geopandas numpy pandas matplotlib scipy scikit-learn bs4 requests seaborn gdown shapely werkzeug flask geojson sodapy statsmodels polars pyarrow tqdm 


## Override command to disable running jupyter notebook at launch
#CMD ["/bin/bash"]