# boston_property_assessment

# Introduction
This is a data engineering project developed at Insight Data Science, Boston. The project is about exploring Boston property assessment and neighborhood data. The project aims to accomplish these features:
Build a data pipeline to translating different datasets into business insights and provide clean data for analysis or perform queries.
Enable a framework for viewing property and neighborhood data, so that a user can have a better view of property assessment across a city and able to spot any specific neighborhood to view historical data related to crime, fire, rental, and police stations.

# Data Set
The project is based on historical geolocation data from Boston government website, collected over a year's time frame (200MB). They are six different datasets which are a crime report, a fire report, police stations, property, short-term rental eligibily, and boston address. These datasets are updated every year. The following table provides a snap shot of the raw data from those datasets:

# Data Pipeline
Those datasets are loaded into S3 data bucket. Then, a python script is developed running on EC2 instance along with data structure libraries to create schema for datasets, clean, and join those data. Once the data is clean and the relationship is built, it then is written into a database which is postgresql through defined schema. The property information such as rental eligibility, price, mail address, owner, land size, number of floors, etc will be displayed on the website through Flask framework. The property data is represtend as a bar chart. The number of crimes, fires, and police stations are rendered on Google Maps and updated based on distance via AJAX and PostGIS.

# Repo directory structure

# Schema Design

![ER](https://user-images.githubusercontent.com/41086130/74114047-d41abb80-4b75-11ea-9ab8-22c43a5d9006.jpg)

# Environmental Setup
EC2 setup
PostgreSQL setup

# Instructions to run the pipeline
Cleaning and Writing data into a database
  Install python packages
  Install pandas libary
  Install psycopg2 libary
  Install boto3 libary
  Install SqlAlchemy libary
  Run MrJob
  
Flask application
  Install flask
  Run run.py to start the web server
