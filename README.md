# boston_property_assessment

## Table of Content
1. [Introduction](README.md#introduction)
1. [DataSet](README.md#dataset)
1. [DataPipeline](README.md#data-pipeline)
1. [Repo Directory Structure](README.md#repo-directory-structure)
1. [Schema Design](README.md#schema-design)
1. [Environmental Setup](README.md#environmental-setup)
1. [Running Instructions](README.md#running-instructions)
1. [Demo](README.md#demo)
1. [Assumptions](README.md#assumptions)

## Introduction
This is a data engineering project developed at Insight Data Science, Boston. The project is about exploring Boston property assessment and neighborhood data. The project aims to accomplish these features:
* Build a data pipeline to translating different datasets into business insights and provide clean data for analysis or perform queries.
* Enable a framework for viewing property and neighborhood data, so that a user can have a better view of property assessment across a city and able to spot any specific neighborhood to view historical data related to crime, fire, rental, and police stations.

## DataSet
The project is based on historical geolocation data from Boston government website, collected over a year's time frame (200MB). They are six different datasets which are a crime report, a fire report, police stations, property, short-term rental eligibily, and boston address. These datasets are updated every year. 

## Data Pipeline

![Screen Shot 2020-02-010](https://user-images.githubusercontent.com/41086130/74114198-923e4500-4b76-11ea-9cea-438e2f737ebe.jpg)

Those datasets are loaded into S3 data bucket. Then, a python script is developed running on EC2 instance along with data structure libraries to create schema for datasets, clean, and join those data. Once the data is clean and the relationship is built, it then is written into a database which is postgresql through defined schema. The property information such as rental eligibility, price, mail address, owner, land size, number of floors, etc will be displayed on the website through Flask framework. The property data is represtend as a bar chart. The number of crimes, fires, and police stations are rendered on Google Maps and updated based on distance via AJAX and PostGIS.

## Repo Directory Structure
### MrJob
This is a folder to keep all the scripts related to creating schemas, cleaning data, and building a relationship between those datasets
#### db_config.py
This file has just only information about the PostgreSQL database which is running on AWS RDS, including username, passowrd, host, and port
#### main.py
This file performs these main functions which are establishing a connection with db, getting data from S3, passing data to other python files to perform its tasks. This is also the main running file.
#### fire.py
This file contains two main functions which are defining a schema for the fire table and cleaning and writing dataframe receiving from main.py to database
#### property.py
This file contains two main functions which are defining a schema for the property table and cleaning and writing dataframe receiving from main.py to database
#### crime.py
This file contains two main functions which are defining a schema for the crime table and cleaning and writing dataframe receiving from main.py to database
#### police.py
This file contains two main functions which are defining a schema for the police table and cleaning and writing dataframe receiving from main.py to database
#### rental.py
This file contains two main functions which are defining a schema for the rental table and cleaning and writing dataframe receiving from main.py to database
#### address.py
This file is used to create the relationship between those datasets. There are 6 main functions in this file which are creating a schema to keep boston address and foreign keys with other datasets and building a relationship with the fire table, crime table, property table, police table, and rental table.

### Flask
This is where my Flask program is stored. It references various forms and .html files that are stored in their proper place. 
#### run.py
This file is to help to start the running server and controls all the requests receiving from html files
#### base.html
This file helps to build web structure and controls all AJAX requests from users
#### main.css
This file stores the css of the base html file

## Schema Design

![ER](https://user-images.githubusercontent.com/41086130/74114047-d41abb80-4b75-11ea-9ab8-22c43a5d9006.jpg)

The primary keys first are developed for each dataset. Then the table in the middle is built to store the relationship of those datasets through foreign keys which are linked to a proper dataset. This schema is built through Python scripts above

## Environmental Setup
1. Set up EC2 instance (The scripts are running on Linux AMI)
2. Set up PostgreSQL on AWS RDS. Make sure there are 2 public IP addresses available for installing it on your VPC. Ignore if it is installed on default VPC

## Running Instructions
1. Cleaning and Writing data into a database
  * Install python packages 
  ```sudo yum install python3```
  * Install pandas libary 
  ```pip install pandas```
  * Install psycopg2 libary
  ```pip install psycopg2```
  * Install boto3 libary
  ```pip install boto3```
  * Install SqlAlchemy libary
  ```pip install SQLAlchemy```
  * Run MrJob by running main.py
  
  
2. Flask application
  * Create Python environment
  ```pip install virtualenv```
  * Install flask
  ```pip install Flask```
  * Run run.py to start the web server
  
## Demo
https://youtu.be/fNze9Uu3IO0
  
## Assumptions
* The user stores their information in an Amazon S3 bucket. 
* The datasets are organized in a .csv type format. 
* The information from boston address dataset is correct 

## Contact Information
Email: nguyenttha89@gmail.com
