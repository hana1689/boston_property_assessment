import boto3
import psycopg2
import db_config
import pandas as pd
from sqlalchemy import create_engine
import fire
import crime
import police
import rental
import property
import address

dbname = db_config.db_name
user = db_config.db_username
host = db_config.db_endpoint
password = db_config.db_password
DESTINATION = 'boston-data'

conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'" \
                        .format(dbname, user, host, password))
engine = create_engine(
    'postgresql+psycopg2://postgres:postgres@ha-postgresql.cnhi406edexl.us-east-1.rds.amazonaws.com:5432/ha_db')
cur = conn.cursor()


def create_schema():
    """
    This function is used to create schema for each dataset
    """
    fire.create_fire_table(cur, conn)
    crime.create_crime_table(cur, conn)
    property.create_property_table(cur, conn)
    rental.create_rental_table(cur, conn)
    police.create_police_table(cur, conn)
    address.create_boston_table()


def read_and_write_db():
    """
    This function is used to get data from s3, convert to data frame, and pass each data frame to a proper dataset python file to
    perform cleaning, validation, and writing into the database
    """
    s3 = boto3.client('s3')                                                                                                                                       
    key = ['fire-data.csv', 'crime_data.csv', 'Boston_Police_Stations.csv', 'property-assessment.csv', 'short-term-rental-eligibility.csv', 'boston_address.csv'] 
    list = ['fire', 'crime', 'police', 'property', 'rental', 'address']                                                                                           
    dict = {}                                                                                                                                                     
                                                                                                                                                              
    index = 0                                                                                                                                                     
    for item in key:                                                                                                                                              
        obj =  s3.get_object(Bucket=DESTINATION, Key=item)                                                                                                        
        df = pd.read_csv(obj['Body'])                                                                                                                             
        dict[list[index]] = [df]                                                                                                                                  
        index += 1                                                                                                                                                

    fire.read_and_write_fire(dict['fire'], engine)                                                                                                                
    crime.read_and_write_crime(dict['crime'], engine)                                                                                                             
    police.read_and_write_police(dict['police'], engine)                                                                                                          
    rental.read_and_write_rental(dict['rental'], engine)                                                                                                          
    property.read_and_write_property(dict['property'], engine)                                                                                                    
    address.write_directions(dict['address'])                                                                                                                     
                                                                                                                                                              

def main():
    create_schema()
    read_and_write_db()


if __name__ == '__main__':
    main()
