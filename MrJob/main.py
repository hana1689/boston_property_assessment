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
    directions_obj = s3.get_object(Bucket=DESTINATION, Key='boston_address.csv')
    fire_obj = s3.get_object(Bucket=DESTINATION, Key='fire-data.csv')
    crime_obj = s3.get_object(Bucket=DESTINATION, Key='crime_data.csv')
    police_obj = s3.get_object(Bucket=DESTINATION, Key='Boston_Police_Stations.csv')
    property_obj = s3.get_object(Bucket=DESTINATION, Key='property-assessment.csv')
    rental_obj = s3.get_object(Bucket=DESTINATION, Key='short-term-rental-eligibility.csv')

    directions_df = pd.read_csv(directions_obj['Body'])
    fire_df = pd.read_csv(fire_obj['Body'])
    crime_df = pd.read_csv(crime_obj['Body'])
    police_df = pd.read_csv(police_obj['Body'])
    property_df = pd.read_csv(property_obj['Body'])
    rental_df = pd.read_csv(rental_obj['Body'])

    fire.read_and_write_fire(fire_df, engine)
    crime.read_and_write_crime(crime_df, engine)
    police.read_and_write_police(police_df, engine)
    rental.read_and_write_rental(rental_df, engine)
    property.read_and_write_property(property_df, engine)
    address.write_directions(directions_df)


def main():
    create_schema()
    read_and_write_db()


if __name__ == '__main__':
    main()
