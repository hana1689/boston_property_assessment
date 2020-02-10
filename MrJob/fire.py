def create_fire_table(cur, conn):
    """
    This function is used to define schema and data types
    :param cur: pass the cursor to execute query
    :param conn: pass the connection
    """
    cur.execute("""DROP TABLE IF EXISTS fire""")
    cur.execute("""CREATE TABLE fire(
        fire_id int PRIMARY KEY,
        incident_number text,
        exposure_number bigint,
        alarm_date text,
        alarm_time text,
        incident_type text,
        incident_description text,
        estimated_property_loss bigint,
        estimate_content_loss bigint,
        district bigint,
        city_section text,
        neighborhood text,
        zip bigint,
        property_use text,
        property_description text,
        street_name text,
        address_2 text,
        xstreet_prefix text,
        xstreet_name text,
        xstreet_suffix text,
        xstreet_type text)""")
    conn.commit()

def read_and_write_fire(fire_df, engine):
    """
    This function is used to rewrite column names, concatenate street name, create a format for column street name
    (Ex: street name should be 17 A St #34), define a primary key, and drop unused columns. Then, the script calls method to_sql to write a whole
    data frame to sql through defined schema
    :param fire_df: pass the data frame obtained from s3
    :param engine: pass the sqlalchemy engine to write data frame into db
    """
    fire_df = fire_df.rename(
        columns={"Incident Number": "incident_number", "Exposure Number": "exposure_number", "Alarm Date": "alarm_date",
                 "Alarm Time": "alarm_time", "Incident Type": "incident_type",
                "Incident Description": "incident_description", "Estimated Property Loss": "estimated_property_loss", "Estimated Content Loss": "estimate_content_loss", "District": "district",
                "City Section": "city_section", "Neighborhood": "neighborhood", "Zip": "zip", "Property Use": "property_use", "Property Description": "property_description", "Street Name": "street_name", "Address 2": "address_2",
                "XStreet Prefix": "xstreet_prefix", "XStreet Name": "xstreet_name", "XStreet Suffix": "xstreet_suffix", "XStreet Type": "xstreet_type"})
    for index, row in fire_df.iterrows():
        row['street_body'] = row['street_name'] + row['Street Suffix'] + row['Street Type']
        row['street_full_name'] = row['Street Number'] + row['Street Prefix'] + row['street_name'] + row['Street Suffix'] + \
                                 row['Street Type']
        fire_df.at[index, 'street_name'] = " ".join(row['street_full_name'].split())
        fire_df.at[index, 'street_body'] = " ".join(row['street_body'].split())
        fire_df.at[index, 'fire_id'] = index + 1
    fire_columns = ['Street Number', 'Street Prefix', 'Street Suffix', 'Street Type']
    fire_df = fire_df.drop(fire_columns, axis=1)

    fire_df.to_sql("fire", engine, if_exists='append', index=False)


