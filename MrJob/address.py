import pandas as pd
from pandas import DataFrame
import main

conn = main.conn
cur = main.cur
engine = main.engine

def create_boston_table():
    """
    This function is used to define schema and data types for the relationship table. This table is used to store foreign keys and relationship with
    other datasets
    """
    cur.execute("""DROP TABLE IF EXISTS boston_data""")
    cur.execute("""CREATE TABLE boston_data(
        x double precision,
        y double precision,
        sam_address_id bigint PRIMARY KEY,
        full_address text,
        full_street_name text,
        zip_code bigint,
        property_id integer REFERENCES property_assessment(property_id),
        crime_id integer REFERENCES crime(crime_id),
        fire_id integer REFERENCES fire(fire_id),
        police_id integer REFERENCES police_station(objectid),
        rental_id integer REFERENCES short_term_rental_eligibility(sam_address_id)
    )""")
    conn.commit()

def write_directions(directions_df):
    """
    This function is used to write street information obtained from boston address dataset in s3. Then, the relationship with other datasets is generated
    based on this information
    :param directions_df: pass the data frame obtained from s3
    """
    directions_df.columns = map(str.lower, directions_df.columns)  # lower column names
    # remove unused columns
    directions_column = ['relationship_type', 'building_id', 'street_number', 'is_range', 'range_from', 'range_to',
                         'unit', 'street_id',
                         'street_prefix', 'street_body', 'street_suffix_abbr', 'street_full_suffix',
                         'mailing_neighborhood',
                         'street_suffix_dir', 'street_number_sort', 'sam_street_id', 'ward', 'precinct_ward', 'parcel',
                         'x_coord', 'y_coord']
    directions_df = directions_df.drop(directions_column, axis=1)
    # cast zipcode column to int
    directions_df['zip_code'] = pd.to_numeric(directions_df['zip_code'], downcast='signed')
    directions_df['full_street_name'] = directions_df['full_street_name'].str.upper()

    # insert default values to other columns as the join is performed later
    directions_df = directions_df.sort_values(by=['full_street_name'])
    directions_df['sam_id'] = directions_df['sam_address_id']

    # sort column street_name by alphabetic for the purpose of joining later
    new_data = merge_rental(directions_df)
    new_data = merge_police(new_data)
    new_data = merge_property(new_data)
    new_data = merge_crime(new_data)
    new_data = merge_fire(new_data)

    new_data.to_sql("boston_data", engine, if_exists='append', index=False)


def merge_property(new_data):
     """
     This function is used to create the relationship for property table. First, it selects the id, street name, and zipcode from database. Then, the code
     performs the join between boston address table and property table through street name and zipcode. Then, only id is updated back to the boston
     address table
     :param new_data: pass the boston address table obtained from the function above
     """
     sql_statement = "select property_id, st_name, zipcode from property_assessment"
     new_data['full_address'] = new_data['full_address'].str.upper()
     cur.execute(sql_statement)
     conn.commit
     tmp = cur.fetchall()
     col_names = []
     for elt in cur.description:
         col_names.append(elt[0])
     data = DataFrame(tmp, columns=col_names)
     property_data = pd.merge(new_data, data, left_on=['full_address', 'zip_code'], right_on=['st_name', 'zipcode'],
                              how='left').drop(['st_name', 'zipcode'], axis=1)
     return property_data


def merge_rental(new_data):
    """
    This function is used to create the relationship for rental table. First, it selects the id from database. Then, the code
    performs the join between boston address table and rental table through the id. Then, id is updated back to the boston
    address table
    :param new_data: pass the boston address table obtained from the function above
    """
    # this query is executed to check if the sam_address_id is the same and that property is eligible for renting
    sql_statement = "select sam_address_id as rental_id from short_term_rental_eligibility where home_share_eligible = 'Y'"
    cur.execute(sql_statement)
    conn.commit
    tmp = cur.fetchall()
    col_names = []
    for elt in cur.description:
        col_names.append(elt[0])
    data = DataFrame(tmp, columns=col_names)
    # this line of code is used to merge the rental dataset and the boston address dataset
    rental_data = pd.merge(new_data, data, left_on=['sam_address_id'], right_on=['rental_id'], how='left')
    return rental_data


def merge_police(new_data):
    """
    This function is used to create the relationship for police table. First, it selects the id and address from database. Then, the code
    performs the join between boston address table and police table through address. Then, only id is updated back to the boston
    address table
    :param new_data: pass the boston address table obtained from the function above
    """
    sql_statement = "select objectid as police_id, address from police_station"
    cur.execute(sql_statement)
    conn.commit
    tmp = cur.fetchall()
    col_names = []
    for elt in cur.description:
        col_names.append(elt[0])
    data = DataFrame(tmp, columns=col_names)
    # this line of code is used to merge the police dataset and the boston address dataset
    police_data = pd.merge(new_data, data, left_on=['full_address'], right_on=['address'], how='left').drop(['address'], axis=1)
    return police_data


def merge_crime(new_data):
    """
    The idea of this function is one street address may have many crimes. First, we will obtain the unique street name. Then, a loop is built to iterate
    through that list of unique street name. Then, we select only records from the dataframe where street name matches. The last step is to retrieve crime_id
    from crime table at this street name and update it back to the boston address dataframe
    :param new_data: pass the boston address table obtained from the function above
    """
    # this code receives a lit of unique street name from the boston address dataframe
    street = new_data['full_street_name'].unique()
    # iterate through each street
    for i in range(street):
        street_name = street[i]
        # select all crime_id from table crime where street is equal to the street name above
        crime_sql = "SELECT crime_id FROM crime WHERE street = %(st_name)s"
        prop_id = pd.read_sql(crime_sql, con=engine, params={"st_name": street_name})
        # this line of code retrieves a list of rows where street name is equal to the street name above
        prop_id1 = new_data[new_data['full_street_name'] == street_name]
        # only index is obtained and added to list
        new_data = pd.DataFrame(prop_id1['sam_address_id'])
        test = new_data.reset_index().values.tolist()

        counter = 0
        if len(prop_id1) < len(prop_id):
            counter = len(prop_id1)
        else:
            counter = len(prop_id)

        # this block of code updates the crime_id column in the boston address dataframe where only index is met
        for k in range(counter):
            crime = prop_id.loc[k, 'crime_id']
            sam_id = test[k][0]
            new_data.at[sam_id, 'crime_id'] = crime
    return new_data

def merge_fire(new_data):
    """
    The idea of this function is one street address may have many fires. First, we will obtain the unique street name. Then, a loop is built to iterate
    through that list of unique street name. Then, we select only records from the dataframe where street name matches. The last step is to retrieve fire_id
    from fire table at this street name and update it back to the boston address dataframe
    :param new_data: pass the boston address table obtained from the function above
    """
    # this code receives a lit of unique street name from the boston address dataframe
    street = new_data['full_street_name'].unique()
    for i in range(street):
        street_name = street[i]
        # select all fire_id from table fire where street is equal to the street name above
        fire_sql = "SELECT fire_id FROM fire WHERE street_body = %(st_name)s"
        prop_id = pd.read_sql(fire_sql, con=engine, params={"st_name": street_name})
        # this line of code retrieves a list of rows where street name is equal to the street name above
        prop_id1 = new_data[new_data['full_street_name'] == street_name]
        # only index is obtained and added to list
        new_data = pd.DataFrame(prop_id1['sam_address_id'])
        test = new_data.reset_index().values.tolist()

        counter = 0
        if len(prop_id1) < len(prop_id):
            counter = len(prop_id1)
        else:
            counter = len(prop_id)

        # this block of code updates the fire_id column in the boston address dataframe where only index is met
        for k in range(counter):
            fire = prop_id.loc[k, 'fire_id']
            sam_id = test[k][0]
            new_data.at[sam_id, 'fire_id'] = fire
    return new_data


