import pandas as pd
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

    # insert default values to other columns as the join is performed later
    directions_df.insert(6, 'property_id', 0)
    directions_df.insert(7, 'crime_id', 0)
    directions_df.insert(8, 'fire_id', 0)
    directions_df.insert(9, 'police_id', 0)
    directions_df.insert(10, 'rental_id', 0)

    # sort column street_name by alphabetic for the purpose of joining later
    directions_df = directions_df.sort_values(by=['full_street_name'])

    directions_df.to_sql("boston_data", engine, if_exists='append', index=False)

    merge_property(directions_df)
    merge_rental(directions_df)
    merge_police(directions_df)
    merge_crime(directions_df)
    merge_fire(directions_df)


def merge_property(directions_df):
     """
     This function is used to create the relationship for property table. First, it receives address from table above. Then, the loop is executed to compare
     the street name between 2 datasets in each row. If the street name matches, the function gets the property_id from the property dataset to write into
     the address table. If there is no match, property_id is set to 0. These values are then written to the database
     :param directions_df: pass the boston address table obtained from the function above
     """
     property_sql = 'SELECT property_id FROM property_assessment WHERE st_name = %(st_name)s'
     for index, row in directions_df.iterrows():
         street_name = str(row['full_address'].upper())
         sam_id = str(row['sam_address_id'])
         # execute the query to check if the street_name is the same. The query returns the property_id where it matches with the street_name in boston address
         prop_id = pd.read_sql(property_sql, con=engine, params={"st_name": street_name})
         if prop_id.empty:
             cur.execute("UPDATE boston_data SET property_id = 0 where sam_address_id = '%s'" % sam_id)
             conn.commit()
         else:
             for i in range(len(prop_id)):
                 cur.execute("UPDATE boston_data SET property_id = %s where sam_address_id = %s" %(prop_id.loc[i, 'property_id'], sam_id))
                 conn.commit()


def merge_rental(directions_df):
    """
    This function is used to create the relationship for rental table. First, it receives sam_address_id from table above. Then, the loop is executed to compare
    the sam_address_id between 2 datasets in each row. If there is a match, the function gets the id from the rental dataset to write into
    the address table. If there is no match, rental_id is set to 0. These values are then written to the database
    :param directions_df: pass the boston address table obtained from the function above
    """

    # this query is executed to check if the sam_address_id is the same and that property is eligible for renting
    rental_sql = 'SELECT sam_address_id FROM short_term_rental_eligibility WHERE sam_address_id = %{sam_id}s AND home_share_eligible = \'Y\''

    for i in range(len(directions_df)):
        sam_id = directions_df.loc[i, 'sam_address_id']
        print i
        rental_id = pd.read_sql(rental_sql, con=engine, params={"sam_id": sam_id})

        if rental_id.empty:
            cur.execute("UPDATE boston_data SET rental_id = 0 where sam_address_id = '%s'" % sam_id)
            conn.commit()
        else:
            for k in range(len(rental_id)):
                cur.execute("UPDATE boston_data SET rental_id = %s where sam_address_id = %s" %(rental_id.loc[k, 'sam_address_id'], sam_id))
                conn.commit()


def merge_police(directions_df):
    """
    This function is used to create the relationship for police table. First, it receives street_name from table above. Then, the loop is executed to compare
    the street_name between 2 datasets in each row. If there is a match, the function gets the id from the rental dataset to write into
    the address table. If there is no match, police_id is set to 0. These values are then written to the database
    :param directions_df: pass the boston address table obtained from the function above
    """

    # since the street_name in police table has 2 different formats (Ex: 12 A St and A St), the comparision takes care of 2 different formats
    police_sql = 'SELECT objectid FROM police_station WHERE address = upper(%(street_name)s) OR address = upper(%(street_body)s)'

    for index, row in directions_df.iterrows():
        street_name = str(row['full_address'])
        street_body = str(row['full_street_name'])
        sam_id = str(row['sam_address_id'])

        pol_id = pd.read_sql(police_sql, con=engine, params={"street_name": street_name, "street_body": street_body})

        if pol_id.empty:
            cur.execute("UPDATE boston_data SET police_id = 0 where sam_address_id = '%s'" % sam_id)
            conn.commit()
        else:
            for i in range(len(pol_id)):
                cur.execute("UPDATE boston_data SET police_id = %s where sam_address_id = %s" % (
                pol_id.loc[i, 'objectid'], sam_id))
                conn.commit()


def merge_crime(directions_df):
    """
    The idea of this function is one street address may have many crimes. For every loop through boston address table, the function obtains only one crime_id
    if there is a match. Then this id is added to an array. When it comes to the next row with the same street address, if this id is already existed
    in the array, the query obtains another id which is different from the one in the array.
    :param directions_df: pass the boston address table obtained from the function above
    """
    crime_sql = 'SELECT crime_id FROM crime WHERE street = upper(%(street_body)s) AND crime_id not in %(crime_id)s limit 1'
    crime_sql1 = 'SELECT crime_id FROM crime WHERE street = upper(%(street_body)s) limit 1'
    id_previous = []

    for index in range(len(directions_df)):
        street_body = directions_df.loc[index, "full_street_name"]
        sam_id = directions_df.loc[index, "sam_address_id"]

        crime_identity = ""
        crime_id = pd.read_sql(crime_sql1, con=engine, params={"street_body": street_body})
        print index;

        if index == 0:
            if crime_id.empty:
                directions_df.loc[index, "crime_id"] = 0
                id_previous.append('0')
                cur.execute("UPDATE boston_data SET crime_id = 0 where sam_address_id = '%s'" % sam_id)
                conn.commit()
            else:
                for h in range(len(crime_id)):
                    directions_df.loc[index, 'crime_id'] = crime_id.loc[h, 'crime_id']
                id_previous.append(directions_df.loc[index, 'crime_id'])
                cur.execute("UPDATE boston_data SET crime_id = %s where sam_address_id = %s" % (directions_df.loc[index, 'crime_id'], sam_id))
                conn.commit()
        else:
            k = index - 1
            while (k > 0) & (directions_df.loc[k, "full_street_name"] == street_body):
                id_previous.append(directions_df.loc[k, "crime_id"])
                k = k - 1

            if crime_id.empty:
                directions_df.loc[index, "crime_id"] = 0
                cur.execute("UPDATE boston_data SET crime_id = 0 where sam_address_id = '%s'" % sam_id)
                conn.commit()
            else:
                for p in range(len(crime_id)):
                    crime_identity = crime_id.loc[p, 'crime_id']
                if crime_identity not in id_previous:
                    directions_df.loc[index, "crime_id"] = crime_identity
                    cur.execute("UPDATE boston_data SET crime_id = %s where sam_address_id = %s" % (
                        crime_identity, sam_id))
                    conn.commit()

                else:
                    crime_id2 = pd.read_sql(crime_sql, con=engine,
                                            params={"street_body": street_body, "crime_id": tuple(id_previous)})
                    if crime_id2.empty:
                        directions_df.loc[index, "crime_id"] = 0
                        cur.execute("UPDATE boston_data SET crime_id = 0 where sam_address_id = '%s'" % sam_id)
                        conn.commit()
                    else:
                        for t in range(len(crime_id2)):
                            directions_df.loc[index, 'crime_id'] = crime_id2.loc[t, 'crime_id']
                        cur.execute("UPDATE boston_data SET crime_id = %s where sam_address_id = %s" % (directions_df.loc[index, 'crime_id'], sam_id))
                        conn.commit()


def merge_fire(directions_df):
    """
    The idea of this function is one street address may have many crimes. For every loop through boston address table, the function obtains only one crime_id
    if there is a match. Then this id is added to an array. When it comes to the next row with the same street address, if this id is already existed
    in the array, the query obtains another id which is different from the one in the array.
    :param directions_df: pass the boston address table obtained from the function above
    """
    fire_sql = 'SELECT fire_id FROM fire WHERE street_name = upper(%(street_body)s) AND fire_id not in %(fire_id)s limit 1'
    fire_sql1 = 'SELECT fire_id FROM fire WHERE street_name = upper(%(street_body)s) limit 1'
    id_previous = list()

    for index in range(len(directions_df)):
         street_body = directions_df.loc[index, "full_street_name"]
         sam_id = directions_df.loc[index, "sam_address_id"]

         fire_identity1 = ""
         fire_identity2 = ""
         fire_id = pd.read_sql(fire_sql1, con=engine,
                               params={"street_body": street_body})

         if index == 0:
             if not fire_id.empty:
                 for h in range(len(fire_id)):
                     directions_df.loc[index, 'fire_id'] = fire_id.loc[h, 'fire_id']
                 id_previous.append(directions_df.loc[index, 'fire_id'])
                 cur.execute("UPDATE boston_data SET fire_id = %s where sam_address_id = %s" % sam_id)
                 conn.commit()
             else:
                 cur.execute("UPDATE boston_data SET fire_id = 0 where sam_address_id = %s" % sam_id)
                 conn.commit()
                 directions_df.loc[index, 'fire_id'] = 0
                 id_previous.append('0')

         else:
             k = index - 1
             while (k > 0) & (directions_df.loc[k, "full_street_name"] == street_body):
                id_previous.append(directions_df.loc[k, "fire_id"])
                k = k - 1

             if not fire_id.empty:
                 for m in range(len(fire_id)):
                     fire_identity1 = fire_id.loc[m, 'fire_id']
                 if fire_identity1 not in id_previous:
                     directions_df.loc[index, "fire_id"] = fire_identity1
                     cur.execute("UPDATE boston_data SET fire_id = %s where sam_address_id = %s" % (
                         fire_identity1, sam_id))
                     conn.commit()
                 else:
                     fire_id2 = pd.read_sql(fire_sql, con=engine,
                                            params={"street_body": street_body,
                                                    "fire_id": tuple(id_previous)})
                     if not fire_id2.empty:
                         for n in range(len(fire_id2)):
                             fire_identity2 = fire_id2.loc[n, 'fire_id']
                         directions_df.loc[index, 'crime_id'] = fire_identity2
                         cur.execute("UPDATE boston_data SET fire_id = %s where sam_address_id = %s" % (
                             fire_identity2, sam_id))
                         conn.commit()
             else:
                 cur.execute("UPDATE boston_data SET fire_id = 0 where sam_address_id = %s" % sam_id)
                 conn.commit()
                 directions_df.loc[index, 'fire_id'] = 0