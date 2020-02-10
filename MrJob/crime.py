def create_crime_table(cur, conn):
    """
    This function is used to define schema and data types
    :param cur: pass the cursor to execute query
    :param conn: pass the connection
    """
    cur.execute("""DROP TABLE IF EXISTS crime""")
    cur.execute("""CREATE TABLE crime(
        crime_id int PRIMARY KEY,
        incident_number text,
        offense_code bigint,
        offense_code_group text,
        offense_description text,
        district text,
        reporting_area text,
        shooting text,
        occurred_on_date text,
        year int,
        month int,
        day_of_week text,
        hour int,
        ucr_part text,
        street text,
        lat double precision,
        long double precision
    )""")
    conn.commit()

def read_and_write_crime(crime_df, engine):
    """
    This function is used to drop unused columns and define primary keys for table. Then, the script calls method to_sql to write a whole data frame
    to sql through defined schema
    :param crime_df: pass the data frame obtained from s3
    :param engine: pass the sqlalchemy engine to write data frame into db
    """
    crime_df.columns = map(str.lower, crime_df.columns)
    crime_df = crime_df.drop(['location'], axis=1)
    for index, row in crime_df.iterrows():
        crime_df.at[index, 'crime_id'] = index + 1

    crime_df.to_sql("crime", engine, if_exists='append', index=False)
