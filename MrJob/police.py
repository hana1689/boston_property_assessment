def create_police_table(cur, conn):
    """
    This function is used to define schema and data types
    :param cur: pass the cursor to execute query
    :param conn: pass the connection
    """
    cur.execute("""DROP TABLE IF EXISTS police_station""")
    cur.execute("""CREATE TABLE police_station(
        x double precision,
        y double precision,
        objectid int PRIMARY KEY,
        bldg_id text,
        bid bigint,
        address text,
        point_x double precision,
        point_y double precision,
        name text,
        neighborhood text,
        city text,
        zip bigint,
        ft_sqft bigint,
        story_ht int,
        parcel_id bigint
    )""")
    conn.commit()

def read_and_write_police(police_df, engine):
    """
    The function calls method to_sql to write a whole data frame to sql through defined schema
    :param police_df: pass the data frame obtained from s3
    :param engine: pass the sqlalchemy engine to write data frame into db
    """
    police_df.columns = map(str.lower, police_df.columns)
    police_df.to_sql("police_station", engine, if_exists='append', index=False)