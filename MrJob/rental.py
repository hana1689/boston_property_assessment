def create_rental_table(cur, conn):
    """
    This function is used to define schema and data types
    :param cur: pass the cursor to execute query
    :param conn: pass the connection
    """
    cur.execute("""DROP TABLE IF EXISTS property_assessment""")
    cur.execute("""CREATE TABLE short_term_rental_eligibility(
        sam_address_id bigint PRIMARY KEY,
        issued_registration text,
        sam_address text,
        home_share_eligible text,
        limited_share_eligible text,
        owner_adjacent_eligible text,
        owners_current_license_types text,
        income_restricted text,
        problem_property text,
        problem_property_owner text,
        open_violation_count int,
        violations_in_the_last_6_months int,
        legally_restricted text,
        unit_owner_occupied text,
        building_owner_occupied text,
        units_in_building int,
        building_single_owner text
    )""")
    conn.commit()

def read_and_write_rental(rental_df, engine):
    """
    This function is used to rewrite column names. Then, the script calls method to_sql to write a whole data frame to sql through defined schema
    :param rental_df: pass the data frame obtained from s3
    :param engine: pass the sqlalchemy engine to write data frame into db
    """
    rental_df = rental_df.rename(
        columns={"home-share eligible": "home_share_eligible", "limited-share eligible": "limited_share_eligible",
                 "owner-adjacent eligible": "owner_adjacent_eligible",
                 "income restricted": "income_restricted", "problem property": "problem_property",
                 "problem property owner": "problem_property_owner", "open violation count": "open_violation_count",
                 "violations in the last 6 months": "violations_in_the_last_6_months",
                 "legally restricted": "legally_restricted",
                 "unit owner-occupied": "unit_owner_occupied", "building owner-occupied": "building_owner_occupied", "units in building": "units_in_building",
                 "building single owner": "building_single_owner"})
    rental_df.to_sql("short_term_rental_eligibility", engine, if_exists='append', index=False)