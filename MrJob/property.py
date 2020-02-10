import pandas as pd

def create_property_table(cur, conn):
    """
    This function is used to define schema and data types
    :param cur: pass the cursor to execute query
    :param conn: pass the connection
    """
    cur.execute("""DROP TABLE IF EXISTS property_assessment""")
    cur.execute("""CREATE TABLE property_assessment(
        property_id int PRIMARY KEY,
        pid bigint,
        cm_id bigint,
        gis_id bigint,
        st_name text,
        zipcode bigint,
        ptype bigint,
        lu text,
        own_occ text,
        owner text,
        mail_addressee text,
        mail_address text,
        mail_cs text,
        mail_zipcode bigint,
        av_land bigint,
        av_bldg bigint,
        av_total bigint,
        gross_tax bigint,
        land_sf bigint,
        yr_built bigint,
        yr_remod bigint,
        gross_area bigint,
        living_area bigint,
        num_floors float4,
        structure_class text,
        r_bldg_styl text,
        r_roof_typ text,
        r_ext_fin text,
        r_total_rms int,
        r_bdrms int,
        r_full_bth int,
        r_half_bth int,
        r_bth_style text,
        r_bth_style2 text,
        r_bth_style3 text,
        r_kitch int,
        r_kitch_style text,
        r_kitch_style2 text,
        r_kitch_style3 text,
        r_heat_typ text,
        r_ac text,
        r_fplace int,
        r_ext_cnd text,
        r_ovrall_cnd text,
        r_int_cnd text,
        r_int_fin text,
        r_view text,
        s_num_bldg int,
        s_bldg_styl text,
        s_unit_res text,
        s_unit_com text,
        s_unit_rc text,
        s_ext_fin text,
        s_ext_cnd text,
        u_base_floor int,
        u_num_park int,
        u_corner text,
        u_orient text,
        u_tot_rms text,
        u_bdrms int,
        u_full_bth int,
        u_half_bth int,
        u_bth_style text,
        u_bth_style2 text,
        u_bth_style3 text,
        u_kitch_type text,
        u_kitch_style text,
        u_heat_typ text,
        u_ac text,
        u_fplace text,
        u_int_fin text,
        u_int_cnd text,
        u_view text  
    )""")
    conn.commit()

def read_and_write_property(property_df, engine):
    """
    This function is used to rewrite column names, concatenate street name, create a format for column street name
    (Ex: street name should be 17 A St #34), define a primary key, drop unused columns, and fix data type for the zipcode column as
    there are mixed data types. Then, the script calls method to_sql to write a whole data frame to sql through defined schema
    :param property_df: pass the data frame obtained from s3
    :param engine: pass the sqlalchemy engine to write data frame into db
    """
    property_df.columns = map(str.lower, property_df.columns)
    property_df = property_df.rename(columns={"mail cs": "mail_cs"})
    property_df = property_df.head(100)

    for index, row in property_df.iterrows():
        if pd.isnull(row['unit_num']):
            row['street_name'] = str(row['st_num']) + " " + str(row['st_name']) + " " + str(row['st_name_suf'])
        else:
            row['unit_num'] = str(row['unit_num']).replace('-', '')
            row['street_name'] = str(row['st_num']) + " " + str(row['st_name']) + " " + str(row['st_name_suf']) + " #" + \
                                 row['unit_num']
        property_df.at[index, 'st_name'] = row['street_name']

        check = str(row['mail_zipcode']).isdigit()
        if not check:
            property_df.at[index, 'mail_zipcode'] = 0000
        else:
            property_df.at[index, 'mail_zipcode'] = row['mail_zipcode']

        property_df.at[index, 'property_id'] = index + 1

    property_columns = ['st_num', 'st_name_suf', 'unit_num']
    property_df = property_df.drop(property_columns, axis=1)
    property_df.to_sql("property_assessment", engine, if_exists='append', index=False)