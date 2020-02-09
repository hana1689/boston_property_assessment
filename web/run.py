from flask import Flask, request, session, g, redirect, \
    url_for, abort, render_template, flash, json
from flask import jsonify
import psycopg2
from flask_googlemaps import GoogleMaps
from flask_googlemaps import Map, icons
from config import Config
import os

app = Flask(__name__)
SECRET_KEY = os.urandom(32)
app.config['SECRET_KEY'] = SECRET_KEY

app.config.from_object(Config)

app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql+psycopg2://postgres:postgres@ha-postgresql.cnhi406edexl.us-east-1.rds.amazonaws.com:5432/ha_db"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False


def connect():
    conn = psycopg2.connect(user="postgres",
                            password="postgres",
                            host="ha-postgresql.cnhi406edexl.us-east-1.rds.amazonaws.com",
                            port="5432",
                            database="ha_db")
    return conn


@app.route('/', methods=['GET', 'POST'])
def index():
    zip_code = get_zipcode_data()
    return render_template('base.html', items=zip_code)


def get_zipcode_data():
    conn = connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "select distinct(zip_code) from boston_data where zip_code is not null")
        conn.commit()
        zip_code = cur.fetchall()
        my_list = []
        for row in zip_code:
            my_list.append(row[0])
        return my_list
    except:
        conn.rollback()
        raise
    finally:
        conn.close()


@app.route('/get_street_list', methods=['POST'])
def get_street_list():
    conn = connect()
    cur = conn.cursor()
    try:
        code = request.form.get('zip_code')
        cur.execute(
            "select distinct(full_street_name) from boston_data where zip_code = '%s'" % code)
        conn.commit()
        street = cur.fetchall()
        """ my_list = list()
        for row in street:
            my_list.append(row) """
        return json.dumps(street)
    except:
        conn.rollback()
        raise
    finally:
        conn.close()


@app.route('/get_crime_map_data', methods=['POST'])
def get_crime_map_data():
    conn = connect()
    cur = conn.cursor()
    try:
        street_name = request.form.get('street_name')
        zipcode = request.form.get('zip_code')
        location_query = "select geom from boston_data where full_street_name = " + \
            "'" + street_name + "'" + " and zip_code = " + zipcode + " limit 1"
        map_query = "SELECT x, y, crime_id FROM boston_data WHERE ST_DWithin(geom, (" + \
            location_query + ")::geography, 100) and crime_id is not null"
        cur.execute(map_query)
        conn.commit()
        ggl_data = cur.fetchall()

        map_data = []
        item_as_dict = {"lng": '', "lat": '', "crime_id": ''}

        for item in ggl_data:
            item_as_dict = {
                'lng': item[0],
                'lat': item[1],
                'crime_id': item[2]
            }
            map_data.append(item_as_dict)

        return jsonify(map_data)
    except:
        conn.rollback()
        raise
    finally:
        conn.close()


@app.route('/get_fire_map_data', methods=['POST'])
def get_fire_map_data():
    conn = connect()
    cur = conn.cursor()
    try:
        street_name = request.form.get('street_name')
        zipcode = request.form.get('zip_code')
        location_query = "select geom from boston_data where full_street_name = " + \
            "'" + street_name + "'" + " and zip_code = " + zipcode + " limit 1"
        map_query = "SELECT x, y, fire_id FROM boston_data WHERE ST_DWithin(geom, (" + \
            location_query + ")::geography, 100) and fire_id is not null"
        cur.execute(map_query)
        conn.commit()
        ggl_data = cur.fetchall()

        map_data = []
        item_as_dict = {"lng": '', "lat": '', "fire_id": ''}

        for item in ggl_data:
            item_as_dict = {
                'lng': item[0],
                'lat': item[1],
                'fire_id': item[2]
            }
            map_data.append(item_as_dict)

        return jsonify(map_data)
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

@app.route('/get_police_data', methods=['POST'])
def get_police_data():
    conn = connect()
    cur = conn.cursor()
    try:
        street_name = request.form.get('street_name')
        zipcode = request.form.get('zip_code')
        location_query = "select geom from boston_data where full_street_name = " + \
            "'" + street_name + "'" + " and zip_code = " + zipcode + " limit 1"
        map_query = "SELECT x, y, police_id FROM boston_data WHERE ST_DWithin(geom, (" + \
            location_query + ")::geography, 100) and police_id != 0"
        cur.execute(map_query)
        conn.commit()
        ggl_data = cur.fetchall()

        map_data = []
        item_as_dict = {"lng": '', "lat": '', "police_id": ''}

        for item in ggl_data:
            item_as_dict = {
                'lng': item[0],
                'lat': item[1],
                'police_id': item[2]
            }
            map_data.append(item_as_dict)

        return jsonify(map_data)
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

def load_ggl_data(query_id, street_name, zip_code, distance):
    conn = connect()
    cur = conn.cursor()
    try:
        location_query = "select geom from boston_data where full_street_name = " + \
            "'" + street_name + "'" + " and zip_code = " + zip_code + " limit 1"
        map_query = "SELECT x, y, " + query_id + " FROM boston_data WHERE ST_DWithin(geom, (" + \
            location_query + ")::geography, " + distance + ") and " + query_id + " != 0"
        
        cur.execute(map_query)
        conn.commit()
        ggl_data = cur.fetchall()

        map_data = []
        item_as_dict = {"lng": '', "lat": '', query_id: ''}

        for item in ggl_data:
            item_as_dict = {
                'lng': item[0],
                'lat': item[1],
                query_id: item[2]
            }
            map_data.append(item_as_dict)

        return map_data

    except:
        conn.rollback()
        raise
    finally:
        conn.close()


@app.route('/get_distance_data', methods=['GET'])
def get_distance_data():
        
    street_name = request.args.get('street_name')
    zipcode = request.args.get('zip_code')  
    distance = request.args.get('distance')  

    crime_data = load_ggl_data('crime_id', street_name, zipcode, distance)
    fire_data = load_ggl_data('fire_id', street_name, zipcode, distance)
    police_data = load_ggl_data('police_id', street_name, zipcode, distance)
        
    data = {"crime": crime_data, "fire": fire_data, "police": police_data}
    return jsonify(data)


@app.route('/get_body_data', methods=['POST', 'GET'])
def get_body_data():
    conn = connect()
    cur = conn.cursor()
    try:
        street_name = request.form.get('street_name')
        zipcode = request.form.get('zip_code')
        price = request.form.get('price')
        rental = request.form.get("rental")
        name = '%' + street_name + '%'
        sql_query = ''
        if (rental == 'No') | (not rental):
            sql_query = "select distinct(st_name), av_total, mail_address, av_land, yr_built, yr_remod, num_floors from property_assessment where st_name like upper(" + \
                "'" + name + "') and zipcode = " + zipcode + \
                " and av_total " + price + " and av_total != 0"
        else:
            sql_query = "select distinct(st_name), av_total, mail_address, av_land, yr_built, yr_remod, num_floors from property_assessment, boston_data where property_assessment.property_id = boston_data.property_id and st_name like upper(" + \
                "'" + name + "') and zipcode = " + zipcode + \
                " and av_total " + price + " and av_total != 0 and rental_id != 0"
        cur.execute(sql_query)
        conn.commit()
        prop_data = cur.fetchall()

        bar_data = []
        item_as_dict = {'st_name': '', 'av_total': '', 'mail_address': '',
                        'av_land': '', 'yr_built': '', 'yr_remod': '', 'num_floors': ''}

        for item in prop_data:
            name = item[0]
            if item_as_dict.get('st_name') != name:
                item_as_dict = {
                    'st_name': name,
                    'av_total': item[1],
                    'mail_address': item[2],
                    'av_land': item[3],
                    'yr_built': item[4],
                    'yr_remod': item[5],
                    'num_floors': item[6]
                }

            bar_data.append(item_as_dict)

        return jsonify(bar_data)
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

 
if __name__ == '__main__':
      app.run(host='0.0.0.0', port=80) 
