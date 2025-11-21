# gans_weather_airports
data base structure:
# code mysql
-- Drop the database if it already exists
DROP DATABASE IF EXISTS atd_testrun_f ;

-- Create the database
CREATE DATABASE atd_testrun_f;

-- Use the database
USE atd_testrun_f;

-- Create the 'authors' table
CREATE TABLE cities_f (
    city_id INT AUTO_INCREMENT, -- Automatically generated ID for each author
    city_name VARCHAR(255) NOT NULL, -- Name of the author
    PRIMARY KEY (city_id) -- Primary key to uniquely identify each author
);

-- Create the 'books' table
CREATE TABLE geo_f (
    geo_id INT AUTO_INCREMENT, -- Automatically generated ID for each book
    country_name VARCHAR(255) NOT NULL,
    city_latitude VARCHAR(255) NOT NULL, -- Title of the book
    city_longitude VARCHAR(255) NOT NULL, -- Title of the book
    populations VARCHAR(255) NOT NULL,
    city_id INT, -- ID of the author who wrote the book
    PRIMARY KEY (geo_id), -- Primary key to uniquely identify each book
    FOREIGN KEY (city_id) REFERENCES cities(city_id) -- Foreign key to connect each book to its author
);

CREATE TABLE weather_data_f (
    city_id INT , -- Automatically generated ID for each author
    temp VARCHAR(255) NOT NULL, -- Name of the author
    cond VARCHAR(255) NOT NULL,
    wind VARCHAR(255) NOT NULL,
    forecast_time timestamp,
    register_time timestamp,
    FOREIGN KEY (city_id) REFERENCES cities(city_id) -- Primary key to uniquely identify each author
);
#arrival_icao	departure_icao	flight_number	arrival_time
CREATE TABLE flights_f (
    flight_id INT AUTO_INCREMENT, -- Automatically generated ID for each author
    arrival_icao VARCHAR(255) NOT NULL, -- Name of the author
    departure_icao VARCHAR(255) NOT NULL,
    flight_number VARCHAR(255) NOT NULL,
    arrival_time timestamp,
    PRIMARY KEY (flight_id), 
    FOREIGN KEY (arrival_icao) REFERENCES airports(airport_icao) -- Primary key to uniquely identify each author
);
CREATE TABLE airports_f (
    airport_icao VARCHAR(255) NOT NULL,
    airport_name VARCHAR(255) NOT NULL, -- Automatically generated ID for each author
    PRIMARY KEY (airport_icao) -- Primary key to uniquely identify each author
);
CREATE TABLE cities_airports_f (
    city_id INT NOT NULL,
    airport_icao VARCHAR(255) NOT NULL, -- Automatically generated ID for each author
    FOREIGN KEY (city_id) REFERENCES cities(city_id), -- Primary key to uniquely identify each author
    FOREIGN KEY (airport_icao) REFERENCES airports(airport_icao)
);

#drop table geo_f;
#drop table cities_f;
#drop table weather_data_f;
#drop table flights_f;
select * from geo_f;
select * from cities_f;
select * from weather_data_f;
select * from weather_data_f right join cities using(city_id);
select * from flights_f;
select * from airports_f;
select * from cities_airports_f;

#  code
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import datetime
def get_city_info_from_wiki(city_name): #test
  
  url = "https://en.wikipedia.org/wiki/" + city_name    
  headers = {'User-Agent': 'Chrome/134.0.0.0'}
  response = requests.get(url, headers=headers)
  soup = BeautifulSoup(response.content, 'html.parser')

  tmp_dict = {}
    
  tmp_dict["city_name"] = soup.find(class_ = "mw-page-title-main").get_text()
  tmp_dict["country_name"] = soup.find(class_="infobox-data").get_text()  
  tmp_dict["city_latitude"] = soup.find(class_="external text").find(class_ = "latitude").get_text()
  tmp_dict["city_longitude"] = soup.find(class_="external text").find(class_ = "longitude").get_text()
  tmp_dict["populations"] = soup.find(string="Population").find_next("td").get_text()
    
  return tmp_dict
def fill_non_relational_df(city_name,df=None): #df test
    dct = get_city_info_from_wiki(city_name)
    if df is None:
        return pd.DataFrame([dct])
    return pd.concat([df,pd.DataFrame([dct])]).reset_index(drop=True)
def connection_string():
    schema = "atd_testrun_f"
    host = "127.0.0.1"
    user = "ola"
    password = "dupa"
    port = 3306

    return  f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
basic_info_df = fill_non_relational_df('Vienna')
basic_info_df = fill_non_relational_df('Berlin', basic_info_df)
basic_info_df = fill_non_relational_df('Munich', basic_info_df)
basic_info_df = fill_non_relational_df('Hanover', basic_info_df)
basic_info_df = fill_non_relational_df('Cologne', basic_info_df)
basic_info_df = fill_non_relational_df('Bochum', basic_info_df)
basic_info_df = fill_non_relational_df('London', basic_info_df)
basic_info_df = fill_non_relational_df('Paris', basic_info_df)
basic_info_df
cities_unique_f = basic_info_df["city_name"].unique() 
cities_df_f = pd.DataFrame(data = cities_unique_f, columns = ["city_name"])

cities_df_f.to_sql('cities_f',
                 if_exists='append',
                 con=connection_string(),
                 index=False)
cities_from_sql_f = pd.read_sql("cities_f", con=connection_string())
geo_df_f = basic_info_df.merge(cities_from_sql_f,
                                   on = "city_name",
                                   how="left"
                                )
geo_df_f = geo_df_f.drop(columns=["city_name"])
geo_df_f.to_sql('geo_f',
                  if_exists='append',
                  con=connection_string(),
                  index=False)
#sql
def city_list_weather_sql(city_list):
    
    API = "f2df36bc0d3912bb55c79b2d3087e142"
    
    city_list_sql_str = ", ".join([ f"'{city}'" for city in city_list])
    
    tmp_df = pd.read_sql(f"""
                SELECT DISTINCT city_latitude, city_longitude, city_id
                FROM geo_f
                JOIN cities_f
                USING(city_id)
                WHERE city_name IN ({city_list_sql_str})
                """,
                con=connection_string())
    
    tmp_df = pd.DataFrame(tmp_df)
    
    dct = {"city_id" : [], "temp" : [], "cond" : [], "wind" : [], "forecast_time" : [], "register_time" : []}
    
    for e, elat, elon in zip(tmp_df.city_id, tmp_df.city_latitude, tmp_df.city_longitude):
        
        tmp_r_lat = re.findall(r'\d+', elat)
        tmp_r_lon = re.findall(r'\d+', elon)
        if len(tmp_r_lat)<3:
            tlat = 0
        else:
            tlat= float(tmp_r_lat[2])
        if len(tmp_r_lon)<3:
            tlon = 0
        else:
            tlon= float(tmp_r_lat[2])    
        tmp_cl_lat = float(tmp_r_lat[0]) + float(tmp_r_lat[1])/60 + tlat/3600
        tmp_cl_lon = float(tmp_r_lon[0]) + float(tmp_r_lon[1])/60 + tlon/3600                          #tmp_r_lon[0]+'.'+tmp_r_lon[1]
        lat = round(tmp_cl_lat,2)
        lon = round(tmp_cl_lon,2)
        lat = tmp_cl_lat
        lon = tmp_cl_lon
        
        #api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={API key}
        weather_test = requests.get(f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&cnt=5&appid={API}&units=metric")
        weather_test_json = weather_test.json()

        dct['city_id'].append(e)
        dct['temp'].append(weather_test_json['list'][1]['main']['temp'])
        dct['cond'].append(weather_test_json['list'][1]['weather'][0]['description'])
        dct['wind'].append(weather_test_json['list'][1]['wind']['speed'])
        dct['forecast_time'].append(weather_test_json['list'][1]['dt_txt'])
        dct['register_time'].append(datetime.datetime.now())
        
        result_df = pd.DataFrame(data = dct).copy()
 
    return result_df
city_list_weather = cities_from_sql_f["city_name"]


weather_data_df_f = city_list_weather_sql(city_list_weather)
weather_data_df_f.to_sql('weather_data_f',
                          if_exists='append',
                          con=connection_string(),
                          index=False)
def get_airports(latitudes, longitudes):
  # API headers
  headers = {
      "X-RapidAPI-Key": "36beed95bamsh46a1d4b7e3ed4d0p16b0e3jsnc8ab4ff3f7dd",
      "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
  }

  querystring = {"withFlightInfoOnly": "true"}

  # DataFrame to store results
  all_airports = []

  for lat, lon in zip(latitudes, longitudes):
    # Construct the URL with the latitude and longitude
    url = f"https://aerodatabox.p.rapidapi.com/airports/search/location/{lat}/{lon}/km/50/16"

    # Make the API request
    response = requests.get(url, headers=headers, params=querystring)
    #print(response.text)

    if response.status_code == 200:
      data = response.json()
      airports = pd.json_normalize(data.get('items', []))
      all_airports.append(airports)

  return pd.concat(all_airports, ignore_index=True)
#sql
def get_latitudes_longitudes_sql():
    
    tmp_df = pd.read_sql(f"""
                SELECT DISTINCT city_latitude, city_longitude, city_id
                FROM geo_f
                JOIN cities_f
                USING(city_id)
                """,
                con=connection_string())
    
    tmp_df = pd.DataFrame(tmp_df)
    
    tmp_cities_list = []
    tmp_latitude_list = []
    tmp_longitude_list = []
    
    for e, elat, elon in zip(tmp_df.city_id, tmp_df.city_latitude, tmp_df.city_longitude):
        
        tmp_r_lat = re.findall(r'\d+', elat)
        tmp_r_lon = re.findall(r'\d+', elon)
        if len(tmp_r_lat)<3:
            tlat = 0
        else:
            tlat= float(tmp_r_lat[2])
        if len(tmp_r_lon)<3:
            tlon = 0
        else:
            tlon= float(tmp_r_lat[2])    
        tmp_cl_lat = float(tmp_r_lat[0]) + float(tmp_r_lat[1])/60 + tlat/3600
        tmp_cl_lon = float(tmp_r_lon[0]) + float(tmp_r_lon[1])/60 + tlon/3600                          #tmp_r_lon[0]+'.'+tmp_r_lon[1]
        lat = round(tmp_cl_lat,2)
        lon = round(tmp_cl_lon,2)
        
        tmp_cities_list.append(tmp_df["city_id"])
        tmp_latitude_list.append(lat)
        tmp_longitude_list.append(lon)
 
    return [tmp_latitude_list,tmp_longitude_list]
coordinates = get_latitudes_longitudes_sql()
latitudes = coordinates[0]
longitudes = coordinates[1]

latitudes
longitudes
airports_df_f = get_airports(latitudes, longitudes)
airports_df_f = airports_df_f.drop_duplicates()
icao_name_airports_df[["airport_icao","airport_name"]] = airports_df_f[['icao','name']].copy()
icao_name_airports_df.dropna()
icao_name_airports_df.to_sql('airports_f',
                              if_exists='append',
                              con=connection_string(),
                              index=False)


def get_arrivals_info(IATA):
  from datetime import datetime  
  BER = IATA
  url = "https://aerodatabox.p.rapidapi.com/flights/airports/iata/BER/2025-12-04T20:00/2025-12-05T08:00"

  querystring = {"withLeg":"true","direction":"Both","withCancelled":"true","withCodeshared":"true","withCargo":"true","withPrivate":"true","withLocation":"true"}

  headers = {
    "x-rapidapi-key": "36beed95bamsh46a1d4b7e3ed4d0p16b0e3jsnc8ab4ff3f7dd"
,
    "x-rapidapi-host": "aerodatabox.p.rapidapi.com"
  }

  response = requests.get(url, headers=headers, params=querystring)
  
  arrival_info = {"arrival_icao" : [],"departure_icao" : [], "flight_number" : [], "arrival_time" : []}
  
  if response.status_code == 200:
      data_info = response.json()
  for e in data_info['arrivals']:    
      departure_icao = e['departure']['airport']['icao']
      arrival_info["departure_icao"].append(departure_icao)
      dt_str = e['arrival']['scheduledTime']['local']
      dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M%z')
      arrival_time = dt.strftime('%Y-%m-%d %H:%M:%S')
      arrival_info["arrival_time"].append(arrival_time)
      flight_number = e['number']
      arrival_info["flight_number"].append(flight_number)
      arrival_info["arrival_icao"].append(icao_iata_df['icao'].loc[icao_iata_df['iata'] == IATA].values[0])
      result_df = pd.DataFrame(data = arrival_info).copy()
  print(str(icao_iata_df['icao'].loc[icao_iata_df['iata'] == IATA]))
  return result_df

arrivals_info_df_f = pd.DataFrame(data = arrivals_info_df_f)

arrivals_info_df_f
