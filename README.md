# gans_weather_airports
data base structure:
## code mysql
```sql
-- Drop the database if it already exists
DROP DATABASE IF EXISTS atd_testrun_f ;

-- Create the database
CREATE DATABASE atd_testrun_f;

-- Use the database
USE atd_testrun_f;

-- Create the 'authors' table
CREATE TABLE cities_f (
    city_id INT AUTO_INCREMENT, -- 
    city_name VARCHAR(255) NOT NULL, -- 
    PRIMARY KEY (city_id) -- 
);

-- Create the 'books' table
CREATE TABLE geo_f (
    geo_id INT AUTO_INCREMENT, -- 
    country_name VARCHAR(255) NOT NULL,
    city_latitude VARCHAR(255) NOT NULL, -- 
    city_longitude VARCHAR(255) NOT NULL, -- 
    populations VARCHAR(255) NOT NULL,
    city_id INT, -- 
    PRIMARY KEY (geo_id), -- 
    FOREIGN KEY (city_id) REFERENCES cities(city_id) -- 
);

CREATE TABLE weather_data_f (
    city_id INT , -- 
    temp VARCHAR(255) NOT NULL, -- 
    cond VARCHAR(255) NOT NULL,
    wind VARCHAR(255) NOT NULL,
    forecast_time timestamp,
    register_time timestamp,
    FOREIGN KEY (city_id) REFERENCES cities(city_id) -- 
);
#arrival_icao	departure_icao	flight_number	arrival_time
CREATE TABLE flights_f (
    flight_id INT AUTO_INCREMENT, -- 
    arrival_icao VARCHAR(255) NOT NULL, -- 
    departure_icao VARCHAR(255) NOT NULL,
    flight_number VARCHAR(255) NOT NULL,
    arrival_time timestamp,
    PRIMARY KEY (flight_id), 
    FOREIGN KEY (arrival_icao) REFERENCES airports(airport_icao) -- 
);
CREATE TABLE airports_f (
    airport_icao VARCHAR(255) NOT NULL,
    airport_name VARCHAR(255) NOT NULL, --
    PRIMARY KEY (airport_icao) --
);
CREATE TABLE cities_airports_f (
    city_id INT NOT NULL,
    airport_icao VARCHAR(255) NOT NULL, -- 
    FOREIGN KEY (city_id) REFERENCES cities(city_id), -- 
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
```

## code python

```python
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import datetime
```


```python
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
```


```python
def fill_non_relational_df(city_name,df=None): #df test
    dct = get_city_info_from_wiki(city_name)
    if df is None:
        return pd.DataFrame([dct])
    return pd.concat([df,pd.DataFrame([dct])]).reset_index(drop=True)
```


```python
def connection_string():
    schema = "atd_testrun_f"
    host = "127.0.0.1"
    user = "ola"
    password = "dupa"
    port = 3306

    return  f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
```


```python
basic_info_df = fill_non_relational_df('Vienna')
basic_info_df = fill_non_relational_df('Berlin', basic_info_df)
basic_info_df = fill_non_relational_df('Munich', basic_info_df)
basic_info_df = fill_non_relational_df('Hanover', basic_info_df)
basic_info_df = fill_non_relational_df('Cologne', basic_info_df)
basic_info_df = fill_non_relational_df('Bochum', basic_info_df)
basic_info_df = fill_non_relational_df('London', basic_info_df)
basic_info_df = fill_non_relational_df('Paris', basic_info_df)
basic_info_df
```


```python
cities_unique_f = basic_info_df["city_name"].unique() 
cities_df_f = pd.DataFrame(data = cities_unique_f, columns = ["city_name"])

```

```python
cities_df_f.to_sql('cities_f',
                 if_exists='append',
                 con=connection_string(),
                 index=False)
```

```python
cities_from_sql_f = pd.read_sql("cities_f", con=connection_string())
```


```python
geo_df_f = basic_info_df.merge(cities_from_sql_f,
                                   on = "city_name",
                                   how="left"
                                )
geo_df_f = geo_df_f.drop(columns=["city_name"])
```


```python
geo_df_f.to_sql('geo_f',
                  if_exists='append',
                  con=connection_string(),
                  index=False)
```

```python
#sql
def city_list_weather_sql(city_list):    #api key eater!
    
    API = "yyy"
    
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
        
        lat = lat_lon_transform(elat, elon)[0]
        lon = lat_lon_transform(elat, elon)[1]
        
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
```

```python
city_list_weather = cities_from_sql_f["city_name"]

```

```python

weather_data_df_f = city_list_weather_sql(city_list_weather)
```


```python
weather_data_df_f.to_sql('weather_data_f',
                          if_exists='append',
                          con=connection_string(),
                          index=False)
```

```python
def get_airports(latitudes, longitudes):  ## api key eater
  # API headers
  headers = {
      "X-RapidAPI-Key": "xxx",
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
```


```python
def lat_lon_transform(in_lat, in_lon):
    tmp_r_lat = re.findall(r'\d+', in_lat)
    tmp_r_lon = re.findall(r'\d+', in_lon)
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
    return [lat,lon]
```


```python
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
        
        tmp_cities_list.append(tmp_df["city_id"])
        tmp_latitude_list.append(lat_lon_transform(elat, elon)[0])
        tmp_longitude_list.append(lat_lon_transform(elat, elon)[1])
 
    return [tmp_latitude_list,tmp_longitude_list]
```

```python
coordinates = get_latitudes_longitudes_sql()
latitudes = coordinates[0]
longitudes = coordinates[1]

```

```python
def get_icao_name_airports_df():  #api key eater!
    airports_df_f = get_airports(latitudes, longitudes)
    airports_df_f = airports_df_f.drop_duplicates()
    icao_name_airports_df = airports_df_f[['icao','name']].copy()
    icao_name_airports_df.dropna()
    icao_name_airports_df[["airport_icao","airport_name"]] = pd.DataFrame(data = icao_name_airports_df)
    return icao_name_airports_df[["airport_icao","airport_name"]]
```


```python
icao_name_airports_df = get_icao_name_airports_df()
```

```python
icao_name_airports_df.to_sql('airports_f',
                              if_exists='append',
                              con=connection_string(),
                              index=False)
```

```python
def get_city_id_airport_icao_df(df):
    
    icao_city_id_df= df[['icao','municipalityName']].copy().rename(columns={'municipalityName':'city_name'})  #city_id, airport_icao
    icao_city_id_df.dropna()
    
    city_name_id_df = pd.read_sql(f"""
                SELECT DISTINCT city_name, city_id
                FROM cities_f
                """,
                con=connection_string())
    city_name_id_df = pd.DataFrame(city_name_id_df)
    tmp = city_name_id_df.merge(icao_city_id_df,
                                   on = "city_name",
                                   how="left"
                                )
    tmp = tmp.drop(tmp.columns[[1]], axis = 1).copy()
    return tmp
```

```python
city_id_airport_icao_df = get_city_id_airport_icao_df(airports_df_f)
```


```python
city_id_airport_icao_df.to_sql('cities_airports_f',
                              if_exists='append',
                              con=connection_string(),
                              index=False)
```

```python
icao_iata_df = airports_df_f[['icao','iata']].copy()
```


```python

def get_arrivals_info(IATA):  # api key eater!
  from datetime import datetime  
  BER = IATA
  url = "https://aerodatabox.p.rapidapi.com/flights/airports/iata/BER/2025-12-04T20:00/2025-12-05T08:00"

  querystring = {"withLeg":"true","direction":"Both","withCancelled":"true","withCodeshared":"true","withCargo":"true","withPrivate":"true","withLocation":"true"}

  headers = {
    "x-rapidapi-key": "xxx"
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
  
  return result_df
```

```python
def get_flights_df():
    airport_icao_df = pd.read_sql(f"""
                SELECT DISTINCT airport_icao
                FROM airports_f
                """,
                con=connection_string())
    
    airport_icao_df = pd.DataFrame(airport_icao_df)
    airport_iata = []
    for e in airport_icao_df["airport_icao"]:
        airport_iata.append(icao_iata_df['iata'].loc[icao_iata_df['icao'] == e].values[0])
    arrivals_list = []
    for e in airport_iata:
        arrivals_list.append(get_arrivals_info(e)) 
    flights_df=pd.concat(arrivals_list)  
    return flights_df
```

```python
flights_df.to_sql('flights_f',
                  if_exists='append',
                  con=connection_string(),
                  index=False)
```










