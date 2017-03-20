-- Table: public.pcode_data1(Post code level data)

CREATE TABLE public.pcode_data1
(
  postcode integer,
  state character(20),
  poa_code character(20),
  area_sqkm character varying(50),
  latitude character varying(50),
  longitude character varying(50),
  total_persons_males integer,
  total_persons_females integer,
  total_persons_persons integer,
  median_age_of_persons integer,
  median_total_personal_income_weekly integer,
  median_total_household_income_weekly integer,
  total_dwellings integer,
  geo_traffic_total_no_of_roads integer,
  geo_sum_of_commercial_traffic integer,
  geo_sum_of_non_commercial_traffic integer,
  geo_avg_commercial_traffic integer,
  geo_avg_non_commercial_traffic integer,
  geo_no_of_schools integer,
  geo_number_of_shopping_centre integer,
  geo_number_of_university integer,
  geo_populcation_growth_5yr character varying(50),
  geo_nearest_weather_station_id integer,
  geo_distance_nearest_weather_meter character varying(50)
)
WITH (
  OIDS=FALSE
);



-- Table: public.weather_data1(NSW Weather data)

CREATE TABLE public.weather_data1
(
  location_name character varying(50),
  post_code integer,
  temp_c character varying(5),
  dew_point character varying(5),
  rel_humidity character varying(5),
  press_hpa character varying(15),
  altitude_m integer,
  rain_mm character varying(5),
  low_temp_c character varying(5),
  high_temp_c character varying(5),
  latitude character varying(20),
  longtitude character varying(20)
)
WITH (
  OIDS=FALSE
);


-- Table: public.game_condition(Ideal weather condition for each sport)


CREATE TABLE public.game_condition
(
  game character varying(25),
  high_temp character varying(25),
  low_temp character varying(25),
  max_rain_limit character varying(25),
  max_dew_point character varying(25),
  min_relative_humidity character varying(25)
)
WITH (
  OIDS=FALSE
);




-- Table: public.final_dataset( Final processed data)


CREATE TABLE public.final_dataset
(
  location_name character varying(50),
  temp_c character varying(5),
  dew_point character varying(5),
  rel_humidity character varying(5),
  press_hpa character varying(15),
  altitude_m integer,
  rain_mm character varying(5),
  low_temp_c character varying(5),
  high_temp_c character varying(5),
  postcode integer,
  state character(20),
  poa_code character(20),
  area_sqkm character varying(50),
  latitude character varying(50),
  longitude character varying(50),
  time_stamp character varying(50),
  total_persons_males integer,
  total_persons_females integer,
  total_persons_persons integer,
  median_age_of_persons integer,
  median_total_personal_income_weekly integer,
  median_total_household_income_weekly integer,
  total_dwellings integer,
  geo_traffic_total_no_of_roads integer,
  geo_sum_of_commercial_traffic integer,
  geo_sum_of_non_commercial_traffic integer,
  geo_avg_commercial_traffic integer,
  geo_avg_non_commercial_traffic integer,
  geo_no_of_schools integer,
  geo_number_of_shopping_centre integer,
  geo_number_of_university integer,
  geo_populcation_growth_5yr character varying(50),
  geo_nearest_weather_station_id integer,
  geo_distance_nearest_weather_meter character varying(50),
  weather_condition character varying(50)
)
WITH (
  OIDS=FALSE
);


--Loading the final Calulated data set into the Postgres Table

copy final_dataset FROM '/home/ragavan/Downloads/Calculated_Weather_data.csv' DELIMITER ',' csv HEADER;




--Generating Output files with location/weather condition data  for Football 

copy 
(
select location_name, latitude ||','||longitude||','|| altitude_m as Position, time_stamp as Local_time, weather_condition,temp_c,press_hpa, rel_humidity  
from final_dataset a
	left join game_condition b on (a.temp_c between b.low_temp  and b.high_temp) 
	and rain_mm < max_rain_limit 
	and dew_point < max_dew_point
	where b.game = 'football'
)
to '/var/lib/postgresql/9.5/output_dataset_football.psv' DELIMITER  '|'


--Generating Output files with location/weather condition data  for Cricket 

copy 
(
select location_name, latitude ||','||longitude||','|| altitude_m as Position, time_stamp as Local_time, weather_condition,temp_c,press_hpa, rel_humidity  
from final_dataset a
	left join game_condition b on (a.temp_c between b.low_temp  and b.high_temp) 
	and rain_mm < max_rain_limit 
	and dew_point < max_dew_point
	where b.game = 'cricket'
)
to '/var/lib/postgresql/9.5/output_dataset_cricket.psv' DELIMITER  '|'




--Generating Output files with location/weather condition data  for Hockey 

copy 
(
select location_name, latitude ||','||longitude||','|| altitude_m as Position, time_stamp as Local_time, weather_condition,temp_c,press_hpa, rel_humidity  
from final_dataset a
	left join game_condition b on (a.temp_c between b.low_temp  and b.high_temp) 
	and rain_mm < max_rain_limit 
	and dew_point < max_dew_point
	where b.game = 'hockey'
)
to '/var/lib/postgresql/9.5/output_dataset_hockey.psv' DELIMITER  '|'




--Output file for other calculation/analytics and reporting.  

COPY 
(
select b.game,* from complete_data a
left join game_condition b on a.temp_c between b.low_temp  and b.high_temp
where b.game = 'hockey'
) 
to '/var/lib/postgresql/9.5/hockey_locations_list.psv' DELIMITER  '|' 


