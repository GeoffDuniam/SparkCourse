create external table if not exists housing_text(
longitude             DOUBLE,
latitude              DOUBLE,
housing_median_age    DOUBLE,
total_rooms           DOUBLE,
total_bedrooms        DOUBLE,
population            DOUBLE,
households            DOUBLE,
median_income         DOUBLE,
median_house_value    DOUBLE,
ocean_proximity       STRING
  )
  comment 'california housing data'
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION '/user/hive/warehouse/housing_text';
