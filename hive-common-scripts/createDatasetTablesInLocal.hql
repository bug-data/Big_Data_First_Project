CREATE TABLE historical_stock_prices (
	ticker STRING, 
	open double, 
	close double, 
	adj_close double, 
	low double, 
	high double, 
	volume int, 
	data STRING) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '../dataset/historical_stock_prices.csv' 
OVERWRITE INTO TABLE historical_stock_prices;

CREATE TABLE historical_stock (
	ticker STRING, 
	exchanges STRING, 
	name STRING, 
	sector STRING, 
	industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
       "separatorChar" = ",",
       "quoteChar"     = "\""
)  
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '../dataset/historical_stocks.csv' 
OVERWRITE INTO TABLE historical_stock;