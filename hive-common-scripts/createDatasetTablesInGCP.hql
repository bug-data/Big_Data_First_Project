CREATE EXTERNAL TABLE historical_stock_prices (
	ticker STRING, 
	open double, 
	close double, 
	adj_close double, 
	low double, 
	high double, 
	volume int, 
	data STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION 'gs://bug-data/input/historical_stock_prices/'
TBLPROPERTIES("skip.header.line.count"="1");

CREATE EXTERNAL TABLE historical_stock (
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
LOCATION 'gs://bug-data/input/historical_stocks/'
TBLPROPERTIES("skip.header.line.count"="1");
