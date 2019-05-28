set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE EXTERNAL TABLE historical_stock_prices_no_partition (
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

CREATE TABLE historical_stock_prices (
	ticker STRING, 
	open double, 
	close double, 
	adj_close double, 
	low double, 
	high double, 
	volume int, 
	data STRING)
PARTITIONED BY (year int)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',';


INSERT OVERWRITE TABLE historical_stock_prices 
PARTITION (year) 
SELECT hsp.ticker, hsp.open, hsp.close, hsp.adj_close, hsp.low, hsp.high, hsp.volume, hsp.data, YEAR(hsp.data) 
FROM historical_stock_prices_no_partition hsp
WHERE YEAR(hsp.data)>=1998;

DROP TABLE historical_stock_prices_no_partition;

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