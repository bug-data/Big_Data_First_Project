CREATE VIEW IF NOT EXISTS ticker_min_max_avg AS 
SELECT ticker, min(low) AS min_price, max(high) AS max_price, avg(volume) AS avg_volume 
FROM historical_stock_prices 
WHERE YEAR(data)>=1998 AND YEAR(data)<=2018 GROUP BY ticker;

CREATE VIEW IF NOT EXISTS ticker_min_data AS 
SELECT ticker, min(TO_DATE(data)) AS min_data FROM historical_stock_prices 
WHERE YEAR(data)==1998 
GROUP BY ticker;

CREATE VIEW IF NOT EXISTS ticker_max_data AS 
SELECT ticker, max(TO_DATE(data)) AS max_data FROM historical_stock_prices 
WHERE YEAR(data)==2018 
GROUP BY ticker;

CREATE VIEW IF NOT EXISTS ticker_close_min_data AS 
SELECT h.ticker, h.data, h.close 
FROM ticker_min_data AS t, historical_stock_prices AS h 
WHERE h.ticker=t.ticker AND h.data=t.min_data;

CREATE VIEW IF NOT EXISTS ticker_close_max_data AS 
SELECT h.ticker, h.data, h.close 
FROM ticker_max_data AS t, historical_stock_prices AS h 
WHERE h.ticker=t.ticker AND h.data=t.max_data;

CREATE VIEW IF NOT EXISTS ticker_percentuale AS 
SELECT mi.ticker, ((ma.close-mi.close)/mi.close) AS inc_perc 
FROM ticker_close_max_data AS ma join ticker_close_min_data AS mi on ma.ticker=mi.ticker;

INSERT OVERWRITE DIRECTORY 'gs://bug-data/output/hive/job1/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' 
SELECT a.ticker, b.inc_perc, a.min_price, a.max_price, a.avg_volume 
FROM ticker_min_max_avg AS a join ticker_percentuale AS b on a.ticker=b.ticker 
ORDER BY b.inc_perc DESC limit 10;