CREATE TABLE IF NOT EXISTS first_pricipal_table AS 
SELECT hs.sector, hsp.ticker, hsp.data, hsp.close, hsp.volume 
FROM historical_stock_prices AS hsp JOIN historical_stock AS hs ON hsp.ticker=hs.ticker 
WHERE YEAR(hsp.data)>=2004 AND YEAR(hsp.data)<=2018 AND hs.sector!='N/A';

CREATE VIEW IF NOT EXISTS sector_data_volume AS 
SELECT sector, YEAR(data) AS anno, SUM(volume) AS somma_volume 
FROM first_pricipal_table 
GROUP BY sector, YEAR(data);

CREATE VIEW IF NOT EXISTS sector_data_min_max AS 
SELECT sector, ticker, min(TO_DATE(data)) AS min_data, max(TO_DATE(data)) AS max_data 
FROM first_pricipal_table 
GROUP BY sector, ticker, YEAR(data);

CREATE VIEW IF NOT EXISTS sector_data_close_min AS 
SELECT b.sector, YEAR(b.min_data) AS anno, SUM(a.close) AS min_close 
FROM first_pricipal_table AS a, sector_data_min_max AS b 
WHERE a.sector=b.sector AND a.data=b.min_data AND b.ticker=a.ticker 
GROUP BY b.sector, YEAR(b.min_data);

CREATE VIEW IF NOT EXISTS sector_data_close_max AS 
SELECT b.sector, YEAR(b.max_data) AS anno, SUM(a.close) AS max_close 
FROM first_pricipal_table AS a, sector_data_min_max AS b 
WHERE a.sector=b.sector AND a.data=b.max_data AND b.ticker=a.ticker 
GROUP BY b.sector, YEAR(b.max_data);

CREATE TABLE IF NOT EXISTS sector_data_close AS 
SELECT mi.sector, mi.anno, ROUND((ma.max_close-mi.min_close)/mi.min_close*100,2) AS perc_var_anno 
FROM sector_data_close_min AS mi, sector_data_close_max AS ma 
WHERE mi.sector=ma.sector AND mi.anno=ma.anno
ORDER BY sector, anno; 

CREATE VIEW IF NOT EXISTS sector_data_sum_close AS 
SELECT sector, data, SUM(close) AS somma 
FROM first_pricipal_table 
GROUP BY sector, data;

CREATE VIEW IF NOT EXISTS sector_data_avg_close AS 
SELECT sector, YEAR(data) AS anno, AVG(somma) AS media 
FROM sector_data_sum_close 
GROUP BY sector, YEAR(data);

INSERT OVERWRITE DIRECTORY 'gs://bug-data/output/hive/job2/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT a.sector, a.anno, c.somma_volume, b.perc_var_anno, a.media 
FROM sector_data_avg_close AS a, sector_data_close AS b, sector_data_volume AS c 
WHERE a.sector=b.sector AND b.sector=c.sector AND a.anno=b.anno AND c.anno=b.anno
ORDER BY sector, anno;




