CREATE TABLE IF NOT EXISTS table_ex3 AS 
SELECT hs.name, hs.sector, hsp.data, hsp.close, hs.ticker
FROM historical_stock_prices AS hsp JOIN historical_stock AS hs on hsp.ticker=hs.ticker
WHERE YEAR(data)>=2016 AND YEAR(data)<=2018 AND hs.sector!='N/A';

CREATE VIEW IF NOT EXISTS name_data_min_max AS 
SELECT name, ticker, sector, YEAR(data) AS anno, min(TO_DATE(data)) AS min_data, max(TO_DATE(data)) AS max_data 
FROM table_ex3 
GROUP BY name, ticker, sector, YEAR(data);

CREATE VIEW IF NOT EXISTS name_data_close_min AS 
SELECT b.name, a.sector, YEAR(b.min_data) AS anno, SUM(a.close) AS min_close 
FROM table_ex3 AS a, name_data_min_max AS b 
WHERE a.sector=b.sector AND a.data=b.min_data AND b.ticker=a.ticker
GROUP BY b.name,a.sector, YEAR(b.min_data);

CREATE VIEW IF NOT EXISTS name_data_close_max AS 
SELECT b.name, a.sector, YEAR(b.max_data) AS anno, SUM(a.close) AS max_close 
FROM table_ex3 AS a, name_data_min_max AS b 
WHERE a.sector=b.sector AND a.data=b.max_data AND a.ticker=b.ticker
GROUP BY b.name, a.sector, YEAR(b.max_data); 

CREATE TABLE IF NOT EXISTS name_anno_close AS 
SELECT mi.name, mi.sector, mi.anno, ROUND(((ma.max_close-mi.min_close)/mi.min_close *100 ) , 0) AS perc_var_anno
FROM name_data_close_min AS mi, name_data_close_max AS ma
WHERE mi.name=ma.name AND mi.anno=ma.anno AND mi.sector=ma.sector
ORDER BY name, sector, anno;

CREATE TABLE IF NOT EXISTS name_anno_close_JOIN AS
SELECT n1.name AS name1, n2.name AS name2, n1.anno, n1.perc_var_anno
FROM name_anno_close AS n1, name_anno_close AS n2
WHERE n1.name!=n2.name AND n1.sector!=n2.sector AND n1.anno=n2.anno AND n1.perc_var_anno=n2.perc_var_anno;

INSERT OVERWRITE LOCAL DIRECTORY 'output/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT DISTINCT a.name1, a.name2, a.anno AS anno1, a.perc_var_anno AS perc_var_anno1, b.anno AS anno2, b.perc_var_anno AS perc_var_anno2, c.anno AS anno3, c.perc_var_anno AS perc_var_anno3
FROM name_anno_close_JOIN AS a, name_anno_close_JOIN AS b, name_anno_close_JOIN AS c
WHERE a.name1=b.name1 AND b.name1=c.name1 AND a.name2=b.name2 AND b.name2=c.name2 AND a.anno=2016 AND b.anno=2017 AND c.anno=2018
ORDER BY name1, name2;




