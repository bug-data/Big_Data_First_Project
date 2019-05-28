from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SQLContext
import csv

def get_year(dateAsString):
	return int(dateAsString[:4])

def parse_line(line):
	csv_reader = csv.reader([line], delimiter=',')
	return next(csv_reader)


def min_close(x, y):
	if x[1] > y[1]:
		return y
	else:
		return x


def max_close(x, y):
	if x[1] < y[1]:
		return y
	else:
		return x


conf = SparkConf().setMaster("yarn").setAppName("Job2")
sc = SparkContext(conf=conf)
	
hsp = sc.textFile("gs://bug-data/input/historical_stock_prices/historical_stock_prices.csv") \
		.map(lambda line: parse_line(line)) \
		.filter(lambda line: line[0] != "ticker") \
		.filter(lambda line: get_year(line[7]) >= 2004 and get_year(line[7]) <= 2018)

hs = sc.textFile("gs://bug-data/input/historical_stocks/historical_stocks.csv") \
 	   .map(lambda line: parse_line(line)) \
 	   .filter(lambda line: line[0] != "ticker") \
	   .filter(lambda line: line[3] != "N/A")

join_hsp_hs = hsp \
			  .map(lambda line: (line[0], (line[2], line[6], line[7]))) \
			  .join(hs.map(lambda line: (line[0], (line[3]))))

# restituisce un rdd con ticker, close, volume, data, settore
join_hsp_hs = join_hsp_hs \
			  .map(lambda line: (line[0], float(line[1][0][0]), float(line[1][0][1]), line[1][0][2],
			  					 line[1][1]))

# persist the RDD
join_hsp_hs.persist(StorageLevel.MEMORY_AND_DISK)

# restituisce (settore,anno),somma volume
sum_volume = join_hsp_hs \
			 .map(lambda line: ((line[4], get_year(line[3])), line[2])) \
			 .reduceByKey(lambda x, y: x+y)

# input (settore,data), close - faccio la somma --> mappo su (settore e anno),
# somma dei close --> faccio la media --> restituisce (settore, 
# media dei close)
sum_close_avg = join_hsp_hs \
				.map(lambda line: ((line[4], line[3]), line[1])) \
				.reduceByKey(lambda x, y: x+y) \
				.map(lambda line: ((line[0][0], get_year(line[0][1])), (line[1], 1))) \
				.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) \
				.map(lambda line: (line[0], (line[1][0]/line[1][1])))

# input (ticker, settore, anno), (close, data) - trovo minima data --> mappo su (settore, anno), close --> restitusco (settore, anno), somma dei close
min_data_close = join_hsp_hs \
				 .map(lambda line: ((line[0], line[4], get_year(line[3])), (line[1], line[3]))) \
				 .reduceByKey(lambda x, y: min_close(x, y)) \
				 .map(lambda line: ((line[0][1], line[0][2]), line[1][0])) \
				 .reduceByKey(lambda x, y: x+y)

max_data_close = join_hsp_hs \
				 .map(lambda line: ((line[0], line[4], get_year(line[3])), (line[1], line[3]))) \
				 .reduceByKey(lambda x, y: max_close(x, y)) \
				 .map(lambda line: ((line[0][1], line[0][2]), line[1][0])) \
				 .reduceByKey(lambda x, y: x+y)

join_inc_perc = min_data_close.join(max_data_close)

inc_perc = join_inc_perc.map(lambda line: (line[0], round((line[1][1]-line[1][0])/line[1][0] * 100, 2) ))

result = inc_perc \
		 .join(sum_close_avg) \
		 .join(sum_volume) \
		 .sortBy(lambda a: a[0]) \
		 .map(lambda line: [line[0][0], line[0][1], line[1][0][0], line[1][0][1],
		                    line[1][1]])

result.coalesce(1).saveAsTextFile("gs://bug-data/output/spark/job2/output/")