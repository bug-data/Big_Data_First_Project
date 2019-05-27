from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


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


conf = SparkConf().setMaster("local[*]").setAppName("Job2")
sc = SparkContext(conf=conf)
	
sqlContext = SQLContext(sc)
hsp = sqlContext \
	  .read \
	  .format('com.databricks.spark.csv') \
	  .options(header='true', inferschema='true', quote='"', delimiter=',') \
	  .load("../../dataset/historical_stock_prices.csv").rdd

hsp = hsp \
	 .filter(lambda line: line[7].year >= 2004 and line[7].year <= 2018)

hs = sqlContext.read.format('com.databricks.spark.csv') \
    .options(header='true', inferschema='true', quote='"', delimiter=',') \
    .load("../../dataset/historical_stocks.csv").rdd

hs = hs.filter(lambda line: line[3] != "N/A")

join_hsp_hs = hsp \
			  .map(lambda line: (line[0], (line[2], line[6], line[7]))) \
			  .join(hs.map(lambda line: (line[0], (line[3]))))

# restituisce un rdd con ticker, close, volume, data, settore
join_hsp_hs = join_hsp_hs \
			  .map(lambda line: (line[0], line[1][0][0], line[1][0][1], line[1][0][2],
			  					 line[1][1]))

# restituisce (settore,anno),somma volume
sum_volume = join_hsp_hs \
			 .map(lambda line: ((line[4], line[3].year), line[2])) \
			 .reduceByKey(lambda x, y: x+y)

# input (settore,data), close - faccio la somma --> mappo su (settore e anno),
# somma dei close --> faccio la media --> restituisce (settore, 
# media dei close)
sum_close_avg = join_hsp_hs \
				.map(lambda line: ((line[4], line[3]), line[1])) \
				.reduceByKey(lambda x, y: x+y) \
				.map(lambda line: ((line[0][0], line[0][1].year), (line[1], 1))) \
				.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) \
				.map(lambda line: (line[0], (line[1][0]/line[1][1])))

# input (ticker, settore, anno), (close, data) - trovo minima data --> mappo su (settore, anno), close --> restitusco (settore, anno), somma dei close
min_data_close = join_hsp_hs \
				 .map(lambda line: ((line[0], line[4], line[3].year), (line[1], line[3]))) \
				 .reduceByKey(lambda x, y: min_close(x, y)) \
				 .map(lambda line: ((line[0][1], line[0][2]), line[1][0])) \
				 .reduceByKey(lambda x, y: x+y)

max_data_close = join_hsp_hs \
				 .map(lambda line: ((line[0], line[4], line[3].year), (line[1], line[3]))) \
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

sc.parallelize(result.collect()).coalesce(1).saveAsTextFile("output/results.txt")


