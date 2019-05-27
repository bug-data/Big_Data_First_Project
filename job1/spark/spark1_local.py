from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[*]").setAppName("Job1")
sc = SparkContext(conf=conf)


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
input = sc.textFile("file:///Users/jgmathew/Documents/RomaTre/Magistrale/SecondoAnno/SecondoSemestre/BigData/FirstProject/dataset/historical_stock_prices.csv") \
		.map(lambda line: line.split(","))

input = input.filter(lambda line: line[0] != "ticker")

input = input.filter(lambda line: line[7][0:4] >= "1998" and
								  line[7][0:4] <= "2018")

min_ticker_low = input.map(lambda line: (line[0], float(line[4]))) \
					  .reduceByKey(lambda x, y: min(x, y))

max_ticker_high = input.map(lambda line: (line[0], float(line[5]))) \
					   .reduceByKey(lambda x, y: max(x, y))

avg_ticker_volume = input.map(lambda line: (line[0], (float(line[6]), 1))) \
	.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) \
	.map(lambda line: (line[0], (line[1][0]/line[1][1])))

min_data_close = input.map(lambda line: (line[0], (line[2], line[7]))) \
	.reduceByKey(lambda x, y: min_close(x, y)) \
	.filter(lambda line: line[1][1][0:4] == "1998")
	

max_data_close = input.map(lambda line: (line[0], (line[2], line[7]))) \
	.reduceByKey(lambda x, y: max_close(x, y))\
	.filter(lambda line: line[1][1][0:4] == "2018")

join_inc_perc = min_data_close.join(max_data_close)

inc_perc = join_inc_perc \
		   .map(lambda line: (line[0],
		   	                  (float(line[1][1][0]) - float(line[1][0][0]))/float(line[1][0][0])))

result = max_ticker_high.join(min_ticker_low) \
						.join(inc_perc) \
						.join(avg_ticker_volume) \
						.map(lambda x: [x[0], x[1][0][0][0], x[1][0][0][1], 
						     x[1][0][1], x[1][1]]).sortBy(lambda a: a[3], ascending=False) \
						.take(10)

sc.parallelize(result).coalesce(1) \
					  .saveAsTextFile("file:///Users/jgmathew/Documents/RomaTre/Magistrale/SecondoAnno/SecondoSemestre/BigData/FirstProject/job1/spark/output/")