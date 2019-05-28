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

conf = SparkConf().setMaster("local[*]").setAppName("Job3")
sc = SparkContext(conf=conf)

hsp = sc.textFile("file:///Users/jgmathew/Documents/RomaTre/Magistrale/SecondoAnno/SecondoSemestre/BigData/FirstProject/dataset/historical_stock_prices.csv") \
		.map(lambda line: parse_line(line)) \
		.filter(lambda line: line[0] != "ticker") \
	  	.filter(lambda line: get_year(line[7]) >= 2016 and get_year(line[7]) <= 2018)

hs = sc.textFile("file:///Users/jgmathew/Documents/RomaTre/Magistrale/SecondoAnno/SecondoSemestre/BigData/FirstProject/dataset/historical_stocks.csv") \
 	   .map(lambda line: parse_line(line)) \
 	   .filter(lambda line: line[0] != "ticker") \
	   .filter(lambda line: line[3] != "N/A")

join_hsp_hs = hsp \
			  .map(lambda line: (line[0], (line[2], line[7]))) \
			  .join(hs.map(lambda line: (line[0], (line[2], line[3]))))

# restituisce un rdd con ticker, close, data, name, settore
join_hsp_hs = join_hsp_hs.map(lambda line: (line[0], float(line[1][0][0]),
											line[1][0][1], line[1][1][0], line[1][1][1]))

# persist the RDD
join_hsp_hs.persist(StorageLevel.MEMORY_AND_DISK)

# input (ticker, settore, anno, name), (close, data) - trovo minima data --> 
# mappo su (settore, anno), close --> restitusco (settore, anno),
# somma dei close
min_data_close = join_hsp_hs \
				 .map(lambda line: ((line[0], line[4], get_year(line[2]), line[3]), (line[1],
				 																line[2]))) \
				 .reduceByKey(lambda x, y: min_close(x, y)) \
				 .map(lambda line: ((line[0][1], line[0][2], line[0][3]), line[1][0])) \
				 .reduceByKey(lambda x, y: x+y)
		
max_data_close = join_hsp_hs \
				 .map(lambda line: ((line[0], line[4], get_year(line[2]), line[3]), (line[1],
				 																line[2]))) \
				 .reduceByKey(lambda x, y: max_close(x, y)) \
				 .map(lambda line: ((line[0][1], line[0][2], line[0][3]), line[1][0])) \
				 .reduceByKey(lambda x, y: x+y)

join_inc_perc = min_data_close \
				.join(max_data_close)

inc_perc = join_inc_perc \
		   .map(lambda line: (line[0],
		   					  round((line[1][1]-line[1][0])/line[1][0] * 100, 0)))

three_row = inc_perc \
			.map(lambda line: ((line[0][0], line[0][2]), ([(line[0][1], line[1])]))) \
			.reduceByKey(lambda x, y: x+y)

three_row_clean = three_row.filter(lambda line: len(line[1]) == 3)


def order_three(x):
	temp = []
	if x[1][0][0] < x[1][1][0] and x[1][0][0] < x[1][2][0]:
		temp.append(x[1][0])
		if x[1][1][0] < x[1][2][0]:
			temp.append(x[1][1])
			temp.append(x[1][2])
		else:
			temp.append(x[1][2])
			temp.append(x[1][1])
	else:
		if x[1][1][0] < x[1][0][0] and x[1][1][0] < x[1][2][0]:
			temp.append(x[1][1])
			if x[1][0][0] < x[1][2][0]:
				temp.append(x[1][0])
				temp.append(x[1][2])
			else:
				temp.append(x[1][2])
				temp.append(x[1][0])
		else:
			temp.append(x[1][2])
			if x[1][0][0] < x[1][1][0]:
				temp.append(x[1][0])
				temp.append(x[1][1])
			else:
				temp.append(x[1][1])
				temp.append(x[1][0])
	return (x[0], temp)


three_row_clean_order = three_row_clean \
						.map(lambda line: order_three(line))


def no_sector(x, y):
	return x

result = three_row_clean_order \
		 .map(lambda line: (tuple(line[1]),([(line[0])]))) \
		 .reduceByKey(lambda x,y: x+y).filter(lambda line: len(line[1])>=2) \
		 .filter(lambda line: line[1][0][0]!=line[1][1][0]) \
		 .map(lambda line: (line[1][0][1],line[1][1][1],line[0])) \
		 .sortBy(lambda a: a[0])

result.coalesce(1).saveAsTextFile("file:///Users/jgmathew/Documents/RomaTre/Magistrale/SecondoAnno/SecondoSemestre/BigData/FirstProject/job3/spark/output/")