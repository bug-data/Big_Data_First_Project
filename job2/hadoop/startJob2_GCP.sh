#!/bin/bash

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
				-D stream.num.map.output.key.fields=3 \
				-D mapreduce.partition.keypartitioner.options=-k1,1 \
				-D mapreduce.job.reduces=4 \
				-files gs://bug-data/job2/hadoop/mapper.py,gs://bug-data/job2/hadoop/reducer.py,gs://bug-data/input/historical_stocks.csv \
				-mapper mapper.py \
				-reducer reducer.py \
				-input gs://bug-data/input/historical_stock_prices/historical_stock_prices.csv \
				-output gs://bug-data/output/job2_hadoop \
				-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner