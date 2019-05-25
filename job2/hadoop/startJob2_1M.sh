#!/bin/bash

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
				-D stream.num.map.output.key.fields=3 \
				-D mapreduce.partition.keypartitioner.options=-k1 \
				-files gs://bug-data/job2/hadoop/mapper.py,gs://bug-data/job2/hadoop/reducer.py,gs://bug-data/input/historical_stocks.csv \
				-mapper mapper.py \
				-reducer reducer.py \
				-input gs://bug-data/input/historical_stock_prices_1M.csv \
				-output gs://bug-data/output/job2_hadoop_1M