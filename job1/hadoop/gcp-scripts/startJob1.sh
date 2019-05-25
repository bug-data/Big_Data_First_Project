#!/bin/bash

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
		-D stream.num.map.output.key.fields=2 \
		-D mapreduce.job.reduces=1 \
		-files gs://bug-data/job1/hadoop/mapper.py,gs://bug-data/job1/hadoop/reducer.py \
		-mapper mapper.py \
		-reducer reducer.py \
		-input gs://bug-data/input/historical_stock_prices.csv \
		-output gs://bug-data/output/job1_hadoop