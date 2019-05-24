#!/bin/bash

hadoop jar hadoop-streaming-2.9.2.jar \
		-D stream.num.map.output.key.fields=2 \
		-files gs://bug-data/job1/hadoop/mapper.py,gs://bug-data/job1/hadoop/reducer.py \
		-mapper mapper.py \
		-reducer reducer.py \
		-input gs://bug-data/input/historical_stock_prices_5M.csv \
		-output gs://bug-data/output/job1_hadoop_5M