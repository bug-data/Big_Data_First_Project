#!/bin/bash

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
				-D stream.num.map.output.key.fields=3 \
				-files gs://bug-data/job3/hadoop/firstMapper.py,gs://bug-data/job3/hadoop/firstReducer.py,gs://bug-data/input/historical_stocks.csv \
				-mapper firstMapper.py \
				-reducer firstReducer.py \
				-input gs://bug-data/input/historical_stock_prices.csv \
				-output gs://bug-data/output/job3_hadoop_tmp \
&& 	\
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
				-D stream.num.map.output.key.fields=3 \
				-files gs://bug-data/job3/hadoop/secondMapper.py,gs://bug-data/job3/hadoop/secondReducer.py \
				-mapper secondMapper.py \
				-reducer secondReducer.py \
				-input gs://bug-data/output/job3_hadoop_tmp/part-00000 \
				-output gs://bug-data/output/job3_hadoop