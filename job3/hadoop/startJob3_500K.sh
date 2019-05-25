#!/bin/bash

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
				-D stream.num.map.output.key.fields=3 \
				-D mapreduce.partition.keypartitioner.options=-k1,1 \
				-files gs://bug-data/job3/hadoop/firstMapper.py,gs://bug-data/job3/hadoop/firstReducer.py,gs://bug-data/input/historical_stocks.csv \
				-mapper firstMapper.py \
				-reducer firstReducer.py \
				-input gs://bug-data/input/historical_stock_prices_500K.csv \
				-output gs://bug-data/output/job3_hadoop_tmp_500K \
&& 	\
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
				-D stream.num.map.output.key.fields=3 \
				-D mapreduce.job.reduces=1 \
				-files gs://bug-data/job3/hadoop/secondMapper.py,gs://bug-data/job3/hadoop/secondReducer.py \
				-mapper secondMapper.py \
				-reducer secondReducer.py \
				-input gs://bug-data/output/job3_hadoop_tmp_500K/part-00000 \
				-output gs://bug-data/output/job3_hadoop_500K