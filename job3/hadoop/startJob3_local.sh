#!/bin/bash

mapred streaming \
				-D stream.num.map.output.key.fields=3 \
				-D mapreduce.partition.keypartitioner.options=-k1,1 \
				-D mapreduce.job.reduces=2 \
				-files firstMapper.py,firstReducer.py,../../dataset/historical_stocks.csv \
				-mapper firstMapper.py \
				-reducer firstReducer.py \
				-input input/historical_stock_prices.csv \
				-output output/job3_hadoop_tmp \
				-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
&& 	\
mapred streaming \
				-D stream.num.map.output.key.fields=3 \
				-D mapreduce.job.reduces=1 \
				-files secondMapper.py,secondReducer.py \
				-mapper secondMapper.py \
				-reducer secondReducer.py \
				-input output/job3_hadoop_tmp/part-* \
				-output output/job3_hadoop