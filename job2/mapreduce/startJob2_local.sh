#!/bin/bash

mapred streaming \
				-D stream.num.map.output.key.fields=3 \
				-D mapreduce.partition.keypartitioner.options=-k1,1 \
				-D mapreduce.job.reduces=2 \
				-files mapper.py,reducer.py,../../dataset/historical_stocks.csv \
				-mapper mapper.py \
				-reducer reducer.py \
				-input input/historical_stock_prices.csv \
				-output output/job2_hadoop \
				-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner