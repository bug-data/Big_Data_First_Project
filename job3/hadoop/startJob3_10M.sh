#!/bin/bash

mapred streaming \
				-D stream.num.map.output.key.fields=3 \
				-files firstMapper.py,firstReducer.py,../../dataset/historical_stocks.csv \
				-mapper firstMapper.py \
				-reducer firstReducer.py \
				-input input/historical_stock_prices_10M.csv \
				-output output/job3_hadoop_tmp_10M \
&& 	\
mapred streaming \
				-D stream.num.map.output.key.fields=3 \
				-files secondMapper.py,secondReducer.py \
				-mapper secondMapper.py \
				-reducer secondReducer.py \
				-input output/job3_hadoop_tmp_10M/part-00000 \
				-output output/job3_hadoop_10M