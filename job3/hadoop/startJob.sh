#!/bin/bash

mapred streaming \
				-D stream.num.map.output.key.fields=3 \
				-files firstMapper.py,firstReducer.py,../../dataset/historical_stocks.csv \
				-mapper firstMapper.py \
				-reducer firstReducer.py \
				-input input/historical_stock_prices.csv \
				-output output/job3_hadoop_tmp \
&& 	\
mapred streaming \
				-D stream.num.map.output.key.fields=3 \
				-files secondMapper.py,secondReducer.py \
				-mapper secondMapper.py \
				-reducer secondReducer.py \
				-input output/job3_hadoop_tmp/part-00000 \
				-output output/job3_hadoop