#!/bin/bash

mapred streaming \
				-D stream.num.map.output.key.fields=3 \
				-files mapper.py,reducer.py,../../dataset/historical_stocks.csv \
				-mapper mapper.py \
				-reducer reducer.py \
				-input input/historical_stock_prices_10M.csv \
				-output output/job2_hadoop_10M