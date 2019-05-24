#!/bin/bash

mapred streaming \
		-D stream.num.map.output.key.fields=2 \
		-files mapper.py,reducer.py \
		-mapper mapper.py \
		-reducer reducer.py \
		-input input/historical_stock_prices_10M.csv \
		-output output/job1_hadoop_10M