#!/bin/bash

mapred streaming \
		-D stream.num.map.output.key.fields=2 \
		-D mapreduce.job.reduces=1 \
		-files mapper.py,reducer.py \
		-mapper mapper.py \
		-reducer reducer.py \
		-input input/historical_stock_prices_double.csv \
		-output output/job1_hadoop