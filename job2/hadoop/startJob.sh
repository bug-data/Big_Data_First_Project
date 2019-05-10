#!/bin/bash

mapred streaming \
				-D stream.num.map.output.key.fields=3 \
				-files mapper.py,reducer.py,../../dataset/historical_stocks_reduced.csv \
				-mapper mapper.py \
				-reducer /bin/cat \
				-input input/historical_stock_prices_reduced.csv \
				-output output/job2_hadoop