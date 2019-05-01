#!/bin/bash

mapred streaming -file mapper.py -file reducer.py -mapper mapper.py -reducer reducer.py -input input/historical_stock_prices.csv -output output/job1_hadoop