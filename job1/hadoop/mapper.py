#!/usr/bin/env python

import sys

START_RANGE = 1998
END_RANGE = 2018

rangeValues = range(START_RANGE, END_RANGE + 1)

for line in sys.stdin:
    # turn each row into a list of strings
    data = line.strip().split(',') 
    if len(data) == 8:
        ticker, _, close, _, low, high, volume, date = data
        
        # ignore file's first row
        if ticker != 'ticker':
            year = int(date[0:4])
            # check if year is in range startRange-endRange
            if year in rangeValues:
                print('{}\t{}\t{}\t{}\t{}\t{}'.format(ticker,
                                                      date,
                                                      close,
                                                      low,
                                                      high,
                                                      volume))
