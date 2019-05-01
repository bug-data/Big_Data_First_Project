#!/usr/bin/env python

import sys

startRange = 1998
endRange = 2018

rangeValues = range(startRange, endRange + 1)

for line in sys.stdin:
    #turn each row into a list of strings
    data = line.strip().split(',') 
    if len(data) == 8:
        ticker, _, close, _, low, high, volume, date = data
        
        #ignore file's first row
        if ticker != 'ticker':
            year = int(date[0:4])
            # check if year is in range startRange-endRange
            if year in rangeValues:
                print('{}\t{} {} {} {} {}'.format(ticker, close, low, high, volume, date))
