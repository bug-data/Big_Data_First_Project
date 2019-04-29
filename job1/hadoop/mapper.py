#!/usr/bin/env python

import sys

startRange = 1998
endRange = 2018

rangeValues = range(startRange, endRange+1)

for line in sys.stdin:
    #turn each row into a list of strings
    data = line.strip().split(',') 
    if len(data) == 8:
        ticker, _, close, _, low, high, volume, date = data
        
        #check year interval range and if it is the first row of the file
        year = int(date[0:4])
        if ticker != 'ticker' and year in rangeValues:
            key = ticker + '_' + date
            print('{}\t{} {} {} {}'.format(key, close, low, high, volume))
