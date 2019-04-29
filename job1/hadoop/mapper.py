#!/usr/bin/env python

import sys

for line in sys.stdin:
    #turn each row into a list of strings
    data = line.strip().split(',') 
    if len(data) == 8:
        ticker, _, close, _, low, high, volume, date = data
        
        #first row of the file
        if ticker != 'ticker':
            key = ticker + '_' + date
            print('{}\t{} {} {} {}'.format(key, close, low, high, volume))
