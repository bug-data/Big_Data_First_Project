#!/usr/bin/env python

import sys
import csv

startRange = 2004
endRange = 2018

rangeValues = range(startRange, endRange + 1)

tickerToSectorMap = {}

#reading from the distributed cache
with open('historical_stocks.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    firstLine = True

    for row in csv_reader:
        if not firstLine:
            ticker, _, _, sector, _ = row
            if sector != 'N/A':
                tickerToSectorMap[ticker] = sector
        else:
            firstLine = False

for line in sys.stdin:
    #turn each row into a list of strings
    data = line.strip().split(',') 
    if len(data) == 8:
        ticker, _, close, _, _, _, volume, date = data
        #ignore file's first row
        if ticker != 'ticker':
            year = int(date[0:4])

            # check if year is in range startRange-endRange
            # check if the ticker has a corresponding sector (we filter out tickers whose sectors are N/A)
            if year in rangeValues and ticker in tickerToSectorMap:
                sector = tickerToSectorMap[ticker]
                print('{}\t{}\t{}\t{}\t{}'.format(sector, ticker, date, close, volume))
