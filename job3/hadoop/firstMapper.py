#!/usr/bin/env python

import sys
import csv

STARTRANGE = 2016
ENDRANGE = 2018

rangeValues = range(STARTRANGE, ENDRANGE + 1)

tickerToInfoMap = {}

# reading from the distributed cache
with open('historical_stocks.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    # ignore first line
    firstLine = True

    for row in csv_reader:
        if not firstLine:
            ticker, _, name, sector, _ = row
            if sector != 'N/A':
                tickerToInfoMap[ticker] = {'sector': sector, 'name': name}
        else:
            firstLine = False

for line in sys.stdin:
    # turn each row into a list of strings
    data = line.strip().split(',') 
    if len(data) == 8:
        ticker, _, close, _, _, _, _, date = data
        # ignore file's first row
        if ticker != 'ticker':
            year = int(date[0:4])

            # check if year is in range startRange-endRange
            # check if the ticker has a corresponding sector (we filter out
            # tickers whose sectors are N/A)
            if year in rangeValues and ticker in tickerToInfoMap:
                sector = tickerToInfoMap[ticker]['sector']
                name = tickerToInfoMap[ticker]['name']
                print('{}\t{}\t{}\t{}\t{}'.format(name,
                                                  ticker,
                                                  date,
                                                  sector,
                                                  close))
