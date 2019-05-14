#!/usr/bin/env python

import sys
import csv

startRange = 2016
endRange = 2018

rangeValues = range(startRange, endRange + 1)

tickerToInfoMap = {}

#reading from the distributed cache
with open('../../dataset/historical_stocks_reduced.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    firstLine = True #ignore first line

    for row in csv_reader:
        if not firstLine:
            ticker, _, name, sector, _ = row
            if sector != 'N/A':
                tickerToInfoMap[ticker] = {'sector': sector, 'name': name}
        else:
            firstLine = False

for line in sys.stdin:
    #turn each row into a list of strings
    data = line.strip().split(',') 
    if len(data) == 8:
        ticker, _, close, _, _, _, _, date = data
        #ignore file's first row
        if ticker != 'ticker':
            year = int(date[0:4])

            # check if year is in range startRange-endRange
            # check if the ticker has a corresponding sector (we filter out tickers whose sectors are N/A)
            if year in rangeValues and ticker in tickerToInfoMap:
                sector = tickerToInfoMap[ticker]['sector']
                name = tickerToInfoMap[ticker]['name']
                print('{}\t{}\t{}\t{}'.format(name, date, sector, close))
