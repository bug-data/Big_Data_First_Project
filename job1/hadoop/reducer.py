#!/usr/bin/env python
import sys
from datetime import datetime


#field position in each row
TICKER = 0
CLOSE = 1
LOW = 2
HIGH = 3
VOLUME = 4
DATE = 5

#Number of rows to print to stdout
TOP_N = 10


#global variables
result = []
prevTicker = None
closePriceStartingValue = None
startDate = None
closePriceFinalValue = None
lastDate = None
minLowPrice = sys.maxsize
maxHighPrice = - sys.maxsize
volumeSum = 0
volumeCount = 0


#utility function for appending a new item into result
def writeRecord():
    percentageChange = ((closePriceFinalValue - closePriceStartingValue)/closePriceStartingValue)*100
    avgVolume = volumeSum/volumeCount

    record = {  'ticker': prevTicker, 
                'percentageChange': percentageChange,
                'minLowPrice': minLowPrice,
                'maxHighPrice': maxHighPrice,
                'avgVolume': avgVolume
             }

    result.append(record)


#parse each value in value list
def parseValues(valueList):
    ticker = valueList[TICKER].strip()
    close = float(valueList[CLOSE].strip())
    low = float(valueList[LOW].strip())
    high = float(valueList[HIGH].strip())
    volume = int(valueList[VOLUME].strip())
    date = datetime.strptime(valueList[DATE].strip(), '%Y-%m-%d')
    return [ticker, close, low, high, volume, date]

#main script
for line in sys.stdin:
    valueList = line.strip().split('\t')

    if len(valueList) == 6:
        ticker, close, low, high, volume, date = parseValues(valueList)

        if prevTicker and prevTicker != ticker:
            #key value changed. Append a new item into result list and update values for the new key
            writeRecord()

            #update variable values
            prevTicker = ticker
            closePriceStartingValue= close
            closePriceFinalValue = close
            startDate = date
            lastDate = date
            minLowPrice = low
            maxHighPrice = high
            volumeSum = volume
            volumeCount = 1

        else:
            #key value unchanged (or this is the first row of the file). 
            #in case this is the first row of the file
            if not prevTicker:
                startDate = date
                lastDate = date
            
            #Update values for the current key
            prevTicker = ticker

            if date < startDate:
                startDate = date
                closePriceStartingValue = close
            
            elif date > lastDate:
                lastDate = date
                closePriceFinalValue = close
            
            minLowPrice = min(minLowPrice, low)
            maxHighPrice = max(maxHighPrice, high)
            
            volumeSum += volume
            volumeCount += 1

#add last computed key into result
if prevTicker:
    writeRecord()

sortedList = sorted(result, key = lambda k: k['percentageChange'], reverse=True)
#print sorted list


for item in sortedList:
    if TOP_N > 0:
        print('{}\t{}%\t{}\t{}\t{}'.format(item['ticker'], item['percentageChange'], item['minLowPrice'], item['maxHighPrice'], item['avgVolume']))
        TOP_N -= 1

#TODO: set (possibly) a combiner and tune the number of reducer