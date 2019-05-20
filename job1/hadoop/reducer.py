#!/usr/bin/env python
import sys
from datetime import datetime


# field position in each row
TICKER = 0
CLOSE = 2
LOW = 3
HIGH = 4
VOLUME = 5


# Number of rows to print to stdout
TOP_N = 10


# global variables
result = []
prevTicker = None
closePriceStartingValue = None
closePriceFinalValue = None
minLowPrice = sys.maxsize
maxHighPrice = - sys.maxsize
volumeSum = 0
volumeCount = 0


# utility function for appending a new item into result
def appendItemToList():
    closeDifference = closePriceFinalValue - closePriceStartingValue
    percentageChange = closeDifference/closePriceStartingValue
    avgVolume = volumeSum/volumeCount

    record = {'ticker': prevTicker,
              'percentageChange': percentageChange*100,
              'minLowPrice': minLowPrice,
              'maxHighPrice': maxHighPrice,
              'avgVolume': avgVolume
              }

    result.append(record)


# parse each value in value list
def parseValues(valueList):
    ticker = valueList[TICKER].strip()
    close = float(valueList[CLOSE].strip())
    low = float(valueList[LOW].strip())
    high = float(valueList[HIGH].strip())
    volume = float(valueList[VOLUME].strip())
    return [ticker, close, low, high, volume]

# main script
for line in sys.stdin:
    valueList = line.strip().split('\t')

    if len(valueList) == 6:
        ticker, close, low, high, volume = parseValues(valueList)

        if prevTicker and prevTicker != ticker:
            # key value changed. Append a new item into result list 
            # and update values for the new key
            appendItemToList()

            # update variable values
            closePriceStartingValue = close
            closePriceFinalValue = close
            minLowPrice = low
            maxHighPrice = high
            volumeSum = volume
            volumeCount = 1

        else:
            # key value unchanged (or this is the first row of the file).
            # in case this is the first row of the file
            if not prevTicker:
                closePriceStartingValue = close

            # Update values for the current key
            closePriceFinalValue = close
            minLowPrice = min(minLowPrice, low)
            maxHighPrice = max(maxHighPrice, high)
            volumeSum += volume
            volumeCount += 1
        
        prevTicker = ticker

# add last computed key into result
if prevTicker:
    appendItemToList()

sortedList = sorted(result, key=lambda k: k['percentageChange'], reverse=True)

# print sorted list
for i in range(TOP_N):
    item = sortedList[i]
    print('{}\t{}%\t{}\t{}\t{}'.format(item['ticker'],
                                       item['percentageChange'],
                                       item['minLowPrice'],
                                       item['maxHighPrice'],
                                       item['avgVolume']))