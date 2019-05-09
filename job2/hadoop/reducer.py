#!/usr/bin/env python
import sys
from datetime import datetime


#field position in each row
SECTOR = 0
TICKER = 1
DATE = 2
CLOSE = 3
VOLUME = 4


#global variables
prevSector = None
prevTicker = None
prevYear = None
prevClose = 0
#data structure to avoid date comparison
#it stores data for a SINGLE sector
#so it will be resetted on each sector
'''
key: year, value: dictionary {
    entireVolume: entireVolumeValue, 
    closePriceStartingValue: closePriceStartingValue, 
    closePriceFinalValue: closePriceFinalValue,
    totalClosePrice: totalClosePriceValue,
    closePriceCount: closePriceCountValue
    }
'''
yearToSectorTrend = {}

# *** utility functions *** 

#utility function for printing a set of key value pairs
def writeRecord():
    for year, sectorTrend in yearToSectorTrend.items():
        entireVolume = sectorTrend['entireVolume']
        percentChange = (sectorTrend['closePriceFinalValue'] - sectorTrend['closePriceStartingValue'])/sectorTrend['closePriceStartingValue']
        averageClosePrice = sectorTrend['totalClosePrice']/sectorTrend['closePriceCount']
        print('{}\t{}\t{}\t{}\t{}'.format(prevSector, year, entireVolume, percentChange, averageClosePrice))

#add or set "value" to yearToSectorTrend[year] dictionary based on key presence
def updateSectorTrend(year, key, value):
    if key in yearToSectorTrend[year]:
        yearToSectorTrend[year] += value
    else: 
        yearToSectorTrend[year] = value

#parse each value in value list
def parseValues(valueList):
    sector = valueList[SECTOR].strip()
    ticker = valueList[TICKER].strip()
    year = valueList[DATE].strip()[0:4]
    close = float(valueList[CLOSE].strip())
    volume = float(valueList[VOLUME].strip())
    return [sector, ticker, year, close, volume]


#main script
for line in sys.stdin:
    valueList = line.strip().split('\t')

    if len(valueList) == 5:
        sector, ticker, year, close, volume = parseValues(valueList)

        #update total close and volume values for this sector in this year
        updateSectorTrend(year, 'totalClosePrice', close)
        updateSectorTrend(year, 'entireVolume', volume)
        updateSectorTrend(year, 'closePriceCount', 1)

        if prevSector and prevSector != sector:
            #sector value (and consequently ticker value) changed. 
            # So we set final close price for this ticker,
            # write a new record for the previous sector 
            # and update global variables for the new sector
            writeRecord()

            #reset variable values
            prevSector = sector
            prevTicker = ticker
            prevYear = year
            prevClose = close
            # reset our dictionary
            yearToSectorTrend = {}

            #this is the first available date for this new ticker's record, so we update closePriceStartingValue
            updateSectorTrend(year, 'closePriceStartingValue', close)
        else:
            #key value unchanged (or this is the first row of the file). 
            prevSector = sector
            
            #Two cases: same ticker or different ticker

            #Different ticker
            if prevTicker and prevTicker != ticker:
                #this means that previous close value was the last value
                updateSectorTrend(prevYear, 'closePriceFinalValue', prevClose)

                #this also means that the current close value is the first close value for this tickers
                updateSectorTrend(year, 'closePriceStartingValue', close)
                
                #update global variables
                prevTicker = ticker
                prevYear = year
                prevClose = close

            #same ticker or first row of the file
            else:
                
                #first row of the file
                if not prevTicker:
                    prevTicker = ticker
                    #this also means that the current close value is the first close value for this tickers
                    updateSectorTrend(year, 'closePriceStartingValue', close)

                #update global variables
                prevTicker = ticker
                prevYear = year
                prevClose = close

#add last computed key into result
if prevSector:
    #this means that previous close value was the last value
    updateSectorTrend(prevYear, 'closePriceFinalValue', prevClose)
    writeRecord()