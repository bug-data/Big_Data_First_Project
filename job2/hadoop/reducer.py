#!/usr/bin/env python
import sys

# field position of each row taken from stdin
SECTOR = 0
TICKER = 1
DATE = 2
CLOSE = 3
VOLUME = 4


# global variables
prevSector = None
prevTicker = None
prevYear = None
prevClose = 0
# data structures to avoid date comparison
# they store data for a SINGLE sector
# so they will be resetted on each sector
# first data structure
'''
key: year, value: dictionary {
    entireVolume: entireVolumeValue,
    closePriceStartingValue: closePriceStartingValue,
    closePriceFinalValue: closePriceFinalValue
    }
'''
yearToSectorTrend = {}

'''
key: year, value: dictionary {
    date: sumOfCloseValues
    }

where date is a string in YYYY-MM-DD format and
sumOfCloseValues is the sum of the close values of tickers which
share the same sector
'''
yearToSectorDailyClosePrice = {}

# *** utility functions ***


# utility function for printing a set of key value pairs
# TODO: edit this method
def writeRecord():
    for year in sorted(yearToSectorTrend.keys()):
        sectorTrend = yearToSectorTrend[year]
        sectorDailyClosePrices = yearToSectorDailyClosePrice[year]
        entireVolume = sectorTrend['entireVolume']
        percentChange = (sectorTrend['closePriceFinalValue'] - sectorTrend['closePriceStartingValue'])/sectorTrend['closePriceStartingValue']
        averageClosePrice = getDailyCloseAverage(sectorDailyClosePrices)
        print('{}\t{}\t{}\t{}\t{}'.format(
            prevSector,
            year,
            entireVolume,
            percentChange,
            averageClosePrice))


# given a dictionary returns the average of key values
def getDailyCloseAverage(yearToDailyClosePriceMap):
    count = len(yearToDailyClosePriceMap.keys())
    closeSum = sum(yearToDailyClosePriceMap.values())
    return closeSum/count


# add or set "value" to a data structure whose (first level) keys are years
def updateDataStructure(dataStructure, year, key, value):
    if year in dataStructure:
        if key in dataStructure[year]:
            dataStructure[year][key] += value
        else:
            dataStructure[year][key] = value
    else:
        dataStructure[year] = {}
        dataStructure[year][key] = value


# parse each value in value list
def parseValues(valueList):
    sector = valueList[SECTOR].strip()
    ticker = valueList[TICKER].strip()
    date = valueList[DATE].strip()
    close = float(valueList[CLOSE].strip())
    volume = int(valueList[VOLUME].strip())
    return (sector, ticker, date, close, volume)


# main script
for line in sys.stdin:
    valueList = line.strip().split('\t')

    if len(valueList) == 5:
        sector, ticker, date, close, volume = parseValues(valueList)
        year = date[0:4]

        if prevSector and prevSector != sector:
            # sector value (and consequently ticker value) changed.
            # So we set final close price for this ticker,
            # write a new record for the previous sector
            # and update global variables for the new sector
            updateDataStructure(yearToSectorTrend, prevYear, 'closePriceFinalValue', prevClose)
            writeRecord()

            # reset variable values
            prevSector = sector
            prevTicker = ticker
            prevYear = year
            prevClose = close
            # reset our dictionaries
            yearToSectorTrend = {}
            yearToSectorDailyClosePrice = {}

            # this is the first available date for this new ticker's record,
            # so we update closePriceStartingValue
            updateDataStructure(yearToSectorTrend, year, 'closePriceStartingValue', close)
            # update close and volume values for this sector in this year
            updateDataStructure(yearToSectorTrend, year, 'entireVolume', volume)
            updateDataStructure(yearToSectorDailyClosePrice, year, date, close)

        else:
            # key value unchanged (or this is the first row of the file). 
            prevSector = sector
            
            # update total close and volume values for this sector in this year
            updateDataStructure(yearToSectorTrend, year, 'entireVolume', volume)
            updateDataStructure(yearToSectorDailyClosePrice, year, date, close)

            # Two cases: same ticker or different ticker
            if prevTicker and prevTicker != ticker:
                # Case 1: Different ticker
                # this means that previous close value was the ending close 
                # value for the previous ticker in the previous year
                updateDataStructure(yearToSectorTrend, prevYear, 'closePriceFinalValue', prevClose)

                # this also means that the current close value is the 
                # first close value for this ticker
                updateDataStructure(yearToSectorTrend, year, 'closePriceStartingValue', close)
                
                # update global variables
                prevTicker = ticker
                prevYear = year
                prevClose = close

            else:
                # Case 2: same ticker or first row of the file
                # first row of the file
                if not prevTicker:
                    prevTicker = ticker
                    # this also means that the current close value is the 
                    # first close value for this ticker
                    updateDataStructure(yearToSectorTrend, year, 'closePriceStartingValue', close)

                # we need to establish if the current year has changed.
                # If so we need to update the final close price for 
                # the last ticker in the previous year and the 
                # starting close value for the same ticker in the current year
                if prevYear and prevYear != year:
                    updateDataStructure(yearToSectorTrend, prevYear, 'closePriceFinalValue', prevClose)
                    updateDataStructure(yearToSectorTrend, year, 'closePriceStartingValue', close)
                    
                # update global variables
                prevYear = year
                prevClose = close

# print last computed key
if prevSector:
    # this means that previous close value was the last value
    updateDataStructure(yearToSectorTrend, prevYear, 'closePriceFinalValue', prevClose)
    writeRecord()