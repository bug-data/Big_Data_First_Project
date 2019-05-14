#!/usr/bin/env python
import sys

# field position of each row taken from stdin
NAME = 0
DATE = 1
SECTOR = 2
CLOSE = 3


# global variables
prevName = None
prevYear = None
prevSector = None
prevClose = None
# data structure to avoid date comparison
# it stores data for a SINGLE company name
# so it will be resetted on each company name
'''
key: year, value: dictionary {
    closePriceStartingValue: closePriceStartingValue, 
    closePriceFinalValue: closePriceFinalValue,
    }
'''
yearToCompanyTrend = {}


# *** utility functions ***

# utility function for printing a set of key value pairs
def writeRecord():
    percentChangeMap = {'2016': None, '2017': None, '2018': None}
    for year in sorted(yearToCompanyTrend.keys()):
        sectorTrend = yearToCompanyTrend[year]
        percentChange = (sectorTrend['closePriceFinalValue'] - sectorTrend['closePriceStartingValue'])/sectorTrend['closePriceStartingValue']
        percentChangeMap[year] = round(percentChange*100)

    print('{}\t{}\t{}\t{}\t{}'.format(percentChangeMap['2016'],
                                      percentChangeMap['2017'],
                                      percentChangeMap['2018'],
                                      prevName,
                                      prevSector))


# add or set "value" to yearToCompanyTrend[year]
# dictionary based on key existence
def updateCompanyTrend(year, key, value):
    if year in yearToCompanyTrend:
            yearToCompanyTrend[year][key] = value
    else:
        yearToCompanyTrend[year] = {}
        yearToCompanyTrend[year][key] = value


# parse each value in value list
def parseValues(valueList):
    name = valueList[NAME].strip()
    year = valueList[DATE].strip()[0:4]
    sector = valueList[SECTOR].strip()
    close = float(valueList[CLOSE].strip())
    return (name, year, sector, close)


# main script
for line in sys.stdin:
    valueList = line.strip().split('\t')

    if len(valueList) == 4:
        name, year, sector, close = parseValues(valueList)

        if prevName and prevName != name:
            # company name changed.
            # So we set final close price for the previous company
            # in the previous year, write a new record for the previous company
            # and update global variables for the new company
            updateCompanyTrend(prevYear, 'closePriceFinalValue', prevClose)
            writeRecord()

            # reset variable values
            prevName = name
            prevYear = year
            prevSector = sector
            prevClose = close
            # reset our dictionary
            yearToCompanyTrend = {}

            # this is the first available date for this new company's record
            # so we update closePriceStartingValue
            updateCompanyTrend(year, 'closePriceStartingValue', close)

        else:
            # key value unchanged (or this is the first row of the file).
            prevName = name

            # Two cases: same year or different year
            if prevYear and prevYear != year:
                # Case 1: Different year
                # this means that previous close value was the ending
                # close value for the current company in the previous year
                updateCompanyTrend(prevYear, 'closePriceFinalValue', prevClose)

                # this also means that the current close value is the first
                # close value for this company in this year
                updateCompanyTrend(year, 'closePriceStartingValue', close)
                
                # update global variables
                prevYear = year
                prevSector = sector
                prevClose = close

            else:
                # Case 2: same year or first row of the file
                # first row of the file
                if not prevYear:
                    # this also means that the current close value is the
                    # first close value for this company in this year
                    updateCompanyTrend(year, 'closePriceStartingValue', close)
                    
                # update global variables
                prevYear = year
                prevClose = close
                prevSector = sector

# print last computed key
if prevName:
    # this means that previous close value was the last value
    updateCompanyTrend(prevYear, 'closePriceFinalValue', prevClose)
    writeRecord()