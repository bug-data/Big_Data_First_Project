#!/usr/bin/env python
import sys

# field position of each row taken from stdin
NAME = 0
TICKER = 1
DATE = 2
SECTOR = 3
CLOSE = 4

STARTRANGE = 2016
ENDRANGE = 2018

RECORD_LENGTH = 5

rangeValues = list(range(STARTRANGE, ENDRANGE + 1))

# global variables
prevName = None
prevTicker = None
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

    # First, let's check that a non null value exists for each year
    if all(str(year) in yearToCompanyTrend for year in rangeValues):

        yearToCompanyTrendKeys = yearToCompanyTrend.keys()
        # the last two curly brackets are placeholder
        # for company's sector and name
        listOfSquareBrackets = ['{}'] * len(yearToCompanyTrendKeys) + ['{}', '{}']
        
        formattedString = '\t'.join(listOfSquareBrackets)
        # percentChangeMap = {'2016': None, '2017': None, '2018': None}
        # percentChangeMap is a dictionary whose keys are
        # years (taken from the yearToCompanyTrend keys) and
        # values are None (temporary)

        percentChangeMap = {year: None for year in yearToCompanyTrendKeys}

        for year in sorted(yearToCompanyTrend.keys()):
            sectorTrend = yearToCompanyTrend[year]
            closePriceFinalValue = sectorTrend['closePriceFinalValue']
            closePriceStartingValue = sectorTrend['closePriceStartingValue']
            closeDifference = closePriceFinalValue - closePriceStartingValue
            percentChange = closeDifference/closePriceStartingValue
            percentChangeMap[year] = int(round(percentChange*100))

        sortedPercentChangeMapKeys = sorted(percentChangeMap)
        sortedPercentChangeMapValues = [percentChangeMap[year] for year
                                        in sortedPercentChangeMapKeys]
        valuesToPrint = sortedPercentChangeMapValues + [prevName, prevSector]
        print(formattedString.format(*(valuesToPrint)))


# add or set "value" to yearToCompanyTrend[year]
# dictionary based on key existence
def updateCompanyTrend(dataStructure, year, key, value):
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
    name = valueList[NAME].strip()
    ticker = valueList[TICKER].strip()
    year = valueList[DATE].strip()[0:4]
    sector = valueList[SECTOR].strip()
    close = float(valueList[CLOSE].strip())
    return (name, ticker, year, sector, close)


# main script
for line in sys.stdin:
    valueList = line.strip().split('\t')

    if len(valueList) == RECORD_LENGTH:
        name, ticker, year, sector, close = parseValues(valueList)

        if prevName and prevName != name:
            # company name changed.
            # So we set final close price for the previous company
            # (company's ticker) in the previous year, write a new record for
            # the previous company and reset our dictionary
            updateCompanyTrend(yearToCompanyTrend,
                               prevYear,
                               'closePriceFinalValue',
                               prevClose)
            writeRecord()

            # reset our dictionary
            yearToCompanyTrend = {}

            # this is the first available date for this new company's record
            # so we update closePriceStartingValue
            updateCompanyTrend(yearToCompanyTrend,
                               year,
                               'closePriceStartingValue',
                               close)

        else:
            # key value unchanged (or this is the first row of the file).

            # Two cases: same ticker or different ticker
            # That is because a same company may have different
            # tickers
            if prevTicker and prevTicker != ticker:
                # Case 1: Different tickers
                # this means that previous close value was the ending
                # close value for the previous ticker in the previous year
                updateCompanyTrend(yearToCompanyTrend,
                                   prevYear,
                                   'closePriceFinalValue',
                                   prevClose)

                # this also means that the current close value is the first
                # close value for this ticker in this year
                updateCompanyTrend(yearToCompanyTrend,
                                   year,
                                   'closePriceStartingValue',
                                   close)

            else:
                # Case 2: same ticker or first row of the file
                # first row of the file
                
                # Two cases: same year or different year
                if prevYear and prevYear != year:
                    # Case 1: Different year
                    # this means that previous close value was the ending
                    # close value for the current ticker in the previous year
                    updateCompanyTrend(yearToCompanyTrend,
                                       prevYear,
                                       'closePriceFinalValue',
                                       prevClose)

                    # this also means that the current close value is the first
                    # close value for this company in this year
                    updateCompanyTrend(yearToCompanyTrend,
                                       year,
                                       'closePriceStartingValue',
                                       close)
                else:
                    # Another case: same year or first row of the file
                    # first row of the file
                    if not prevYear:
                        # this also means that the current close value is the
                        # first close value for this company in this year
                        updateCompanyTrend(yearToCompanyTrend,
                                           year,
                                           'closePriceStartingValue',
                                           close)
                                           
        # reset variable values
        prevName = name
        prevTicker = ticker
        prevYear = year
        prevSector = sector
        prevClose = close

# print last computed key
if prevName:
    # this means that previous close value was the last value
    updateCompanyTrend(yearToCompanyTrend,
                       prevYear,
                       'closePriceFinalValue',
                       prevClose)
    writeRecord()