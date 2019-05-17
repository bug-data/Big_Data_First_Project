#!/usr/bin/env python
import sys

# field position of each row taken from stdin
PERCENTDIFF2016 = 0
PERCENTDIFF2017 = 1
PERCENTDIFF2018 = 2
NAME = 3
SECTOR = 4

# field position in percent triplet:
PERCENTDIFF2016INTRIPLET = 0
PERCENTDIFF2017INTRIPLET = 1
PERCENTDIFF2018INTRIPLET = 2

# global variables
prevPercentChangeTriplet = None

# list that stores info for a given company which has a given percent triplet
# so this list will be resetted on each new percent triplet
'''
[dict1, dict2, ..]
where dict_i has the following schema:
{name: companyName, sector: companySector}
'''
companyList = []


# *** utility functions ***

# utility function for printing a set of key value pairs
def writeRecord():
    listLength = len(companyList)
    for i in range(listLength - 1):
        for j in range(i, listLength):
            firstCompany = companyList[i]
            secondCompany = companyList[j]
            if firstCompany['sector'] != secondCompany['sector']:
                print('{}\t{}\t2016: {}%\t2017: {}%\t2018: {}%'
                      .format(firstCompany['name'],
                              secondCompany['name'],
                              prevPercentChangeTriplet[PERCENTDIFF2016INTRIPLET],
                              prevPercentChangeTriplet[PERCENTDIFF2017INTRIPLET],
                              prevPercentChangeTriplet[PERCENTDIFF2018INTRIPLET]
                              )
                      )


# add a new entry in companyList global variable
def addItemToList(sector, name):
    entry = {'sector': sector, 'name': name}
    companyList.append(entry)


# parse each value in value list
def parseValues(valueList):
    percentChange2016 = valueList[PERCENTDIFF2016].strip()
    percentChange2017 = valueList[PERCENTDIFF2017].strip()
    percentChange2018 = valueList[PERCENTDIFF2018].strip()
    name = valueList[NAME].strip()
    sector = valueList[SECTOR].strip()
    return ((percentChange2016, percentChange2017, percentChange2018),
            name,
            sector)


# main script
for line in sys.stdin:
    valueList = line.strip().split('\t')

    if len(valueList) == 5:
        percentChangeTriplet, name, sector = parseValues(valueList)

        if prevPercentChangeTriplet and \
           prevPercentChangeTriplet != percentChangeTriplet:
            # triplet changed.
            # So we write a new record for the previous company,
            # update global variables for the new company
            # and finally update company list for the new triplet
            writeRecord()

            # reset variable values
            prevPercentChangeTriplet = percentChangeTriplet

            # reset our list
            companyList = []

            # add a new entry in companyList
            addItemToList(sector, name)

        else:
            # key value unchanged (or this is the first row of the file).
            prevPercentChangeTriplet = percentChangeTriplet

            # add a new entry in companyList
            addItemToList(sector, name)

# print last computed key
if prevPercentChangeTriplet:
    writeRecord()