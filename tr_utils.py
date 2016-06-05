"""
A set of utility functions used by the other scripts.
"""

import datetime, time
import numpy as np
from matplotlib import pyplot as plt


##########################################

def executeQuery(dbCon, query):
    """
    Run a specific query on the database and return the results.
    For this function, the query is a single string.

    :param dbCon: Database connection.
    :param query: String with a query.

    :return: A tuple containing the results of running query.
    """
    cur = dbCon.cursor()
    cur.execute(query)

    # Returns all results as a list of tuples
    return cur.fetchall()

##########################################

def executeQuery2(dbCon, query, pars):
    """
    Run a specific query on the database and return the results.
    This function takes a formatted string that has ? characters to indicate the values of fields in the query

    E.g. the query string can be
       'SELECT * FROM companies WHERE ticker LIKE ?'
    And the parameters can be in the tuple pars which is
       pars = ('N%',)

    :param dbCon: Database connection.
    :param query: String with a query.
    :param pars : Parameters to substitute into the query.

    :return: A tuple containing the results of running query.
    """
    cur = dbCon.cursor()
    cur.execute(query, pars)

    # Returns all results as a list of tuples
    return cur.fetchall()

##########################################

def insertDataIntoDB(dbCon, tableName, data, verbose=False):
    """
    Insert data into a specific table.

    :param dbCon: A database connection.

    :param tableName: The table into which the data should go

    :param data: A set of tuples with the required data. The elements in
                 each tuple should match the fields in the table.

    :param verbose: Set to True for more output.

    """
    fieldCount = len(data[0])

    cur = dbCon.cursor()

    checkCmd = ("SELECT name FROM sqlite_master " +
                "WHERE type='table' AND name='" +
                tableName + "' ;")
    cur.execute(checkCmd)
    nRows = len(cur.fetchall())

    if (nRows == 0):
        raise Exception('insertDataIntoDB: {:s}, no such table.'.format(tableName))

    cmd = ('INSERT OR IGNORE INTO ' + tableName +
           ' VALUES(' +
           ', '.join(['?'] * fieldCount) +
           ') ')

    if verbose:
        print "insertDataIntoDB:  ", cmd

    cur.executemany(cmd, data)

    dbCon.commit()

##########################################

def convertDateFormat(dateStr):
    """
    Convert date formats from DD-Month-YY to YYYY/MM/DD,
    e.g. from 23-Mar-16 to 2016/03/23.

    :param dateStr:  Input date.
    :return:  Converted date.
    """

    # TODO: dd = datetime.datetime.strptime(d, '%d-%b-%y')  ?
    d = time.strptime(dateStr, '%d-%b-%y')
    dd = datetime.date(d.tm_year, d.tm_mon, d.tm_mday)
    return dd.strftime('%Y/%m/%d')

##########################################

def getTimeAndPriceData(dbCon, ticker, startDate, endDate=None, price='Close'):
    """
    Get price data stored in the database for a particular stock.

    :param dbCon:     Database connection
    :param ticker:    The stock to look up
    :param startDate: Start date, inclusive
    :param endDate:   End date, exclusive, current date if not specified.

    :return:          Array of days, each represents an offsets from given start date.
                      Array of prices for each day in the date interval specfied.
                      Same size as the array of days.
    """


    if endDate == None:
        endDate = datetime.date.today().strftime('%Y/%m/%d')

    queryStr = ('SELECT Date, '+ price + ' FROM prices ' +
                'WHERE ticker = ? ' +
                'AND Date >= ? ' +
                'AND Date < ?')
    pars = (ticker, startDate, endDate)

    r = executeQuery2(dbCon, queryStr, pars)

    if len(r) < 1:
        return None


    prices = np.asarray( [x[1] for x in r] )
    dates = [x[0] for x in r]

    x = datetime.datetime.strptime(dates[0], '%Y/%m/%d')

    days = np.zeros(shape=(len(dates),))

    for i, d in enumerate(dates):
        y = datetime.datetime.strptime(d, '%Y/%m/%d')
        days[i] = (y-x).days

    return days, prices


##########################################

def plotTimeAndPriceData(dbCon, ticker, startDate, endDate=None, price='Close'):
    """
    Plot a time series of prices for a stock. Start at the given date, if not
    otherwise specified, price is the closing price.
    :param dbCon:     Database connection
    :param ticker:    The stock to look up
    :param startDate: Start date, inclusive
    :param endDate:   End date, exclusive, current date if not specified.
    :param price:     Open, High, Low, or Close (default)
    """

    data = getTimeAndPriceData(dbCon, ticker, startDate, price=price, endDate=endDate)

    if data == None:
        return

    days, prices = data
    plt.plot(days, prices)
    dlo = np.min(days)
    dhi = np.max(days)

    x = datetime.datetime.strptime(startDate, '%Y/%m/%d')

    dloStr = (x + datetime.timedelta(dlo)).strftime('%Y/%m/%d')
    dhiStr = (x + datetime.timedelta(dhi)).strftime('%Y/%m/%d')
    plt.xticks([dlo, dhi], [dloStr, dhiStr], rotation=-45)
    plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.2)
    plt.show()



#########################################
def getPricesForGroup(dbCon, tickers, start_date_str, end_date_str):
    """
    Get the closing prices for a set of stocks in a date interval.
    Some days in the interval may not have prices for all stocks.
    Function will find the common days on which prices are available
    for all stocks and return these along with the prices.

    Exclude stocks that have prices for fewer than 90% of days on which it is possible to have a price.

    Returns None if no data found.

    :param dbCon:     Database connection
    :param tickers:   The stocks to look up
    :param start_date_str: Start date, inclusive
    :param end_date_str:   End date, exclusive, current date if not specified.

    :return:
    An array of days of length nD, where nD is the nubmer of days on which all stocks have price data.
    A nC x nD array, where nC is the number of companies, each row containing the nD prices for a stock.
    A list of the stocks that have data for the interval.
    """

    daysAll = []
    pricesAll = []
    tickersAll = []

    maxDayCount = 0

    for ticker in tickers:
        data = getTimeAndPriceData(dbCon, ticker, start_date_str, endDate=end_date_str)
        if data == None:
            continue
        days, prices = data

        daysAll.append(days)
        pricesAll.append(prices)
        tickersAll.append(ticker)

        if len(days) > maxDayCount:
            maxDayCount = len(days)

    if len(daysAll) == 0:
        # Have not found any data
        return None

    excludeInds = []
    for n, days in enumerate(daysAll):
        if len(days) < 0.9 * maxDayCount:
            excludeInds.append(n)

    for n in excludeInds:
        daysAll = daysAll[:n] + daysAll[(n+1):]
        pricesAll = pricesAll[:n] + pricesAll[(n+1):]
        tickersAll = tickersAll[:n] + tickersAll[(n+1):]


    daysCommon = set(daysAll[0])
    for days in daysAll:
        daysCommon = daysCommon.intersection(set(days))

    daysCommon = np.asarray(list(daysCommon),dtype=np.int)

    for n, days in enumerate(daysAll):
        ix = np.in1d(days, daysCommon)
        pricesAll[n] = pricesAll[n][ix]


    return daysCommon, np.array(pricesAll), tickersAll