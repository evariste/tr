"""
Script can be used to populate prices data into a database that has previously
been initialised but the init-db script.
"""

import  urllib
import sqlite3
from tr_utils import *
import itertools

##########################################

# To persuade sqlite to interpret numpy data types correctly.
# http://stackoverflow.com/questions/11910584/sqlite3-writes-only-floating-point-numpy-arrays-not-integer-ones
sqlite3.register_adapter(np.int64, int)

# The name of the file containing the sqlite database.
dataDir = 'data'
dbFile = 'trade_data'
dbFileFull = dataDir + '/' + dbFile + '.db'


# Our database connection, later functions will rely on this.
con = sqlite3.connect(dbFileFull)


##########################################

def getPricesFromCSV(csvFile):
    """
    Read price data from a csv file
    :param csvFile:  THe file with the data. It is assumed to be in a fixed format (see below)
    :return: A list of prices, each element corresponds to a row of data.
    """

    #             Date,Open,High,Low,Close,Volume
    fieldTypes = 'S10,float,float,float,float,int'
    headerRows = 1
    data = list(np.genfromtxt(csvFile,  delimiter=',', 
                              dtype=fieldTypes,
                              skip_header=headerRows))
    return data


##########################################

def getPricesFromURL(ticker, start_date_str, end_date_str, verbose=False):
    """
    Get the price data from the google server for a given datae range.

    :param ticker: The stock's short name.
    :param start_date_str: Format YYYY-MM-DD
    :param end_date_str: As above

    :return: list of tuples, each with a line of prices for the date interval
    requested. Format is Date,Open,High,Low,Close,Volume.
    """

    y = int(start_date_str[0:4])
    m = int(start_date_str[5:7])
    d = int(start_date_str[8:10])
    start = datetime.date(y, m, d)

    y = int(end_date_str[0:4])
    m = int(end_date_str[5:7])
    d = int(end_date_str[8:10])
    end = datetime.date(y, m, d)

    start_str = start.strftime('%b %d, %Y')
    end_str = end.strftime('%b %d, %Y')

    prefix = 'LON:'

    url_string = ('http://www.google.com/finance/historical' +
                  '?q={:s}{:s}'.format(prefix, ticker) +
                  '&startdate={0}'.format(start_str) +
                  '&enddate={0}'.format(end_str) +
                  '&output=csv' )

    if verbose:
        print 'Getting following URL: {:s}'.format(url_string)

    csv = urllib.urlopen(url_string)
    csv_lines = csv.readlines()

    for i, line in enumerate(csv_lines):
      csv_lines[i] = line.split(',')

    # Ignore header row.
    return csv_lines[1:]


##########################################

def fixRawPriceData(data, ticker):
    """
    Convert dates for all rows in the list of data and add in the
    ticker name as a first element in each row. The data list is
    modified in place.

    :param data:   A list of rows of prices data.
    :param ticker: The name of the stock to insert at the start of each row.

    """
    for i, row in enumerate(data):
        row[0] = convertDateFormat(row[0])
        data[i] = (ticker,) + tuple(row)


##########################################

def getPricesSince(tickers, start_date_str, end_date_str=None):
    """
    Get the prices of a set of stocks since a given date and insert them into
    the database.

    :param tickers:        The short names of the stocks
    :param start_date_str: Start of date range.
    :param end_date_str:   End of date range.
    """

    if end_date_str == None:
        end_date_str = datetime.date.today().isoformat()

    print 'Collecting price data between the following dates: '
    print start_date_str
    print end_date_str

    for ticker in tickers:
        ticker = ticker.upper()

        data = getPricesFromURL(ticker, start_date_str, end_date_str)

        if len(data) < 1:
            print 'No data for ticker: {:s}'.format(ticker)
            continue

        print '{:s}: Obtained {:d} records'.format(ticker, len(data))

        fixRawPriceData(data, ticker)
        insertDataIntoDB(con, 'prices', data, verbose=False)

    print 'done'

##########################################

def updatePrices(tickers):
    """
    Find most recent date for each stock in the set given.
    Retrieve data for that stock since that date and insert the
    price data into the database.

    If no data exist in the database for a stock a default hard coded date is used. See below.


    :param tickers: The set of stocks to update.
    """

    end_date_str = datetime.date.today().isoformat()

    for ticker in tickers:
        ticker = ticker.upper()

        queryStr = ('SELECT Date FROM prices '
                    'WHERE ticker = ? '
                    'ORDER BY Date DESC '
                    'LIMIT 1')
        pars = (ticker,)
        r = executeQuery2(con, queryStr, pars)

        if len(r) > 0:
            start_date_str = str(r[0][0]).replace('/', '-')
        else:
            start_date_str = '2014-04-01'

        if start_date_str >= end_date_str:
            print '{:s} : Already up to date'.format(ticker)


        getPricesSince([ticker], start_date_str, end_date_str=end_date_str)

##########################################


def initial_set_up(dbCon):
    """
    Call to get prices into an empty database. Dates are hard coded but can be changed.

    :param dbCon: A database connection.

    No return, just updates the database entries.
    """
    start_date_str = '2014-04-01'
    end_date_str = '2016-03-31'

    tickers = executeQuery(dbCon, 'SELECT ticker FROM companies')
    tickers = list( itertools.chain(*tickers) )
    getPricesSince(tickers, start_date_str, end_date_str=end_date_str)


##########################################

def update():
    """
    Go through the database, for each stock, identify the most recent
    date and get all prices since that date. Add them intto the database.

    No return, just updates the database entries.
    """
    tickers = executeQuery(con, 'SELECT ticker FROM companies')
    tickers = list( itertools.chain(*tickers) )

    updatePrices(tickers)






