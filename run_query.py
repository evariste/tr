"""
A set of short functions to test the different functions in tr_utils script.

Assumes the database has already been set up and populated with company and price data.

Calls can be made at end of file.

"""


import sqlite3
import numpy as np
import itertools
from sklearn.preprocessing import scale
from tr_utils import *

##########################################

# Need to persuade sqlite to interpret numpy data types correctly.
# http://stackoverflow.com/questions/11910584/sqlite3-writes-only-floating-point-numpy-arrays-not-integer-ones
sqlite3.register_adapter(np.int64, int)


dataDir = 'data'
dbFile = 'trade_data'
dbFileFull = dataDir + '/' + dbFile + '.db'

# Our database connection, later functions will rely on this.
con = sqlite3.connect(dbFileFull)

##########################################

def checkA():
    t='ADM'

    s = "SELECT * FROM companies WHERE ticker = '{:s}'".format(t)

    r = executeQuery(con, s)
    #
    print r

##########################################
def checkB():
    mySector = 'insurance'

    s = ("SELECT companyName, ticker, marketCap " +
        "FROM companies " +
        "WHERE lower(sector) = '{:s}' ".format(mySector) +
        "ORDER BY marketCap")


    r = executeQuery(con, s)
    #
    for name, ti, cap in r:
        print '{:s} ({:s} : {:0.2f})'.format(name, ti, cap)


##########################################
def checkC():
    s = ("SELECT Ticker, Date, Close FROM prices " +
         "WHERE Ticker = 'OML'" + " and " +
         "Date >= '2016/01/11'")
    # " and Date < '2016/04/10' "
    r = executeQuery(con, s)

    for n, d, c in r:
        print n, d, c

##########################################
def checkD():
    queryStr = 'SELECT * FROM companies WHERE ticker LIKE ?'
    pars = ('%',)

    r = executeQuery2(con, queryStr, pars)
    for x in r:
        print x


##########################################
def checkE():

    ticker = 'NXT'
    startDateStr = '2016/02/01'

    plotTimeAndPriceData(con, ticker, startDateStr)

    print 'x'


##########################################
def checkF():

    ticker = 'NXT'

    queryStr = ('SELECT Date FROM prices '
                'WHERE ticker = ? '
                'AND Date > ? '
                'ORDER BY Date DESC LIMIT 1')
    pars = ('NXT','2016/01/01')
    r = executeQuery2(con, queryStr, pars)
    print len(r)
    for x in r:
        print x


##########################################
def checkG():

    tickers = executeQuery(con, 'SELECT ticker FROM companies')
    tickers = list( itertools.chain(*tickers) )

    start_date_str = '2015/01/01'
    end_date_str = '2015/07/01'


    daysCommon, pricesAll = getPricesForGroup(con, tickers, start_date_str, end_date_str)

    plt.hold(True)

    for p in pricesAll:
        plt.plot(daysCommon, scale(p))

    plt.show()


##########################################

# Uncomment for the required checks.

# checkA()
# checkB()
# checkC()
# checkD()
# checkE()
# checkF()
# checkG()

