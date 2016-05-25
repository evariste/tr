"""
First script to run. It creates tables in the database for the companies and for the prices.
The companies table relies on a file in the data directory, this is currently set to
ftse-companies-2016-05.csv but can be changed below. The prices table will be empty
and can be populated by the get_prices script.

"""

import sqlite3
import numpy as np
import os.path, shutil

#########################################################################

# To persuade sqlite to interpret numpy data types correctly.
# http://stackoverflow.com/questions/11910584/sqlite3-writes-only-floating-point-numpy-arrays-not-integer-ones
sqlite3.register_adapter(np.int64, int)

# The name of the file containing the sqlite database.
dataDir = '/Users/paulaljabar/Documents/tr/data'
dbFile = 'trade_data'
dbFileFull = dataDir + '/' + dbFile + '.db'

# Back up just in case.
if os.path.isfile(dbFileFull):
    shutil.copy(dbFileFull, dataDir + '/' + dbFile + '-bak.db')

# Our database connection, later functions will rely on this.
con = sqlite3.connect(dbFileFull)

#########################################################################

def createTable(tableName, fieldNames, pKey, clobber=False):
    """
    Create a table in the database.
    :param tableName:   Er. Name of table.
    :param fieldNames:  Fields for the table.
    :param pKey:        Primary key(s)
    :param clobber:     Set to true if we want to delete any earlier
                        version of the table and create a new one.
    """
    cur = con.cursor()

    cmd = ("CREATE TABLE " + 
        tableName + 
        "(" + ', '.join(fieldNames) + ", "
        "CONSTRAINT " + 
        tableName + "_pk " + 
        "PRIMARY KEY " + pKey + ")")
    
    print "init-db: ", cmd
    
    checkCmd = ("SELECT name FROM sqlite_master " + 
        "WHERE type='table' AND name='" + tableName + "' ;")
    
    cur.execute(checkCmd)
    
    nRows = len(cur.fetchall())
    
    if (nRows == 0) or ((nRows > 0) and clobber):
        cur.execute("DROP TABLE IF EXISTS " + tableName)
        cur.execute(cmd)
        con.commit()
    else:
        print ('Table "{:s}" already exists '.format(tableName) + 
            'and not clobbering.')

#########################################################################

def populateTableFromCSV(csvFile, tableName, fieldNames, fieldTypes, 
                         clobber=False, verbose=False, headerRows=1):
    """

    :param csvFile:    File with the data.
    :param tableName:  Table to insert the data into.
    :param fieldNames: The names of the fields in the table.
    :param fieldTypes: The types of the columns in the csv file. These columns and
                       their types must correspond with the names of the fields in
                       the table.
    :param clobber:    Set to true if we want to delete any earlier
                       version of the table and create a new one. Default False.
    :param verbose:    Set to True to be more chatty (default = False).
    :param headerRows: The number of header rows in the csv file (derault = 1).
    """


    # Get the data from the csv file
    dataToEnter = list(np.genfromtxt(csvFile,  delimiter=',', 
                                     dtype=fieldTypes, 
                                     skip_header=headerRows))


    # Check if the table already exists.
    checkCmd = ("SELECT name FROM sqlite_master " + 
    "WHERE type='table' AND name='" + tableName + "' ;")
    cur = con.cursor()
    cur.execute(checkCmd)

    nRows = len(cur.fetchall())
    
    if (nRows == 0) or ((nRows > 0) and clobber):
        # Either table does not exist or it does and we are clobbering it.

        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS " + tableName)
    
        cmd = "CREATE TABLE " + tableName + "(" + ', '.join(fieldNames) + ")"
    
        if verbose:
            print "setupDB.populateTableFromCSV: ", cmd
        cur.execute(cmd)
    
        cmd = ("INSERT INTO " + tableName + 
               " VALUES(" + 
               ', '.join(['?'] * len(fieldNames)) + 
               ") ")
    
        if verbose:
            print "setupDB.populateTableFromCSV: ", cmd
        cur.executemany(cmd, dataToEnter)
        con.commit()

    else:
        
        print "Table '{:s}' exists and not clobbering.".format(tableName)


#########################################################################

tableName  = u'prices'

fieldNames = ['Ticker TEXT',
              'Date INTEGER',
              'Open FLOAT', 
              'High FLOAT', 
              'Low FLOAT', 
              'Close FLOAT',
              'Volume INTEGER']

primaryKeys = "(Ticker, Date)"

createTable(tableName, fieldNames, primaryKeys, clobber=True) #  clobber=True

#########################################################################

tableName  = 'companies'

csvFile    = dataDir + '/ftse-companies-2016-05.csv'

fieldNames = ['companyName TEXT', 
              'ticker TEXT PRIMARY KEY',
              'sector TEXT',
              'marketCap FLOAT',
              'employees INT']
fieldTypes = 'S10,S60,S30,float,int'

populateTableFromCSV(csvFile, tableName, fieldNames, fieldTypes,  clobber=True)
