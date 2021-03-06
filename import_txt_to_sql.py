# -*- coding: utf-8 -*-
"""
    Import Flat file (usually from SAP extract, but can be from Excel too)
    to postgresql. Adapted from pyrfc_read_table except that it takes
    a flatfile instead of hitting RFC).

    Requires:
    ./config/readconfig.py
    a config file (default:./import_txt_to_pg.ini)
    see ./config/readme.txt for config file documentation

    Author: Peter C. Lai (peter.lai2@sbdinc.com)
"""
import os, sys, io, re, decimal, datetime, ast, codecs
from config import readconfig

logging_config = readconfig.get_logging()

# setup logging if specified
# h/t: http://www.electricmonk.nl/log/2011/08/14/redirect-stdout-and-stderr-to-a-logger-in-python/
# see also: http://stackoverflow.com/a/19438364/2718295
import logging
class LogWriter:
    def __init__(self, logger):
        self.logger = logger
        self.log_level = logging.INFO
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

if logging_config['logging']:
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s: %(message)s',
        filename=(os.path.splitext(os.path.basename(
                sys.argv[0]))[0] + '.log'),
        filemode = 'a'
    )
    loginstance = logging.getLogger('log')
    logwriterinstance = LogWriter(loginstance)
    sys.stdout = logwriterinstance
    sys.stderr = logwriterinstance

# Helper Functions
def conv_to_pydate(abap_date):
    """
    Converts 'YYYYMMDD' or 'YYYY-MM-DD'-style dates
    to python datetime.date
    """
    abap_date = abap_date.replace('-','')
    if len(abap_date) < 8:
        result = None
    elif abap_date[0:8] == '00000000':
        result = None
    else:
        
            #year = int(abap_date[0:4])
            #month = int(abap_date[4:6])
            #day = int(abap_date[6:8])
            #if year == 0 or month == 0 or day == 0:
            #    result = None
            #else:
                #try:
        try:
            result = datetime.date(int(abap_date[0:4]), 
                int(abap_date[4:6]), int(abap_date[6:8]))
        except:
            result = None
    return result

def conv_to_pytime(abap_time):
    """
    Converts 'HHMMSS' or 'HH:MM:SS'-style times
    to python datetime.time
    """
    abap_time = abap_time.replace(':','')
    if len(abap_time) != 6:
        result = None
    else:
        hour = int(abap_time[:2])
        minute = int(abap_time[2:4])
        second = int(abap_time[4:])
        if hour >= 24 or minute >= 60 or second >= 60:
            result = datetime.time(0,0,0)
        else:
            result = datetime.time(hour, minute, second)
    return result

def conv_to_pydec(abap_packed):
    """
    Converts the argument to a python decimal.Decimal type.
    If the argument is a BCD-packed signed decimal (\d+-),
    then parse it correctly.
    """
    #strip comma
    abap_packed = re.sub(',','',abap_packed)
    #check last char of abap_packed
    if abap_packed[-1] == '-':
        #negative bcd
        result = -1 * decimal.Decimal(abap_packed[:-1])
    else:
        result = decimal.Decimal(abap_packed)
    return result

""" Configuration and Validation Code"""

# Get the flatfile ETL configuration information from the config file
# [flatfile] section
flatfile_config = readconfig.get_flatfile()
#print flatfile_config

# Get the Fieldname Datatype configuration from the config file
# [flatfile] section
fields = flatfile_config['fields']

# Get the delimiter from the config file
# [flatfile] section
# if it is a unicode literal, use unicode literal
# format e.g. u'\u2016'
delim = flatfile_config['delimiter']

# Get the qualifier from the config file
# [flatfile] section
# if it is a unicode literal, use unicode literal
# format e.g. u'\x02'
qual = flatfile_config['qualifier']

escape = flatfile_config['escape']

# Get the character encoding scheme from the config file
# [flatfile] section
# this should be the python-specific codec name
# e.g. cp1252, utf16, utf8
# see also https://docs.python.org/2/library/codecs.html#standard-encodings
encoding = flatfile_config['encoding']

# see https://docs.python.org/2/library/codecs.html#codec-base-classes
# according to ../config/readconfig.py, the default value is 'strict'
decoding_error_handler = flatfile_config['decoding_error_handler']

# we can turn off bulk-reads by setting
# pkgsize in [flatfile] to something small, like 1
# setting it is recommended for files with many fields
# the larger this number, the more memory per read this
# program will take
#if 'pkgsize' in flatfile_config:
#    pkgsize = flatfile_config['pkgsize']
#else:
#    pkgsize = 100000

# we can hardcode the name of the source flatfile
# in the config file under [flatfile]
source_file = flatfile_config['source']

# get configuration from [pgquery] section
# this sets a hardcoding for the target table right now.
pgquery_config = readconfig.get_pgquery()

target_table = pgquery_config['target_table']

# gets debug flag from [debug_config] section
# from config file
debug_config = readconfig.debug_config()

# if no target table was specified, then
# the default target table name should be the source
# file basename
if target_table.strip() == '':
    target_table = os.path.basename(source_file).split('.')[0]

""" Begin Database Operations"""

# auto-validate the choice of table name
print "Stripping leading numerics from table name..."
target_table = re.sub(r'^[0-9]+(.*)', r'\1', target_table)
if debug_config['debug']:
 print target_table
print "Replacing non alphanumerics with '_' from table name..."
target_table = re.sub(r'[^A-Za-z0-9]', '_', target_table)
if debug_config['debug']:
    print target_table
print "Truncating table name length to 63 characters if necessary..."
if len(target_table) > 63:
    target_table = target_table[:62]

print 'Resulting table name: %s' % target_table

# Get the postgresql Logon information from the config file
sqlserver = readconfig.get_sqlserver()

print "Connecting to database..."

if sqlserver['servertype'] == 'postgres':
    import psycopg2
    pglogon = {}
    for each in ['host','port','dbname','user','password']:
        pglogon[each] = sqlserver[each]
    sqlconn = psycopg2.connect(**pglogon)
    """
    Lookup table that converts the 1-character datatype
    code in the config file to a SQL (postgres) data type
    and the corresponding python type conversion function.
    int(), decimal.Decimal(), and unicode() are from the
    python builtin and decimal core libraries.
    """
    typeconv = {    'I':['integer',int],
                    'F':['numeric', decimal.Decimal],
                    'P':['numeric', conv_to_pydec],
                    'C':['text', unicode],
                    'D':['date', conv_to_pydate],
                    'T':['time', conv_to_pytime],
                    'N':['text', unicode],
                    'STRING':['text', unicode]}
    # we use different placeholder depending on driver
    ph = r'%s'
    autodroptable = 'DROP TABLE IF EXISTS ' + target_table

elif sqlserver['servertype'] == 'mssql':
    import pyodbc
    if not sqlserver['port']:
        sqlserver['port'] = ''
    sqldsn = ('DRIVER={SQL Server};'
        'SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;PORT=%s') % (
            sqlserver['host'],sqlserver['dbname'],sqlserver['user'],
            sqlserver['password'],sqlserver['port'])
    sqlconn = pyodbc.connect(sqldsn)
    # MSSQL is still using legacy numerics
    typeconv = {    'I':['integer',int],
                    'F':['numeric(38,38)', decimal.Decimal],
                    'P':['numeric(38,38)', conv_to_pydec],
                    'C':['nvarchar(max)', unicode],
                    'D':['date', conv_to_pydate],
                    'T':['time', conv_to_pytime],
                    'N':['nvarchar(max)', unicode],
                    'STRING':['nvarchar(max)', unicode]}
    # we use different placeholder depending on driver
    ph = '?'
    autodroptable = ("IF EXISTS (SELECT 1 "
        "from INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{0}') "
        "DROP TABLE {0}").format(target_table)

else:
    raise RuntimeError("servertype in config file's "
                       "[sqlserver] section must be one of "
                       "'postgres' or 'mssql'")

# autogenerate SQL for table creation and
# row insertion
createsql = 'CREATE TABLE %s ' % target_table
createsql += ('(' + ','.join(field[0] + ' ' +
    typeconv[field[1]][0] for field in fields) + ')')
insertsql = 'INSERT INTO %s ' % target_table
insertsql += ('(' + ','.join(field[0] for field in fields) +
              ') VALUES (' +
              ','.join(ph for field in fields) + ')')

# check whether we want to autodrop and completely overwrite
# existing table with same name or if we want to prompt the user
# or if we want to auto-append to existing table
yes_config = readconfig.yes_config()
append_config = readconfig.append_config()
if debug_config['debug']:
    print ("yes config: ",  yes_config)
    print ("append_config: ", append_config)

if yes_config == 0 and append_config == 0:
    confirm1 = raw_input("Drop if exists and create table %s? "
        "Typing 'N' will APPEND to existing table: " % target_table)
elif append_config == 1:
    confirm1 = 'N'
else:
    confirm1 = 'y'

if debug_config['debug']:
    print ('confirm1: ', confirm1)

sqlcur = sqlconn.cursor()

# if we want to autodrop table or we hit 'y' at the prompt
# drop the existing table
if confirm1.strip() == '' or confirm1.strip().lower() == 'y':
    print autodroptable
    sqlcur.execute(autodroptable)

    if debug_config['debug']:
        print createsql

    sqlcur.execute(createsql)
    sqlconn.commit()
    #print pgcur.statusmessage

if debug_config['debug']:
    print insertsql

sizeof_file = os.stat(source_file).st_size
print "File Size: %d" % sizeof_file
number_of_lines = sum(1 for line in codecs.open(
    filename=source_file, mode='rU',encoding=encoding,
    errors='ignore'))
print "Number of Lines in file: %d" % number_of_lines

# average column width:
#average_line_width = sizeof_file / number_of_lines

# pkgsize autoscaling
# take into account # of columns and # of total rows
memory_limit = 10485760
pkgsize = memory_limit / (sizeof_file / number_of_lines)

#if debug_config['debug']:
#    raise SystemExit('debugging stop')

# open the source file for reading
f = codecs.open(filename=source_file, mode='r', encoding=encoding,
    errors=decoding_error_handler)

# skip lines as specified in skiplines config in the
# [flatfile] section of the config file
rowcounter = 0
exceptioncounter = 0
eof = False
if 'skiplines' in flatfile_config:
    if flatfile_config['skiplines'] > 0:
        rowcounter = int(flatfile_config['skiplines'])
        for i in xrange(int(flatfile_config['skiplines'])):
            try: # to read the next line in the file
                f.next()
            except UnicodeDecodeError: # ignore unicode errors on skiplines
                pass
            except StopIteration: # if EOF
                eof = True
                pass

total = 0 # total rows inserted
while not eof:
    insertdata = [] # this is the master array holding multiple
                    # rows to insert (should be <= pkgsize)
    for dummy0 in xrange(pkgsize):
        row = None
        rowcounter += 1
        insertrow = []
        try: # to read next line in file
            row = f.next()
        except UnicodeDecodeError as e:
            print "{} at row {}".format(e, rowcounter)
            exceptioncounter += 1
            continue
        except StopIteration: # if EOF
            eof = True
            break

        rowstrip = row.rstrip('\r\n')
        fieldvalue = []
        row_ = []
        isqualified = False
        rowstriplen = len(rowstrip)
        # parse the line based on qualifier and delimiter
        # tested if all fields in the line is in the format
        # qualFIELDqualdelimqualFIELDqual
        
        for pos in xrange(rowstriplen):            
            if rowstrip[pos] == qual:
                # found qualifier
                if pos == 0: # i'm on the first char of str
                    isqualified = True
                elif not isqualified and rowstrip[pos-1] == delim:
                    # wasn't qualified and the previous char was delim
                    isqualified = True
                elif isqualified and rowstrip[pos-1] == escape:
                    # was qualified and preivous char was escape char
                    isqualified = True
                elif isqualified:
                    isqualified = False
                else:
                    # there is a qual char in the string but this field was not qual
                    fieldvalue.append(rowstrip[pos])
            elif rowstrip[pos] == delim and isqualified is False:
                row_.append(u''.join(fieldvalue))
                fieldvalue = []
            else:
                fieldvalue.append(rowstrip[pos]) # build the string for this field
        # capture the last group
        row_.append(u''.join(fieldvalue))

        for i in xrange(len(fields)):
            # convert all the fields from unicode to python datatype
            # so that we can insert as the correct data type
            # specified in the fields config
            try:
                if row_[i] == u'':
                    insertrow.append(None)
                else:
                    insertrow.append(typeconv[fields[i][1]][1](row_[i]))
            except IndexError: # short read of the line
                print "Truncated Row at row {} after field {}".format(rowcounter, fields[i-1])
                print "Raw Row: {}".format(repr(rowstrip))
                print "Packed Row: {}".format(repr(row_))
                exceptioncounter += 1
                break
        if len(insertrow) == len(fields): # my line is complete
            # short reads should be trapped by the try block above
            insertdata.append(tuple(insertrow))
    if len(insertdata) > 0: # I have data I need to insert
        sqlcur.executemany(insertsql, insertdata)
        total += len(insertdata)
        print "Rows inserted: {}, {}% of file".format(total, (total*100)/number_of_lines)

# no partial commits
sqlconn.commit()

print "Exceptions: {}".format(exceptioncounter)