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

import io, psycopg2, re, sys, decimal, datetime, os, ast, codecs
from config import readconfig

# Helper Functions
def conv_to_pydate(abap_date):
    """
    Converts 'YYYYMMDD' or 'YYYY-MM-DD'-style dates
    to python datetime.date
    """
    abap_date = abap_date.replace('-','')
    if len(abap_date) != 8:
        result = None
    elif abap_date == '00000000':
        result = None
    else:
        year = int(abap_date[:4])
        month = int(abap_date[4:6])
        day = int(abap_date[6:])
        #if year == 0 or month == 0 or day == 0:
        #    result = None
        #else:
            #try:
        result = datetime.date(year, month, day)
            #except:
            #    result = None
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

""" Configuration and Validation Code"""
# Get the postgresql Logon information from the config file
pglogon = readconfig.get_pglogon()

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
if 'pkgsize' in flatfile_config:
    pkgsize = flatfile_config['pkgsize']
else:
    pkgsize = 100000

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
print "Connecting to database..."

pgconn = psycopg2.connect(**pglogon)
pgcur = pgconn.cursor()

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

# autogenerate SQL for table creation and
# row insertion
createsql = 'CREATE TABLE %s ' % target_table
createsql += ('(' + ','.join(field[0] + ' ' +
    typeconv[field[1]][0] for field in fields) + ')')
insertsql = 'INSERT INTO %s ' % target_table
insertsql += ('(' + ','.join(field[0] for field in fields) + ') VALUES (' +
                ','.join(r'%s' for field in fields) + ')')

# check whether we want to autodrop and completely overwrite
# existing table with same name or if we want to prompt the user
# or if we want to auto-append to existing table
yes_config = readconfig.yes_config()
append_config = readconfig.append_config()
if debug_config['debug']:
    print ("yes config: ",  yes_config)
    print ("append_config: ", append_config)

if yes_config == 0 and append_config == 0:
    confirm1 = raw_input("Drop if exists and create table %s? Typing 'N' will APPEND "
                    "to existing table: " % target_table)
elif append_config == 1:
    confirm1 = 'N'
else:
    confirm1 = 'y'

if debug_config['debug']:
    print ('confirm1: ', confirm1)

# if we want to autodrop table or we hit 'y' at the prompt
# drop the existing table
if confirm1.strip() == '' or confirm1.strip().lower() == 'y':
    pgcur.execute('DROP TABLE IF EXISTS ' + target_table)
    if debug_config['debug']:
        print createsql

    pgcur.execute(createsql)
    pgconn.commit()
    print pgcur.statusmessage

if debug_config['debug']:
    print insertsql

# open the source file for reading
f = open(source_file, mode='rU')

# skip lines as specified in skiplines config in the
# [flatfile] section of the config file
rowcounter = 0
exceptioncounter = 0
eof = 0
if 'skiplines' in flatfile_config:
    if flatfile_config['skiplines'] > 0:
        rowcounter = int(flatfile_config['skiplines'])
        for i in xrange(int(flatfile_config['skiplines'])):
            try: # to read the next line in the file
                f.next()
            except StopIteration: # if EOF
                eof = 1
                pass

total = 0 # total rows inserted
while eof == 0:
    insertdata = [] # this is the master array holding multiple
                    # rows to insert (should be <= pkgsize)
    for line in xrange(pkgsize):
        row = None
        rowcounter += 1
        insertrow = []
        try: # to read next line in file
            row = f.next()
        except StopIteration: # if EOF
            eof = 1
            break

        try: #to convert line to a suitable unicode string
            row = codecs.decode(row, encoding, decoding_error_handler)
        except Exception as e: # we were unable to parse the line according
                               # to the encoding specified in config
            print "{} at row {}".format(e, rowcounter)
            #print repr(row)
            exceptioncounter += 1
            continue # go to next line

        rowstrip = row.rstrip('\r\n')
        fieldvalue = []
        row_ = []
        isqualified = False
        # parse the line based on qualifier and delimiter
        # tested if all fields in the line is in the format
        # qualFIELDqualdelimqualFIELDqual
        for pos in xrange(len(rowstrip)):
            if rowstrip[pos] == qual:
                # found qualifier
                isqualified = not isqualified
                # toggle the qualifier start/stop indicator
                continue # goto next char
            if rowstrip[pos] == delim and isqualified is False:
                row_.append(''.join(fieldvalue))
                fieldvalue = []
                continue # goto next char
            fieldvalue.append(rowstrip[pos]) # build the string for this field
        # capture the last group
        row_.append(''.join(fieldvalue))
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
                print "Truncated Row at {} after field {}".format(rowcounter, fields[i-1])
                print "Raw Row: {}".format(repr(rowstrip))
                print "Packed Row: {}".format(repr(row_))
                exceptioncounter += 1
                break
        if len(insertrow) == len(fields): # my line is complete
            insertdata.append(tuple(insertrow))
            #rowcounter += 1
            #print rowcounter
    if len(insertdata) > 0: # I have data I need to insert
        pgcur.executemany(insertsql, insertdata)
        pgconn.commit()
        print pgcur.statusmessage
        total += len(insertdata)
        print "Rows inserted: %d" % total
    #if row is None:
print "Exceptions: {}".format(exceptioncounter)