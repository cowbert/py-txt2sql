"""
    Import Flat file (usually from SAP extract, but can be from Excel too)
    to postgresql. Adapted from pyrfc_read_table except that it takes
    a flatfile instead of hitting RFC).

    Requires: ./config/readconfig.py and ./import_txt_to_pg.ini
"""

import io, psycopg2, re, sys, decimal, datetime, os, ast
from config import readconfig

def conv_to_pydate(abap_date):
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
    #strip comma
    abap_packed = re.sub(',','',abap_packed)
    #check last char of abap_packed
    if abap_packed[-1] == '-':
        #negative bcd
        result = -1 * decimal.Decimal(abap_packed[:-1])
    else:
        result = decimal.Decimal(abap_packed)
    return result

typeconv = {    'I':['integer',int],
                'F':['numeric', decimal.Decimal],
                'P':['numeric', conv_to_pydec],
                'C':['text', unicode],
                'D':['date', conv_to_pydate],
                'T':['time', conv_to_pytime],
                'N':['text', unicode],
                'STRING':['text', unicode]}

pglogon = readconfig.get_pglogon()

flatfile_config = readconfig.get_flatfile()
#print flatfile_config

fields = flatfile_config['fields']

delim = flatfile_config['delimiter']

qual = flatfile_config['qualifier']

encoding = flatfile_config['encoding']

if 'pkgsize' in flatfile_config:
    pkgsize = flatfile_config['pkgsize']
else:
    pkgsize = 100000

source_file = flatfile_config['source']

pgquery_config = readconfig.get_pgquery()

target_table = pgquery_config['target_table']

debug_config = readconfig.debug_config()

if target_table.strip() == '':
    target_table = os.path.basename(source_file).split('.')[0]

print "Connecting to database..."

pgconn = psycopg2.connect(**pglogon)
pgcur = pgconn.cursor()

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

createsql = 'CREATE TABLE %s ' % target_table
createsql += ('(' + ','.join(field[0] + ' ' +
    typeconv[field[1]][0] for field in fields) + ')')
insertsql = 'INSERT INTO %s ' % target_table
insertsql += ('(' + ','.join(field[0] for field in fields) + ') VALUES (' +
                ','.join(r'%s' for field in fields) + ')')

confirm1 = raw_input("Drop if exists and create table %s? Typing 'N' will APPEND "
                    "to existing table: " % target_table)

if confirm1.strip() == '' or confirm1.strip().lower() == 'y':
    pgcur.execute('DROP TABLE IF EXISTS ' + target_table)
    if debug_config['debug']:
        print createsql

    pgcur.execute(createsql)
    pgconn.commit()
    print pgcur.statusmessage

if debug_config['debug']:
    print insertsql

f = io.open(source_file, mode='r', encoding=encoding)

#skip lines
if 'skiplines' in flatfile_config:
    if flatfile_config['skiplines'] > 0:
        for i in xrange(int(flatfile_config['skiplines'])):
            next(f, None)

total = 0
while True:
    insertdata = []
    for line in xrange(pkgsize):
        insertrow = []
        row = next(f, None)
        if row is None:
            break
        else:
            #row_ = row.rstrip('\r\n').split(delim)
            rowstrip = row.rstrip('\r\n')
            fieldvalue = []
            row_ = []
            isqualified = False
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

                fieldvalue.append(rowstrip[pos]) # build the string
            # capture the last group
            row_.append(''.join(fieldvalue))

        for i in xrange(len(fields)):
            if row_[i] == '':
                insertrow.append(None)
            else:
                insertrow.append(typeconv[fields[i][1]][1](row_[i]))
        if len(insertrow) > 0:
            insertdata.append(tuple(insertrow))
    if len(insertdata) > 0:
        pgcur.executemany(insertsql, insertdata)
        pgconn.commit()
        print pgcur.statusmessage
        total += len(insertdata)
        print "Rows inserted: %d" % total
    if row is None:
        break
