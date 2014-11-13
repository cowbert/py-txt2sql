import argparse, ConfigParser, ast
import os, sys

parser = argparse.ArgumentParser(
    description='ETL Data from Data Source to PostgreSQL target')
parser.add_argument('-c', '--config')
parser.add_argument('-f', '--from', dest='src_data')
parser.add_argument('-t', '--to', dest='target_table')
parser.add_argument('-d', '--debug', action='store_true')
parser.add_argument('src_data_', metavar='SRC_DATA', nargs='?',
    default='')
parser.add_argument('target_table_', metavar='TARGET_TABLE', nargs='?',
    default='')
args = parser.parse_args()
fullconfig = os.path.splitext(sys.argv[0])[0] + '.ini'
localconfig = os.path.splitext(os.path.basename(
    sys.argv[0]))[0] + '.ini'

config = ConfigParser.RawConfigParser(allow_no_value=True)
if args.config:
    config.read([args.config])
else:
    config.read([fullconfig, localconfig])

def get_flatfile():
    flatfile = {}

    # read items in the .ini file
    for item in config.items('flatfile'):
        flatfile[item[0]] = item[1]

    # try to find a suitable delimiter
    try:
        delim = ast.literal_eval(flatfile['delimiter'])
        if not isinstance(delim, basestring):
            raise SystemExit(
                'Specified Delimiter is not a string, '
                'was given {0!r}'.format(delim))
        else:
            delim_len = len(delim)
            if delim_len != 1:
                raise SystemExit(
                    'Delimiter must be 1 character wide '
                    'but is {} wide in config'.format(delim_len))
        flatfile['delimiter'] = delim
    except KeyError:
        raise SystemExit(
            'Delimiter not specified in config file for section [flatfile]')

    #try to find a suitable qualifier, if specified
    try:
        qual = ast.literal_eval(flatfile['qualifier'])
        if not isinstance(qual, basestring):
            raise SystemExit(
                'Specified Qualifier is not a string, '
                'was given {0!r}'.format(qual))
        else:
            qual_len = len(qual)
            if qual_len != 1:
                raise SystemExit(
                    'Qualifier must be 1 character wide '
                    'but is {} wide in config'.format(qual_len))
        flatfile['qualifier'] = qual
    except KeyError:
        flatfile['qualifier'] = ''

    # try to parse a list of field identifiers with data types
    # syntax is FIELDNAME, ABAP_TYPE
    # example: fields= MANDT N, BUKRS C
    try:
        fields = [field.strip() for field in
            flatfile['fields'].split(',')]
        if len(fields) < 1:
            raise SystemExit('No fields defined in config to map')
        fields_ = [field.split() for field in fields]
        for field in fields_:
            if len(field) != 2:
                raise SystemExit(
                    "Field spec {} must be in format: FIELD TYPE".format(
                    field))
        flatfile['fields'] = fields_
    except KeyError:
        raise SystemExit('No fields defined in config to map')

    if 'encoding' not in flatfile:
        flatfile['encoding'] = 'ascii'

    if 'source' not in flatfile:
        if args.src_data:
            flatfile['source'] = args.src_data
        elif args.src_data_:
            flatfile['source'] = args.src_data_
        else:
            raise SystemExit('Source data file path must be '
                'specified in the .ini file, using -f or as an '
                'argument')

    return flatfile

def get_pgquery():
    pgquery = {}

    # mostly exists so that target table can be defined in
    # .ini file
    try:
        for item in config.items('pgquery'):
            pgquery[item[0]] = item[1]
    except ConfigParser.NoSectionError:
        pass

    if 'target_table' not in pgquery:
        pgquery['target_table'] =''
    if args.target_table and args.target_table.strip() != '':
        pgquery['target_table'] = args.target_table
    elif args.target_table_ and args.target_table_.strip() != '':
        pgquery['target_table'] = args.target_table_

    return pgquery

# logon credentials and target db for postgresql
def get_pglogon():
    pglogon = {}

    for item in config.items('pglogon'):
        pglogon[item[0]] = item[1]
    return pglogon

# logon credentials and target instance for SAP (RFC)
def get_saplogon():
    saplogon = {}

    for item in config.items('saplogon'):
        saplogon[item[0]] = item[1]
    return saplogon

    #return (fullconfig, localconfig, pglogon, saplogon)

# configure the Read Table Query for SAP RFC_READ_TABLE
def get_sapreadtable():
    sapquery = {}

    for item in config.items('sap_rfc_read_table'):
        sapquery[item[0]] = item[1]
    if 'where_clause' not in sapquery:
        sapquery['where_clause'] = ''
    if 'keyfields' not in sapquery:
        sapquery['keyfields'] = ''
    if 'extractfields' not in sapquery:
        sapquery['extractfields'] = ''
    return sapquery

def debug_config():
    debug = {}

    # read debug config from .ini
    try:
        for item in config.items('debug'):
            debug[item[0]] = item[1]
    except ConfigParser.NoSectionError:
        debug['debug'] = 0

    # check if invoked with -d cli argument
    if args.debug:
        debug['debug'] = 1

    # default to no debug
    if 'debug' not in debug:
        debug['debug'] = 0

    return debug


# Unit tests
#if __name__ == "__main__":
    #flatfile_config = get_flatfile()
    #print flatfile_config
    #pgquery_config = get_pgquery()
    #print pgquery_config
    #print debug_config()