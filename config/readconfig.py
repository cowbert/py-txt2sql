import argparse, ConfigParser, ast
import os, sys

# http://stackoverflow.com/questions/3853722/python-argparse-how-to-insert-newline-in-the-help-text
# mutate argparse.HelpFormatter._split_lines
class SmartFormatter(argparse.HelpFormatter):
    def _split_lines(self, text, width):
        return text.splitlines()

parser = argparse.ArgumentParser(
    description='ETL Data from Data Source to PostgreSQL target',
    formatter_class=SmartFormatter)
parser.add_argument('-a','--append', action='store_true')
parser.add_argument('-c', '--config')
parser.add_argument('-f', '--from', dest='src_data')
parser.add_argument('-t', '--to', dest='target_table')
parser.add_argument('-d', '--debug', action='store_true')
parser.add_argument('-y', '--yes', action='store_true')
parser.add_argument('--delim', dest='delim')
parser.add_argument('--qual', dest='qual')
parser.add_argument('--encoding',dest='encoding')
parser.add_argument('--override',action='store_true')
parser.add_argument('--decoding-error-handler', default='strict',
    choices = ['strict','ignore','replace'],
    dest='decoding_error_handler',
    help=(
        'This overrides the default behavior of the Character Encoding '
        'decoder so that invalid characters found in the input string ('
        'that does not have a valid mapping for the code page specified '
        'by --encoding does not throw an exception. The accceptable values '
        'are:\n'
        'strict - default, on decoding error, will throw exception and bail\n'
        'ignore - ignore character\n'
        'replace - replace with U+FFFD (<?>)'))
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
    errors = 0
    flatfile = {}

    # read items in the .ini file
    for item in config.items('flatfile'):
        flatfile[item[0]] = item[1]

    # try to find a suitable delimiter
    try:
        delim0 = flatfile['delimiter']
    except KeyError:
        delim0 = args.delim
        if delim0 is None and not args.override:
            raise SystemExit('No Delimited specified in the config file under the [flatfile] section nor with --delim')
    if delim0 is None:
        delim = ''
    else:
        if delim0[0] == 'u':
            delim = ast.literal_eval(delim0)
        elif delim0[0] == '\\':
            delim = ast.literal_eval("u'"+delim0+"'")
        else:
            delim = delim0
        if args.debug:
            print "Delim Raw: {}".format(repr(delim0))
            print "Delim postproc: {}".format(repr(delim))
        if not isinstance(delim, basestring):
            raise SystemExit(
                'Specified Delimiter is not a string, '
                'was given {0!r}'.format(delim))
        else:
            if len(delim) != 1 and not args.override:
                print repr(delim)
                raise SystemExit(
                    'Delimiter must be 1 character wide '
                    'but is {} wide in config'.format(len(delim)))
    flatfile['delimiter'] = delim

    #try to find a suitable qualifier, if specified
    try:
        qual0 = flatfile['qualifier']
    except KeyError:
        qual0 = args.qual
    if qual0 is None:
        qual = ''
    else:
        if qual0[0] == 'u':
            qual = ast.literal_eval(qual0)
        elif qual0[0] == '\\':
            qual = ast.literal_eval("u'"+qual0+"'")
        else:
            qual = qual0
        if not isinstance(qual, basestring):
            raise SystemExit(
                'Specified Qualifier is not a string, '
                'was given {0!r}'.format(qual))
        if len(qual) > 1:
            raise SystemExit(
                'Qualifier must be 1 character wide '
                'but is {} wide in config'.format(len(qual)))
        if args.debug:
            print "Qual 0: {}".format(repr(qual0))
            print "Qual postproc: {}".format(repr(qual))
    flatfile['qualifier'] = qual

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
        if args.encoding is None:
            flatfile['encoding'] = 'ascii'
        else:
            flatfile['encoding'] = args.encoding

    flatfile['decoding_error_handler'] = args.decoding_error_handler

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

def yes_config():
    yes_flag = 0
    if args.yes:
        yes_flag = 1
    return yes_flag

def append_config():
    append_flag = 0
    if args.append:
        append_flag = 1
    return append_flag

def validator_config():
    errors = 0
    validator = {}

    # read items in the .ini file
    for item in config.items('validator'):
        validator[item[0]] = item[1]
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

# Unit tests
#if __name__ == "__main__":
    #flatfile_config = get_flatfile()
    #print flatfile_config
    #pgquery_config = get_pgquery()
    #print pgquery_config
    #print debug_config()