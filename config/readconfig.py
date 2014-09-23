import argparse, ConfigParser, ast
import os, sys

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config')
args = parser.parse_args()
fullconfig = os.path.splitext(sys.argv[0])[0] + '.ini'
localconfig = os.path.splitext(os.path.basename(sys.argv[0]))[0] + '.ini'

config = ConfigParser.RawConfigParser(allow_no_value=True)
if args.config:
    config.read([args.config])
else:
    config.read([fullconfig, localconfig])

def get_flatfile():
    flatfile = {}
    
    for item in config.items('flatfile'):
        flatfile[item[0]] = item[1]
    try:
        delim = ast.literal_eval(flatfile['delimiter'])
        if not isinstance(delim, basestring):
            raise SystemExit(
                'Specified Delimiter is not a string, '
                'was given {}'.format(repr(delim)))
        else:
            delim_len = len(delim)
            if delim_len > 1:
                raise SystemExit(
                    'Delimiter must be 1 character wide '
                    'but is {} wide in config'.format(delim_len))
        flatfile['delimiter'] = delim
    except KeyError:
        raise SystemExit(
            'Delimiter not specified in config file for section [flatfile]')

    return flatfile

def get_pgquery():
    pgquery = {}
    
    for item in config.items('pgquery'):
        pgquery[item[0]] = item[1]
    if 'target_table' not in pgquery:
        pgquery['target_table'] =''
    return pgquery

def get_pglogon():
    pglogon = {}

    for item in config.items('pglogon'):
        pglogon[item[0]] = item[1]
    return pglogon

def get_saplogon():        
    saplogon = {}

    for item in config.items('saplogon'):
        saplogon[item[0]] = item[1]
    return saplogon
    
    #return (fullconfig, localconfig, pglogon, saplogon)
    
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


# Unit tests
if __name__ == "__main__":
    flatfile_config = get_flatfile()
    print flatfile_config