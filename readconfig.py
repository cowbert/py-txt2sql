import argparse, ConfigParser
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

def get_pglogon():
    pglogon = {}

    for item in config.items('postgresql'):
        pglogon[item[0]] = item[1]
    if 'target_table' not in pglogon:
        pglogon['target_table'] =''
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

"""
if __name__ == "__main__":
    fullconfig, localconfig, pglogon, saplogon = getconfig()
    print fullconfig
    print localconfig
    print pglogon
    print saplogon
"""