#!/usr/bin/env python

import yaml
import argparse
import pydot
import os
import pathlib
import shutil
import glob
import getpass
import uuid
import datetime
import subprocess # TODO refactor subprocess --> sh
import sh
import sys
import time

def readConfig( filename ):

    defaults = {}
    with open(filename,'r') as stream:
        try:
            defaults = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    return defaults['Donkey']

def queryRucioForDatasetsMatching( conditions, filematch ):
    print('query %s %s'%(conditions,filematch) )
    if conditions:
        result = sh.rucio.ls( '--short', '--filter', conditions, filematch )
    else:
        result = sh.rucio.ls( '--short', filematch )
    result=result.strip('\n')
    return result.split()

def pollRucioForDatasetsMatching( conditions, filematch, delta ):

    # Initialize to the current time
    utclast  = str(datetime.datetime.utcnow())
    
    while True:

        utcnow  = str(datetime.datetime.utcnow())
        select  = ( 'updated_at>%s,'%utclast + conditions ) .strip(',')
        result  = queryRucioForDatasetsMatching( select, filematch )
        message = '[Donkey %s polling datasets since %s : %i datasets match]'%(utcnow,utclast,len(result))
        print(message)
        utclast = utcnow
        
        yield result

        time.sleep( delta )
        
        




    
    

def main():

    defaults = readConfig( 'shrek/config/site.yaml' )

    parser = argparse.ArgumentParser(description='Monitors rucio for new datasets and submits to PanDA')

    parser.add_argument( '--period', type=int, help='Amount of time between polling / querying / spammin rucio for new datasets', required=False )
    parser.set_defaults( period=defaults['period'] )

    parser.add_argument( '--scope', type=str, help='Rucio scope for the query', required=False )
    parser.set_defaults( scope=defaults['scope'] )

    parser.add_argument( '--match', type=str, help='Datasets match this string (only "*" allowed as wildcards)' )
    parser.set_defaults( match=defaults['match'] )

    parser.add_argument( '--conditions', type=str, help='Filter conditions applied to dataset metadata' )
    parser.set_defaults( conditions='' )

    args, globalvars = parser.parse_known_args()

    testDS1 = pollRucioForDatasetsMatching( args.conditions, ':'.join([args.scope,'*.test1']), 0 )
    testDS2 = pollRucioForDatasetsMatching( args.conditions, ':'.join([args.scope,'*.test2']), 0 )

    for ds1,ds2 in zip ( testDS1, testDS2 ):

        print(ds1)
        print(ds2)

        time.sleep( args.period )
    






if __name__ == '__main__':
    main()
