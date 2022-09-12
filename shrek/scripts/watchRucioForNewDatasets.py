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
    if conditions:
        result = sh.rucio.ls( '--short', '--filter', conditions, filematch )
    else:
        result = sh.rucio.ls( '--short', filematch )
    result=result.strip('\n')
    return result.split()

def pollRucioForDatasetsMatchin( conditions, filematch ):
    
    

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
    parser.set_defaults( conditions=None )

    args, globalvars = parser.parse_known_args()

    go = True

    # Last polling time
    utclast  = str(datetime.datetime.utcnow())
       
    while go:

        # Current time
        utcnow  = str(datetime.datetime.utcnow())
        conditions = 'created_at>%s'%utclast

        # Supplemental conditions supplied on command line
        if args.conditions:
            conditions = conditions + ',' + args.conditions
               
        result = queryRucioForDatasetsMatching( conditions, '%s:%s'%(args.scope, args.match) )
        message = '[Donkey %s polling datasets since %s : %i datasets match]'%(utcnow,utclast,len(result))
        print(message)

        utclast = utcnow
        time.sleep( args.period )


if __name__ == '__main__':
    main()
