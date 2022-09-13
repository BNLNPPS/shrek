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

from sh import shrek_submit as shrek

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

def pollRucioForDatasetsMatching( conditions, filematch, delta, utclast_ ):

    count = 0
    utclast = utclast_

    while True:

        utcnow  = str(datetime.datetime.utcnow())
        select  = ( 'updated_at>%s,'%utclast + conditions ) .strip(',')
        result  = queryRucioForDatasetsMatching( select, filematch )
        message = '[Donkey %s polling datasets since %s : %i datasets match : %s]'%(utcnow,utclast,len(result),select)
        utclast = utcnow        
        print(message)
        count = count + 1
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

    parser.add_argument( '--dsflag', type=str, help='Specifies the name of the arguement for the dataset')
    parser.set_defaults( dsflag='INPUT' )

    parser.add_argument( '--conditions', type=str, help='Filter conditions applied to dataset metadata' )
    parser.set_defaults( conditions='' )

    parser.add_argument( "--workflows", type=str, help='SHREK workflows to submit' )

    parser.add_argument( '--startdate', type=str, help='Start date/time for query [defaults to invocation time]' )
    parser.set_defaults( startdate=str(datetime.datetime.utcnow()) )

    args, extraargs = parser.parse_known_args()

    count = 0
    for datasets in  pollRucioForDatasetsMatching( args.conditions, ':'.join([args.scope,args.match]), args.period, args.startdate ):

        # Loop over all datasets and submit them via shrek
        for ds in datasets:

            # Remove the scope from the dsname
            dsname = ds.split(':')[1]
            
            dsname = dsname.split('_')
            dsname.pop()
            dsname = '_'.join(dsname)
                        
            print('shrek --no-submit','--no-check','--tag=sP22a-test66-%i'%count,'%s=%s'%(args.dsflag,dsname), ' '.join(extraargs))
            count = count + 1
        








if __name__ == '__main__':
    main()
