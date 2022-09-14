#!/usr/bin/env python

import yaml
import argparse
import shutil
import datetime
import sh
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
               
def captureActorOutput( out ):
    print(out)

def captureActorError( out ):
    print(out)
       
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

    parser.add_argument( '--startdate', type=str, help='Start date/time for query [defaults to invocation time]' )
    parser.set_defaults( startdate=str(datetime.datetime.utcnow()) )

    parser.add_argument('--submit',    dest='submit', action='store_true',  help="Submit jobs to PanDA through SHREK")
    parser.add_argument('--no-submit', dest='submit', action='store_false', help="No submission, just print out command")
    parser.set_defaults(submit=False)

    parser.add_argument('--actor',     dest='actors', action='append',  help="Attach an actor, which will be run on each matching dataset, with the ds passed as the first argument and a running count as the second." )
    parser.set_defaults(actors=[])

    parser.add_argument( '--prescale', type=int, help="Launch only 1 in prescale jobs" )
    parser.set_defaults( prescale=1 )

    args, extraargs = parser.parse_known_args()

    count = 0

    # First verify that all actors are correctly specified... building a map for
    # future lookup.  If we throw here... we throw.
    action = {}
    for actor in args.actors:
        print("Verifying ", actor )
        action[ actor ] = sh.Command(actor)
    
    for datasets in  pollRucioForDatasetsMatching( args.conditions, ':'.join([args.scope,args.match]), args.period, args.startdate ):

        # Loop over all datasets and submit them via shrek
        for ds in datasets:

            # Remove the scope from the dsname
            dsname = ds.split(':')[1]
            
            dsname = dsname.split('_')
            dsname.pop()
            dsname = '_'.join(dsname)

            # Apply prescaling
            if ( count % args.prescale > 0 ):
                count = count + 1                
                continue

            uniqueId = count # but may want to make this a uuid...
            
            for actor in args.actors:
                print('Action: ', actor, dsname)
                # TODO: Implement sane exception handling
                action[actor] ( dsname, uniqueId, _out=captureActorOutput, _err=captureActorError )

            count = count + 1                                            

if __name__ == '__main__':
    main()
