#!/usr/bin/env python

import argparse
import datetime
import sh
import time
import uuid
import os
import cmd
import pprint

from rucio.client       import Client
from rucio.common.utils import adler32

from rucio.common.exception import DataIdentifierNotFound

client = Client() # Get the rucio client

from shrek.scripts.simpleLogger import DEBUG, INFO, WARN, ERROR, CRITICAL

def parse_args( defaults=None ):

    epilog="""
    Example usage:
    --------------

    Create a dataset to add files to
    $ farquaad add-dataset --dataset TEST-RUN-2304010001

    Add files to the dataset created above.  The data identifiers in rucio will be
    the name of the added physical files.
    $ farquaad add-file --pfn /path/to/physical/files/* --dataset TEST-RUN-2304010001

    Add a single file and specify the data identifier for rucio
    $ farquaad add-file --pfn /path/to/a/single/file --did something-different

    Close the dataset.  From this point you can no longer add files
    $ farquaad close-dataset --dataset TEST-RUN-2304010001    
    """
    
    parser = argparse.ArgumentParser(description='Registers data files and datasets with rucio',epilog=epilog,formatter_class=argparse.RawTextHelpFormatter)

    # NOTE: This should be done as sub-parsers.  TODO.
    subcommands=['add-file','add-dataset','close-dataset' ]
    parser.add_argument('cmd', metavar="COMMAND", type=str, choices=subcommands, help="Subcommand to execute [%s]"%','.join(subcommands))

    parser.add_argument('--pfn',     type=str, default=[], nargs="+",help="Physical location of file(s) to register to rucio.  DID will be the tail of the path unless specified with --did.")
    parser.add_argument('--did',     type=str,default=None,help="Specifies the DID for the file if provided.  Only makes sense for a single file.")
    parser.add_argument('--scope',   type=str,default='group.sphenix',help="Scope into which we place the dataset / files")
    parser.add_argument('--rse',     type=str,default='MOCK',help="Specifies the rucio storage element")
    parser.add_argument('--dataset', type=str,help="Name of the dataset to register the file to")
    parser.add_argument('--verbose', type=int,default=0,help="Sets verbosity flag")
    parser.add_argument('--simulate',action='store_true',help="Simulates the action (and raises verbosity)",default=False)

    return parser.parse_known_args()

def get_file_path( path_ ):
    """
    Expands environment variables and "~" in the path.  Returns the
    absolute path of the file.  Throws OSError on a problem.
    """
    path = os.path.expandvars ( path_ )
    path = os.path.expanduser ( path  )
    path = os.path.normpath   ( path  )
    if os.path.islink( path ):
        WARN("%s is a link / replacing with real path and filename"%path)
    path = os.path.realpath   ( path  ) # get rid of symbolic links
    # ... but make sure that we aren't going to /direct/sphenix+lustre01 ...
    path = path.replace('direct/','')
    path = path.replace('+','/')
    return path

def add_dataset_to_rucio( args ):
    did = None
    if args.simulate==False:

        # Verify that the dataset exists and is open
        try:
            did = client.get_did( args.scope, args.dataset )
            WARN("%s:%s already exists"%(args.scope,args.dataset))

        # Create the dataset if we were not able to find it
        except DataIdentifierNotFound:             
            WARN("Creating dataset %s:%s"%(args.scope,args.dataset))
            client.add_dataset( args.scope_, args.dataset, rse=args.rse )
            did = client.get_did( args.scope, args.dataset )

    else:

        did = client.get_did( args.scope, args.dataset )

    if args.verbose>0:
        pprint.pprint(did)

    return did



def register_single_file( path_, args ):

    path = get_file_path( path_ )

    if os.path.exists( path ):

        head, tail = os.path.split( path )        
    
        scope_   = args.scope
        pfn_     = 'file://localhost' + path
        name_    = tail
        if args.did:
            name_ = args.did

        bytes_   = os.path.getsize( path )
        adler32_ = adler32( path )

        replica = {
            'scope'  : scope_,    # user scope
            'name'   : name_,     # filename / data identifier
            'pfn'    : pfn_,      # physical filename (full path to file)
            'bytes'  : bytes_,    # size of file in bytes
            'adler32': adler32_,  # adler32 checksum
        }

        if args.verbose>0:
            INFO("Add %s @%s to %s with %s"%(path,args.rse,args.dataset,scope_))
            pprint.pprint(replica)

       
        if args.simulate==False:

            rdataset = add_dataset_to_rucio( args )
            if rdataset['open'] != True:
                CRITICAL('%s is not open.  Cannot assoicate to this dataset'%args.dataset)
                return            

            # And add the file to the dataset
            client.add_files_to_dataset( scope_, args.dataset, [replica,], args.rse )
           
                
    else:

        WARN("%s was not found / skip")

    
def close_dataset( args ):
    did = None
    if args.simulate==False:

        # Verify that the dataset exists and is open
        try:
            did = client.get_did( args.scope, args.dataset )
            if did['open']==True:
                client.close( args.scope,args.dataset )
            else:
                # Warn if it was already closed
                WARN('%s:%s is already closed'%(args.scope,args.dataset) )
                
        # Warn if it doesn't exist
        except DataIdentifierNotFound:             
            WARN('%s:%s does not exist'%(args.scope,args.dataset) )

    else:

        did = client.get_did( args.scope, args.dataset )

    if args.verbose>0:
        pprint.pprint(did)

    return did


def main():
    args, globalvars = parse_args()
    if args.simulate:
        args.verbose = 1000

    if args.cmd == 'add-file':

        if args.did and len(args.pfn)>1:
            CRITICAL("Cannot specify --did when registering more than one file")
            return

        for pfn in args.pfn:

            if os.path.isdir( pfn ):
                WARN("%s is a directory, skip") # in future recurse?
                continue
            
            register_single_file( pfn, args )

    elif args.cmd == 'add-dataset':

        add_dataset_to_rucio( args )

    elif args.cmd == 'close-dataset':

        close_dataset( args ) 

    

if __name__ == '__main__':
    main()
