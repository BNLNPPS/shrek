#!/usr/bin/env python 

import yaml
import argparse
import pydot
import os
import pathlib

from shrek.scripts.buildJobScript import buildJobScript
from shrek.scripts.buildCommonWorklow import buildCommonWorkflow

def jobDirectoryName( tag ):
    for i in range(0,20):
        yield "job-submission-%s.%i"%(tag,i)
    assert( 0 == "Past maximum number of submission directories for single production... clean up please.")


def buildSubmissionDirectory( tag, jdfs_ ):

    # Make certain we have absolute path to job definition files
    jdfs = []
    for jdf in jdfs_:
        jdfs.append( os.path.abspath( jdf ) )

    # 
    subdir = ""
    for s in jobDirectoryName( tag ):
        if os.path.exists( s ):
            print('[Skip existing submission directory %s]'%s)
        else:
            subdir = s            
            os.mkdir( subdir )
            break

    # Build job scripts
    for jdf in jdfs:
        stem = pathlib.Path(jdf).stem        
        script = buildJobScript( jdf, tag )
        print( jdf )
        assert(script)
        with open( subdir + '/' + stem + '.sh', 'w' ) as f:
            f.write(script)

    # Build CWL for PanDA submission
    cwf = buildCommonWorkflow( jdfs, tag )
    with open( subdir + '/%s-workflow.cwl'%tag, 'w') as f:
        f.write( cwf )
    with open( subdir + '/%s-input.yaml'%tag, 'w') as f:
        f.write( '# dummy' )
        
def main():

    parser = argparse.ArgumentParser(description='Build job submission area')
    parser.add_argument('yaml', metavar='YAML', type=str, nargs="+",
                                        help='input filename')
    parser.add_argument('--tag',  type=str, help='production tag' )
    parser.set_defaults(submit=False)

    args = parser.parse_args()

    buildSubmissionDirectory( args.tag, args.yaml )

if __name__ == '__main__':
    main()    
                    
    
