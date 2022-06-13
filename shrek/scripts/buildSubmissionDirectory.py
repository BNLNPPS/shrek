#!/usr/bin/env python 

import yaml
import argparse
import pydot
import os
import stat
import pathlib
import shutil
import glob

from shrek.scripts.buildJobScript import buildJobScript
from shrek.scripts.buildCommonWorklow import buildCommonWorkflow

def get_umask():
    umask = os.umask(0)
    os.umask(umask)
    return umask

def chmod_plus_x(path):
    os.chmod(
        path,
        os.stat(path).st_mode |
        (
            (
                stat.S_IXUSR |
                stat.S_IXGRP |
                stat.S_IXOTH
            )
        & ~get_umask()
        )
        )

   
def jobDirectoryName( tag, limit=20 ):
    for i in range(0,limit):
        yield ".shrek-submit-%s-%i"%(tag,i)
    assert( 0 == "Past maximum number of submission directories for single production... clean up please.")


def buildSubmissionDirectory( tag, jdfs_, site, args, opts ):

    # Make certain we have absolute path to job definition files
    jdfs = []
    for jdf in jdfs_:
        jdfs.append( os.path.abspath( jdf ) )

    # 
    subdir = ""
    for s in jobDirectoryName( tag, limit=opts['maxSubmit'] ):
        if os.path.exists( s ):
            print('[Skip existing submission directory %s]'%s)
        else:
            subdir = s            
            os.mkdir( subdir )
            print('[PanDA submission directory created %s]'%s)            
            break

    # Build job scripts and stage into directory
    input_jobs = []
    for jdf in jdfs:
        stem = pathlib.Path(jdf).stem        
        (job, script) = buildJobScript( jdf, tag )

        # A job w/ no name will be treated as pure input
        if job.parameters:
            name = job.parameters.name
        else:
            input_jobs.append( job )
            continue

        assert(script)
        assert(job)

        with open( subdir + '/' + name + '.sh', 'w' ) as f:
            f.write('#!/usr/bin/env bash\n\n')
            if len(job.resources):            
                f.write('# Stage resources into working directory\n')
                f.write('cp -R __%s/* .\n'%name)                        
            f.write(script)

        # Make script executable
        chmod_plus_x(subdir + '/' + name + '.sh') 


        # Create a subdirectory for job resources
        if len(job.resources):
                        
            jobdir = subdir + '/__' + name
            os.mkdir( jobdir )

            for r in job.resources:
                if r.type=='file':
                    print("link %s --> %s"%(r.url,jobdir))
                    for f in glob.glob(r.url):
                        #shutil.copy( f, jobdir )
                        head,tail = os.path.split( f )
                        os.symlink( os.path.abspath(f), jobdir + '/' + tail )
        

    # Build CWL for PanDA submission
    ( cwf, yml ) = buildCommonWorkflow( jdfs, tag, site, args )
    cwlfile = '%s-workflow.cwl'%tag
    ymlfile = '%s-input.yaml'%tag    
    with open( subdir + '/' + cwlfile, 'w') as f:
        f.write( cwf )
    with open( subdir + '/' + ymlfile, 'w') as f:
        f.write( yml )
        f.write('\n')
        for job in input_jobs:
            for inp in job.inputs:
                f.write('# %s %s %s\n'%(job.name,inp.name,inp.datasets))
                f.write('%s: %s\n'%(inp.name,inp.datasets))
                
    return (subdir,cwlfile,ymlfile)
        
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
                    
    
