#!/usr/bin/env python 

import yaml
import argparse
import pydot
import os
import stat
import pathlib
import shutil
import glob
import sh
import networkx as nx

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

   
def jobDirectoryName( tag, opts ):
    limit  = opts['maxSubmit']
    prefix = opts['submissionPrefix']
    # Make sure the prefix directory exists
    if ( not os.path.exists( prefix ) ):
        os.mkdir( prefix )

    # Submission directory
    subdir = "%s/%s"%(prefix,tag)

    yield subdir

def buildSubmissionDirectory( tag, jdfs_, site, args, opts ):

    # Make certain we have absolute path to job definition files
    jdfs = []
    for jdf in jdfs_:
        jdfs.append( os.path.abspath( jdf ) )

    # 
    subdir = ""
    for s in jobDirectoryName( tag, opts ):

        if os.path.exists( s ):
            print('[Existing submission directory %s is cleared]'%s)
            shutil.rmtree( s )
                
        subdir = s            
        os.mkdir( subdir )
        print('[PanDA submission directory %s]'%s)            
        break

    # Copy job description files to staging area
    for j in jdfs:
        print('[Copy %s to submission directory %s]'%(j,subdir))                    
        sh.cp( j, subdir )

    # Build job scripts and stage into directory
    input_jobs = []
    job_scripts = []
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

        job_scripts.append( name + ".sh" )

        with open( subdir + '/' + name + '.sh', 'w' ) as f:
            f.write('#!/usr/bin/env bash\n\n')
            if len(job.resources):            
                f.write('# Stage resources into working directory\n')
                f.write('cp -R __%s/* .\n'%name)                        
            f.write(script)

        # Make script executable
        chmod_plus_x(subdir + '/' + name + '.sh') 


        # Create a subdirectory for job resources
        job_resources = []
        if len(job.resources):
                        
            jobdir = subdir + '/__' + name
            os.mkdir( jobdir )

            for r in job.resources:
                if r.type=='file':
                    print("link %s --> %s"%(r.url,jobdir))
                    for f in glob.glob(r.url):
                        head,tail = os.path.split( f )
                        os.symlink( os.path.abspath(f), jobdir + '/' + tail )

                        job_resources.append( os.path.abspath(f) )
        

    # Build CWL for PanDA submission
    ( cwf, yml, dgr ) = buildCommonWorkflow( jdfs, tag, site, args )
    cwlfile = '%s-workflow.cwl'%tag
    ymlfile = '%s-input.yaml'%tag
    # Output common workflow
    with open( subdir + '/' + cwlfile, 'w') as f:
        f.write( cwf )
    # Output inputs into the yaml file
    with open( subdir + '/' + ymlfile, 'w') as f:
        f.write( yml )
        f.write('\n')
        for job in input_jobs:
            for inp in job.inputs:
                f.write('# %s %s %s\n'%(job.name,inp.name,inp.datasets))
                f.write('%s: %s\n'%(inp.name,inp.datasets))


    # Create PNG from the digraph and store in the staging area along
    # with a markdown file for display purposes
    if dgr:
        dot = nx.drawing.nx_pydot.to_pydot(dgr)
        dot.write_png( "%s/workflow.png"%subdir )

    # Add all artefacts to the git repo
    message = '[Shrek submission tag %s]'%tag
    sh.git.add    ( '*', _cwd=subdir )
    try:
        sh.git.commit ( '-m %s'%message,     _cwd=subdir )
    except sh.ErrorReturnCode_1:
        print("WARN: probably trying to submit duplicate code")
    except sh.ErrorReturnCode:
        print("WARN: unknown error during git commit ")


    # Markdown documentation for this job...
    with open("%s/README.md"%subdir,"w") as md:
        md.write( "## SHREK Inputs\n")
        for j in jdfs:
            md.write( "- %s\n"%j )
        md.write("## Generated scripts\n") # ... unclear input job vs normal ...
        for s in job_scripts:
            md.write("- %s\n"%s )
        md.write("## Job resources\n")
        for r in job_resources:
            md.write("- %s\n"%r)
        if 0==len(job_resources):
            md.write("- none\n")

        md.write( "## Job dependencies\n" )
        md.write( "![Workflow graph](workflow.png)\n" )

        md.write( "## PanDA Monitoring\n" )
        taskname = 'user.%s.%s_*'%(args.user,opts['taguuid'])
        md.write( "[panda monitoring](https://panda-doma.cern.ch/tasks/?taskname=%s)\n"%taskname )

    # Print the monitoring link
    print( "Workflow monitoring https://panda-doma.cern.ch/tasks/?taskname=%s\n"%taskname )        
                        
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
                    
    
