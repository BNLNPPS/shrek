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
import pprint
import inspect

from shrek.scripts.buildJobScript import buildJobScript
from shrek.scripts.buildCommonWorklow import buildCommonWorkflow
from shrek.scripts.buildSubmissionDirectory import buildSubmissionDirectory
from shrek.scripts.ShrekConfiguration import readSiteConfig

# Default configuration options
defaults  = readSiteConfig()
shrekOpts = defaults['Shrek']
pandaOpts = defaults['PanDA']

def progressbar(it, prefix="", size=60, out=sys.stdout): # Python3.3+
    count = len(it)
    def show(j):
        x = int(size*j/count)
        print("{}[{}{}] {}/{}".format(prefix, "#"*x, "."*(size-x), j, count),
              end='\r', file=out, flush=True)
    show(0)
    for i, item in enumerate(it):
        yield item
        show(i+1)
    print("\n", flush=True, file=out)
                                                                

def buildPrunCommand( submissionDirectory, jobDefinitions, args, taguuid  ):  
    assert( len(jobDefinitions)==1 )
    from shrek.scripts.buildCommonWorklow import PANDA_OPTS
    
    job = jobDefinitions[0]

    numInputs      = job.numInputs
    numOutputs     = job.numOutputs
    numSecondaries = job.numSecondaries

    #print('numInputs = '+str(numInputs))
    #print('numOutputs = '+str(numOutputs))
    #print('numSecondaries = '+str(numSecondaries))
    #print( job.inputs )
    #print( job.outputs )
    #print( job.secondaries )
    #print( job.filename )
    #print( job.definition )
    #print( job.name )

    #for outds in job.outputs:
    #    print( outds.filelist )
    # output = '--outputs ' + ','.join(outds.filelist)
    # pchain.append(output)

    pchain = []
    if pandaOpts.get("virtualenv") != None:
        pchain . append( pandaOpts.get("virtualenv") + ' && ' )
    
    pchain . append ( "prun" )
    pchain . append( "-v" )
    pchain . append( "--noBuild" )

    pchain . append( '--site %s'%args.site )
    pchain . append( '--vo %s'%args.vo )
    pchain . append( '--workingGroup %s'%args.workingGroup )
    pchain . append( '--prodSourceLabel %s'%args.prodSourceLabel )

    # Output data set
    pchain . append('--outDS user.%s.%s'%( args.user, taguuid ) )

    # Output data files
    output = '--outputs '
    for outds in job.outputs:
        output += ','.join(outds.filelist)
    output = output.replace('required:','')
    pchain.append(output)        
    

    # Input data files
    
    # Build the shell exec command
    shargs = "%s.sh %%RNDM:%i"%(job.name,args.offset)

    for (i,IN) in enumerate(job.inputs):
        if i==0: shargs += " %IN"
        else:    shargs += " %%IN%i"%(i+1)

    shargs += ' >& _%s.log' % ( job.name ) 
    pchain . append("--exec '%s'"% (shargs) )

    params = job.parameters
    hasMaxAttempt = False
    for par in PANDA_OPTS:
        val = getattr(params,par,None)
        if val:
            if par=='merge':
                pchain.append( '--mergeOutput' )
                pchain.append( '--mergeScript=\\\"%s\\\"' % val )
            else:            
                pchain.append( ' --%s %s '%( par, str(val)) )
                if par=='maxAttempt':
                    hasMaxAttempt = True

    if hasMaxAttempt == False:
        pchain . append( '--maxAttempt 1' )
    
    return pchain

        
def main():

    # Setup the default panda environment to be run in subprocess that launches pchain
    pandaEnv = os.environ.copy()
    pandaEnv['PANDA_URL']         = pandaOpts['url']
    pandaEnv['PANDA_URL_SSL']     = pandaOpts['url_ssl']
    pandaEnv['PANDA_AUTH']        = pandaOpts['auth']
    pandaEnv['PANDA_VERIFY_HOST'] = pandaOpts['verify_host']
    pandaEnv['PANDA_AUTH_VO']     = pandaOpts['auth_vo']

    if pandaOpts.get('cache_url'):        
        pandaEnv['PANDA_CACHE_URL'] = pandaOpts.get('cache_url')
    if pandaOpts.get('mon_url'):
        pandaEnv['PANDAMON_URL'] = pandaOpts.get('mon_url')
    if pandaOpts.get('use_native_httplib'):
        pandaEnv['PANDA_USE_NATIVE_HTTPLIB'] = pandaOpts.get('use_native_httplib')
    if pandaOpts.get('behind_real_lb') != None:
        pandaEnv['PANDA_BEHIND_REAL_LB'] = pandaOpts.get('behind_real_lb')
    if pandaOpts.get("config_root") != None:
        pandaEnv['PANDA_CONFIG_ROOT'] = pandaOpts.get("config_root")
        
    if pandaOpts.get("virtualenv") != None:
        pandaEnv['SHREK_VIRTUAL_ENV_SCRIPT'] = pandaOpts.get("virtualenv")


        

    pprint.pprint(pandaOpts)
                                             
    #
    parser = argparse.ArgumentParser(description='Build job submission area')
    parser.add_argument('yaml', metavar='YAML', type=str, nargs="+",help='input filename')

    # 
    parser.add_argument('--tag',  type=str, help='production tag', required=True )
    parser.add_argument('--offset', type=int, dest='offset', help='job unique id offset')
    parser.set_defaults(offset=0)

    # 
    parser.add_argument('--submit',    dest='submit', action='store_true', help="Job will be submitted to PanDA and archived to github")
    parser.add_argument('--no-submit', dest='submit', action='store_false', help="Job is generated but not submitted or archived")
    parser.set_defaults(submit=False)
    parser.add_argument('--check',    dest='check', action='store_true', help="Job is checked against PanDA")
    parser.add_argument('--no-check', dest='check', action='store_false', help="Job is not checked against PanDA")
    parser.set_defaults(check=True)    
    parser.add_argument('--handshake',    dest='handshake', action='store_true', help="Ensure that PanDA credentials are current")
    parser.add_argument('--no-handshake', dest='handshake', action='store_false', help="Skip credential check")
    parser.set_defaults(handshake=True)

    parser.add_argument('--uuid',    dest='uuid', action='store_true',  help="Tag will be appended by UUID")
    parser.add_argument('--no-uuid', dest='uuid', action='store_false', help="Tag will not be appended by UUID")
    parser.add_argument('--timestamp',    dest='timestamp', action='store_true',  help="Tag will be appended by timestamp")
    parser.add_argument('--no-timestamp', dest='timestamp', action='store_false', help="Tag will not be appended by timestamp")
    parser.set_defaults(uuid=False)
    parser.set_defaults(timestamp=True)

    parser.add_argument('--archive',    dest='archive', action='store_true',  help="Submission directory pushed to git / archived")
    parser.add_argument('--no-archive', dest='archive', action='store_false', help="Submission directory not pushed to git / archived")
    parser.set_defaults(archive=True)

    parser.add_argument('--push',    dest='push', action='store_true',  help="Submission directory pushed to git / archived")
    parser.add_argument('--no-push', dest='push', action='store_false', help="Submission directory pushed to git / archived")
    parser.set_defaults(push=False)

    parser.add_argument('--diagram',    dest='diagram', action='store_true',  help="Add workflow diagram")
    parser.add_argument('--no-diagram', dest='diagram', action='store_false', help="Do not create workflow diagram")
    parser.set_defaults(diagram=False)

    parser.add_argument('--no-pause', dest='pause', action='store_false', help='Do not pause before submitting job' )

    #
    parser.add_argument('--vo', type=str,              default=pandaOpts['vo'])
    parser.add_argument('--site',type=str,             default=pandaOpts['site'])
    parser.add_argument('--prodSourceLabel', type=str, default=pandaOpts['prodSourceLabel'])
    parser.add_argument('--workingGroup', type=str,    default=pandaOpts['workingGroup'])
    parser.add_argument('--user', type=str,            default=getpass.getuser())
    parser.add_argument('--branch', type=str,          default=shrekOpts['defaultBranch'])

    # Unrecognized flags
    args, globalvars = parser.parse_known_args()

    # Set user in shrekOpts
    shrekOpts['user'] = args.user 

    glvars = {}
    for gl in globalvars:
        gl = gl.strip('--')
        gl = gl.strip('-')
        (k,v) = gl.split('=')
        glvars[k] = v

    fullcommandline = str( ' '.join(sys.argv) )
   
    if args.handshake == True:
        hello = sh.Command("shrek/scripts/pokeThePanda.py")
        hello(_fg=True, _env=pandaEnv )


    taguuid = args.tag
    if args.uuid:
        taguuid = taguuid + '-' + str(uuid.uuid1())
    elif args.timestamp:
        stamp = datetime.datetime.utcnow().isoformat('T','minutes')
        stamp = stamp.replace(':','')
        stamp = stamp.replace('-','')
        stamp = stamp.replace('T','-')
        taguuid = taguuid + '-' + stamp
     

    shrekOpts['taguuid'] = taguuid

    (subdir,cwlfile,yamlfile,jobs) = buildSubmissionDirectory( args.tag, args.yaml, args.site, args, shrekOpts, glvars )


    # Build the prun command
    pruncmd = None
    if len(jobs)==1:
        print("This looks like a prun job to me")
        pruncmd = buildPrunCommand( subdir, jobs, args, taguuid )
        # Do not execute workflow check
        args.check = False

    # Build the pchain command
    pchain = []

    if len(jobs)!=1:
        if pandaOpts.get("virtualenv") != None:
            pchain . append( pandaOpts.get("virtualenv") + ' && ' )
    
        pchain . append ( "pchain" )

        pchain . append( '--vo %s'%args.vo )
        pchain . append( '--workingGroup %s'%args.workingGroup )
        pchain . append( '--prodSourceLabel %s'%args.prodSourceLabel )

        pchain . append('--outDS user.%s.%s'%( args.user, taguuid ) )    
        pchain . append('--cwl %s'%cwlfile )
        pchain . append('--yaml %s'%yamlfile )

        pcheck = []
        for p in pchain: pcheck.append(p)
        pcheck .append ( '--check' )


        # Run pchain with --check option to validate against PanDA prior to submission
        #   output is captured
        #   exit code is tested and exception raised if nonzero
        pcheck_result = ""
        if args.check:
            pcheck_result = subprocess.run( ' '.join(pcheck), shell=True, cwd=os.path.abspath(subdir), env=pandaEnv, capture_output=True, check=False )


    # Use prun if we have a single job
    if (len(jobs)==1 ):
        pchain = pruncmd

    # We are now ready to submit the job.  First log everything to a tag file...
    # (can be updated to a local DB)

    # Create a "tag file" which will ride along with the job 
    with open( subdir + '/' + taguuid, 'w' ) as f:
        f.write('SHREK Job Submission %s'%str(datetime.datetime.now()))
        f.write('\n' + fullcommandline )
        f.write('\ncmd args: ')
        f.write('\n')        
        f.write(str(args))
        f.write('\nsubdir: ')
        f.write('\n')        
        f.write(os.path.abspath(subdir))
        f.write('\ntag: ')
        f.write('\n')        
        f.write(taguuid)
        f.write('\ncheck:')
        f.write('\n')
        if args.check:
            f.write(str(pcheck_result.stdout))
            f.write('\n')
            f.write(str(pcheck_result.stderr))
        else:
            f.write('\n')            
            f.write('no PanDA validation\n')
            f.write('\n')            

    if args.check:
        if pcheck_result.returncode != 0:
            print("PanDA did not validate the workflow.  Submission canceled.")
            return

    #
    # Submit job to PanDA
    #
    pchain_result = None
    utcnow = ""
    
    if args.submit:

        # Pause before submission
        if args.pause:
            print("Pausing for 15s before submission\n")
            for i in progressbar( range(60), "Pausing:", 60 ):
                time.sleep(0.25)

        print("Submitting workflow\n")
        
        pchain_result = subprocess.run( ' '.join(pchain), shell=True, cwd=os.path.abspath(subdir), env=pandaEnv, capture_output=True, check=False )
        utcnow = str(datetime.datetime.utcnow())
        with open( subdir + '/' + taguuid, 'a' ) as f:
            f.write('\nsubmit:')
            f.write('\n'+utcnow)
            f.write('\n')
            f.write(str(pchain_result.stdout))
            f.write('\n')
            f.write(str(pchain_result.stderr))
        print('[Job submitted at '+utcnow+' UTC]')

        message='[Shrek submission %s %s UTC]'%(taguuid,utcnow)
        print(message)

        if args.archive:
            # Make sure all artefacts were committed and push (will require git auth)
            sh.git.add      ( '*',                                  _cwd=subdir )
            try:
                sh.git.commit ( '-m "%s"'%message,                  _cwd=subdir )
            except sh.ErrorReturnCode_1:
                print("WARN: git commit duplicate code?")
            except sh.ErrorReturnCode:
                print("WARN: git commit duplicate code?")

            if args.push:
                sh.git.push   (                                       _cwd=subdir )
            
            # Hash for current commit
            githash = sh.git('rev-parse', '--short', 'HEAD',         _cwd=subdir ) .rstrip()

            # n.b. this line is too hardcoded for production / release... contains my github account...
            # githashurl = 'https://github.com/klendathu2k/sPHENIX-test-production/commit/%s'%githash
            githashurl = 'https://github.com/klendathu2k/sPHENIX-test-production/tree/%s/%s'%(githash,args.tag)

            # Open readme file to place a oneliner table entry
            print( shrekOpts['submissionPrefix'] + 'README.md' )
        
            with open( shrekOpts['submissionPrefix'] + 'README.md', 'a' ) as readme:
                update = '|%s|%s|[%s](%s)|%s|\n'%(args.tag,utcnow,githash,githashurl,args.user)
                readme.write( update )

            sh.git.add    ( 'README.md',                          _cwd=shrekOpts['submissionPrefix'])
            try:
                sh.git.commit ( '-m "%s"'%message,                    _cwd=shrekOpts['submissionPrefix'])
                if args.push:
                    sh.git.push   (                                       _cwd=shrekOpts['submissionPrefix'])
            except:
                print ("Warning: README.md not updated" )

    else:
        
        print('To submit by hand:')
        print('  $ cd %s'%subdir )
        print('  $ %s'% ' '.join(pchain) )
        print('    - or -' )
        print('  $ ./submit' )

    # Create the manual submission script regardless of whether we submitted the job or not    
    with open( '%s/submit'%subdir, 'w' ) as doit:
        doit.write( '#!/usr/bin/env bash\n')
        for k,v in pandaEnv.items():
            if k[:5]=='PANDA':
                doit.write('%s=%s\n'%(k,v))
        doit.write( '%s\n'% ' '.join(pchain) )

if __name__ == '__main__':
    main()
    
