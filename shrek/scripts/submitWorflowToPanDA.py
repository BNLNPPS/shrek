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

from shrek.scripts.buildJobScript import buildJobScript
from shrek.scripts.buildCommonWorklow import buildCommonWorkflow
from shrek.scripts.buildSubmissionDirectory import buildSubmissionDirectory

def main():

    # Default configuration options
    defaults = {}
    shrekOpts = {}
    pandaOpts = {}    
    with open("shrek/config/site.yaml", "r") as stream:
        try:
            defaults = yaml.safe_load(stream)
            shrekOpts = defaults['Shrek']
            pandaOpts = defaults['PanDA']
            print(pandaOpts)
        except yaml.YAMLError as exc:
            print(exc)
                                   
    #
    parser = argparse.ArgumentParser(description='Build job submission area')
    parser.add_argument('yaml', metavar='YAML', type=str, nargs="+",help='input filename')

    # 
    parser.add_argument('--tag',  type=str, help='production tag' )
    parser.add_argument('--offset', type=int, dest='offset', help='job unique id offset')
    parser.set_defaults(offset=0)

    # 
    parser.add_argument('--submit',    dest='submit', action='store_true')
    parser.add_argument('--no-submit', dest='submit', action='store_false')
    parser.set_defaults(submit=False)
    parser.add_argument('--check',    dest='check', action='store_true')
    parser.add_argument('--no-check', dest='check', action='store_false')
    parser.set_defaults(check=True)    
    parser.add_argument('--handshake',    dest='handshake', action='store_true')
    parser.add_argument('--no-handshake', dest='handshake', action='store_false')
    parser.set_defaults(handshake=True)    

    #
    parser.add_argument('--vo', type=str,              default=pandaOpts['vo'])
    parser.add_argument('--site',type=str,             default=pandaOpts['site'])
    parser.add_argument('--prodSourceLabel', type=str, default=pandaOpts['prodSourceLabel'])
    parser.add_argument('--workingGroup', type=str,    default=pandaOpts['workingGroup'])
    parser.add_argument('--user', type=str,            default=getpass.getuser())

    args = parser.parse_args()

    if args.handshake == True:
        from pandaclient import panda_api
        client = panda_api.get_api()
        client.hello()

    taguuid = args.tag + '-' + str(uuid.uuid1())
    shrekOpts['taguuid'] = taguuid
        

    (subdir,cwlfile,yamlfile) = buildSubmissionDirectory( args.tag, args.yaml, args.site, args, shrekOpts )

    pchain = [ "pchain" ]

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
        pcheck_result = subprocess.run( ' '.join(pcheck), shell=True, cwd=os.path.abspath(subdir), capture_output=True, check=False )

    # We are now ready to submit the job.  First log everything to a tag file...
    # (can be updated to a local DB)

    # Create a "tag file" which will ride along with the job 
    with open( subdir + '/' + taguuid, 'w' ) as f:
        f.write('SHREK Job Submission %s'%str(datetime.datetime.now()))
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
        pchain_result = subprocess.run( ' '.join(pchain), shell=True, cwd=os.path.abspath(subdir), capture_output=True, check=False )
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

        # Make sure all artefacts were committed and push (will require git auth)
        sh.git.add    ( '*',                              _cwd=subdir )
        try:
            sh.git.commit ( '-m "%s"'%message,                  _cwd=subdir )
        except sh.ErrorReturnCode_1:
            print("WARN: git commit duplicate code?")
        except sh.ErrorReturnCode:
            print("WARN: git commit duplicate code?")

        #sh.git.tag    ( '-a','-m "%s"'%message, '%s'%taguuid, _cwd=subdir )
        sh.git.push   (                                       _cwd=subdir )
        #sh.git.push   ( 'origin', '%s'%taguuid,               _cwd=subdir )

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
            sh.git.push   (                                       _cwd=shrekOpts['submissionPrefix'])
        except:
            print ("Warning: README.md not updated" )

    else:
        print('To submit by hand:')
        print('  cd %s'%subdir )
        print('  %s'% ' '.join(pchain) )

        with open( '%s/submit'%subdir, 'w' ) as doit:
            doit.write( '#!/usr/bin/env bash\n')
            doit.write( '%s\n'% ' '.join(pchain) )

if __name__ == '__main__':
    main()
    
