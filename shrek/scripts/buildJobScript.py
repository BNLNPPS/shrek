#!/usr/bin/env python 

import yaml
import argparse
import pprint

from shrek.yaml.handler import Handler
from shrek.yaml.parameters import ParameterBlock, buildParameterBlock
from shrek.yaml.codeblock import CodeBlock, buildCodeBlock
from shrek.yaml.outputds import OutputDS, buildOutputList 
from shrek.yaml.inputds import InputDS, buildInputList
from shrek.yaml.secondaryds import SecondaryDS, buildSecondaryList
from shrek.yaml.resources import Resource, buildResourceList
from shrek.yaml.jobdefinition import JobDefinition

from shrek.scripts.simpleLogger import DEBUG, INFO, WARN, ERROR, CRITICAL

def getHandler():
    handler = Handler()
    handler.addToken( 'Parameters', buildParameterBlock, None )
    handler.addToken( 'InputDataSets', buildInputList, [] )
    handler.addToken( 'OutputDataSets', buildOutputList, [] )
    handler.addToken( 'SecondaryDataSets', buildSecondaryList, [] )
    handler.addToken( 'Initialize', buildCodeBlock, None )
    handler.addToken( 'InitLocal', buildCodeBlock, None )
    handler.addToken( 'JobCommands', buildCodeBlock, None )
    handler.addToken( 'Finalize', buildCodeBlock, None )
    handler.addToken( 'LocalFinalize', buildCodeBlock, None )
    handler.addToken( 'Resources', buildResourceList, [] )
    return handler

#_______________________________________________________________________________________
def buildJobDefinition( yaml_, tag_ ):

    with open(yaml_, "r") as stream:

        handler = getHandler()

        definition = yaml.safe_load(stream)
        
        job = JobDefinition( yaml_, definition )
    
        handler.traverse( definition )

        job.parameters  = handler.result('Parameters')
        job.inputs      = handler.result('InputDataSets')
        job.outputs     = handler.result('OutputDataSets')
        job.secondaries = handler.result('SecondaryDataSets')
        job.localinit   = handler.result('LocalInit')
        job.init        = handler.result('Initialize')
        job.commands    = handler.result('JobCommands')
        job.finish      = handler.result('Finalize')
        job.localfinish = handler.result('LocalFinalize')
        job.resources   = handler.result('Resources')

        if job.parameters:
            job.name       = job.parameters.name
        else:
            job.name       = "input"    
        job.numInputs      = len( job.inputs )
        job.numSecondaries = len( job.secondaries )
        job.numOutputs     = len( job.outputs )

        return job

def buildJobScript( yaml_, tag_, opts_, glvars_ ):

    job = buildJobDefinition( yaml_, tag_ )

    output = ""

    output += "echo $@\n"
    output += "\n"        
    output += "echo Executing on `hostname`\n"
    output += "uname -a\n"
    output += "lscpu | grep \^CPU\n"    
    output += "free -h --giga\n"
    output += "\n"

    # Export global variables
    # TODO: require pattern match of VAR=VAL
    for (gl,val) in glvars_.items():
        gl = gl.strip('--')
        gl = gl.strip('-')
        output += 'export %s=%s\n'%(gl,val)

    # Export username, tag, etc...
    output += 'export shrek_username=%s\n'%opts_['user']
    output += 'export shrek_tag=%s\n'%opts_['taguuid']
    output += 'export shrek_basename=%s\n'%opts_['basename']
    output += 'export rucio_dsname=user.%s.%s\n'%(opts_['user'],opts_['taguuid']) # NOTE: Does not respect group scope!

    if job == None:
        print("Job not valid... probably no yaml file?")
        return None
        
    else:
        #
        # Convention is that the PanDA will pass certain parameters
        # through the command line
        #

        arg = 1        

        # First parameter is a uniqueId
        output += 'export uniqueId=$%i\n'%arg
        arg = arg + 1

        # Successive arguements will accept inputs
        for (i,ds) in enumerate(job.inputs):
            output += "export IN%i_name=%s\n"%(i+1,ds.name) 
            output += "export IN%i_task=%s\n"%(i+1,ds.name.replace("/outDS","") )           
            output += 'export IN%i=(`echo $%i | tr "," " "`)\n'%(i+1,arg)
            arg = arg + 1

        #
        # Set the tag
        #
        output += 'export shrek_tag=%s\n'%(tag_)


        #
        # Source the plugins
        #
        output += """
# Import any functions and/or environment variables in plugins directory        
for f in plugins/*.sh; do
  echo "Importing environment from $f"
  source "$f"
done
        """
        


        #
        # Build and output environment block
        #
        if job.parameters:
            if job.parameters.params:
                output += job.parameters.params + '\n\n'

        #
        # Inject metadata handling into the script
        #
        output += """
# Function to add metadata information
# if the variable metalogger is defined, append to specified log file 
function addmetadata {
   local datetime=`date`
   # For PanDA
   echo \\\"${1}\\\" : \\\"${2}\\\", >> userJobMetadata.json
   # Logging facility
   if [[ -z ${metalogger} ]]; then
      echo "[" ${datetime} "]" ${shrek_tag} ${name} ${uniqueId}: \\\"${1}\\\" : \\\"${2}\\\"
   else
      echo "[" ${datetime} "]" ${shrek_tag} ${name} ${uniqueId}: \\\"${1}\\\" : \\\"${2}\\\"        
      echo "[" ${datetime} "]" ${shrek_tag} ${name} ${uniqueId}: \\\"${1}\\\" : \\\"${2}\\\"        >> ${metalogger}
   fi
}
# Initialize metadata file
echo '{' > userJobMetadata.json        
echo \\\"shrek_begin_metafile\\\" : 1,  >> userJobMetadata.json

addmetadata shrek_tag ${shrek_tag}
addmetadata shrek_uniqueId ${uniqueId}
addmetadata shrek_start_time \"`date`\"
        """


        #
        # Worker node initialization
        #        
        if job.init:
            output += "addmetadata shrek_init_start \"`date`\"\n"            
            if job.init.block:
                output += job.init.block + '\n'
            output += "addmetadata shrek_init_end \"`date`\"\n"                            

        #
        # Worker node execution script
        #
        if job.commands:
            output += "addmetadata shrek_command_exec_start \"`date`\"\n"            
            if job.commands.block:
                output += job.commands.block + '\n'
            output += "addmetadata shrek_command_exec_end \"`date`\"\n"                            

        #
        # Worker node finalization
        #
        if job.finish:
            output += "addmetadata shrek_finish_start \"`date`\"\n"                        
            if job.finish.block:
                output += job.finish.block + '\n'
            output += "addmetadata shrek_finish_end \"`date`\"\n"                                        

    output += """

    addmetadata shrek_end_time \"`date`\"
    
 echo \\\"shrek_end_metafile\\\" : 1  >> userJobMetadata.json
 echo '}' >> userJobMetadata.json
    """

    return ( job, output )

     

def main():

    parser = argparse.ArgumentParser(description='Build job script from yaml')
    parser.add_argument('yaml', metavar='YAML', type=str, 
                                        help='input filename')
    parser.add_argument('--tag', type=str, help='production tag' )
    args = parser.parse_args()

    print( buildJobScript( args.yaml, args.tag ) )

if __name__ == '__main__':
    main()
