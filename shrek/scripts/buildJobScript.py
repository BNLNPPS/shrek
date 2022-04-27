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

        job.name           = job.parameters.name
        job.numInputs      = len( job.inputs )
        job.numSecondaries = len( job.secondaries )
        job.numOutputs     = len( job.outputs )

        return job

def buildJobScript( yaml_, tag_ ):

    job = buildJobDefinition( yaml_, tag_ )

    output = ""

    if job == None:
        print("Job not valid... probably no yaml file?")
        return None
        
    else:
        #
        # Convention is that the PanDA will pass certain parameters
        # through the command line
        #
        output +="""

# Script command line parameters

#export shrek_job_index=$1
#if [ "$2" ]; then
#   export shrek_input_files=( `echo $2 | tr "," " "` )
#   export shrek_number_input_files=${#shrek_input_files[@]}
#fi
#if [ "$3" ]; then
#   export shrek_secondary_files=( `echo $3 | tr "," " "` )
#   export shrek_number_secondary_files=${#shrek_input_files[@]}
#fi

"""


        #
        # Build and output environment block
        #
        if job.parameters:
            if job.parameters.params:
                output += job.parameters.params + '\n\n'

        #
        # Data set names
        #
        for (i,ds) in enumerate(job.inputs):
            output += "# Input DS %s [%i/%i]\n"%(ds.name,i+1,len(job.inputs))
            output += "export inputDS%i_name=%s\n"%(i+1,ds.name) 

        for (i,ds) in enumerate(job.secondaries):
            output += "# Secondary DS %s [%i/%i]\n"%(ds.name,i+1,len(job.inputs))
            output += "export secondaryDS%i_name=%s\n"%(i+1,ds.name) 

        #
        # Worker node initialization
        #
        if job.init:
            if job.init.block:
                output += job.init.block + '\n'

        #
        # Worker node execution script
        #
        if job.commands:
            if job.commands.block:
                output += job.commands.block + '\n'

        #
        # Worker node finalization
        #
        if job.finish:
            if job.finish.block:
                output += job.finish.block + '\n'

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
