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

        if job.parameters:
            job.name       = job.parameters.name
        else:
            job.name       = "input"    
        job.numInputs      = len( job.inputs )
        job.numSecondaries = len( job.secondaries )
        job.numOutputs     = len( job.outputs )

        return job

def buildJobScript( yaml_, tag_, glvars_ ):

    job = buildJobDefinition( yaml_, tag_ )

    output  = "echo $@\n"
    output += "\n"        
    output += "echo Executing on `hostname`\n"
    output += "uname -a\n"
    output += "lscpu | grep \^CPU\n"    
    output += "free -h --giga\n"
    output += "\n"

    # Export global variables
    # TODO: require pattern match of VAR=VAL
    for gl in glvars_:
        gl = gl.strip('--')
        gl = gl.strip('-')
        output += 'export %s\n'%gl
        
    

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
            output += 'export IN%i=(`echo $%i | tr "," " "`)\n'%(i+1,arg)
            arg = arg + 1

        #
        # Set the tag
        #
        output += 'export shrek_tag=%s\n'%(tag_)

        #
        # Build and output environment block
        #
        if job.parameters:
            if job.parameters.params:
                output += job.parameters.params + '\n\n'


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
