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
from shrek.yaml.jobdefinition import JobDefinition


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

#_______________________________________________________________________________________

def main():

    parser = argparse.ArgumentParser(description='Build job script from yaml')
    parser.add_argument('yaml', metavar='YAML', type=str, 
                                        help='input filename')

    parser.add_argument('--tag', type=str, help='production tag' )
    args = parser.parse_args()
    
    with open(args.yaml, "r") as stream:

        definition = yaml.safe_load(stream)

        job = JobDefinition( args.yaml, definition )
    
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

        #
        # Convention is that the PanDA will pass certain parameters
        # through the command line
        #
        print( \
"""

# Script command line parameters

export shrek_job_index=$1
if [ "$2" ]; then
   export shrek_input_files=( `echo $2 | tr "," " "` )
   export shrek_number_input_files=${#shrek_input_files[@]}
fi
if [ "$3" ]; then
   export shrek_secondary_files=( `echo $3 | tr "," " "` )
   export shrek_number_secondary_files=${#shrek_input_files[@]}
fi

"""
)

        #
        # Build and output environment block
        #
        print(job.parameters.params)

        #
        # Data set names
        #
        for (i,ds) in enumerate(job.inputs):
            print("# Input DS %s [%i/%i]"%(ds.name,i+1,len(job.inputs)))
            print("export inputDS%i_name=%s"%(i+1,ds.name) )

        print("")    

        for (i,ds) in enumerate(job.secondaries):
            print("# Secondary DS %s [%i/%i]"%(ds.name,i+1,len(job.inputs)))
            print("export secondaryDS%i_name=%s"%(i+1,ds.name) )    

            #
            # Worker node initialization
            #
        if job.init.block:
            print( job.init.block )

        #
        # Worker node execution script
        #
        if job.commands.block:
            print( job.commands.block )

        #
        # Worker node finalization
        #
        if job.finish.block:
            print( job.finish.block )



if __name__ == '__main__':
    main()
