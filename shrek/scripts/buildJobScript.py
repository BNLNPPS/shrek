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

parser = argparse.ArgumentParser(description='Build job script from yaml')
parser.add_argument('yaml', metavar='YAML', type=str, 
                                        help='input filename')

parser.add_argument('--tag', type=str, help='production tag' )

args = parser.parse_args()
    
with open(args.yaml, "r") as stream:

    definition = yaml.safe_load(stream)   
    handler.traverse( definition )

    myparameters  = handler.result('Parameters')
    myinputs      = handler.result('InputDataSets')
    myoutlist     = handler.result('OutputDataSets')
    mysecondaries = handler.result('SecondaryDataSets')
    mylocalinit   = handler.result('LocalInit')
    myinit        = handler.result('Initialize')
    myscript      = handler.result('JobCommands')
    myfinal       = handler.result('Finalize')
    mylocalfinal  = handler.result('LocalFinalize')


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
    print(myparameters.params)

    #
    # Data set names
    #
    for (i,ds) in enumerate(myinputs):
        print("# Input DS %s [%i/%i]"%(ds.name,i+1,len(myinputs)))
        print("export inputDS%i_name=%s"%(i+1,ds.name) )

    print("")    

    for (i,ds) in enumerate(mysecondaries):
        print("# Secondary DS %s [%i/%i]"%(ds.name,i+1,len(myinputs)))
        print("export secondaryDS%i_name=%s"%(i+1,ds.name) )    

    #
    # Worker node initialization
    #
    if myinit.block:
        print( myinit.block )

    #
    # Worker node execution script
    #
    if myscript.block:
        print( myscript.block )

    #
    # Worker node finalization
    #
    if myfinal.block:
        print( myfinal.block )
