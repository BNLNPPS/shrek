#!/usr/bin/env python 

import yaml
import argparse
import pprint
import networkx

from shrek.yaml.handler import Handler
from shrek.yaml.parameters import ParameterBlock, buildParameterBlock
from shrek.yaml.codeblock import CodeBlock, buildCodeBlock
from shrek.yaml.outputds import OutputDS, buildOutputList 
from shrek.yaml.inputds import InputDS, buildInputList
from shrek.yaml.secondaryds import SecondaryDS, buildSecondaryList
from shrek.yaml.jobdefinition import JobDefinition
from shrek.yaml.workflow import WorkflowGraph

from shrek.scripts.buildJobScript import buildJobDefinition

def buildWorkflowGraph( yamllist, tag_ ):

    workflow = WorkflowGraph()

    for yaml_ in yamllist:
        job = buildJobDefinition( yaml_, tag_ )        
        workflow.addJob( job )

    #graph = workflow.buildDiGraph()
    return workflow
    

def main():

    parser = argparse.ArgumentParser(description='Build job script from yaml')
    parser.add_argument('yaml', metavar='YAML', type=str, nargs="+",
                                        help='input filename')
    parser.add_argument('--tag', type=str, help='production tag' )
    args = parser.parse_args()

    buildWorkflowGraph( args.yaml, args.tag )
    
if __name__ == '__main__':
    main()


    
