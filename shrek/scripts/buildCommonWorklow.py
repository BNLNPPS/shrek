#!/usr/bin/env python 

import yaml
import argparse
import pprint
import networkx as nx
import pydot

from shrek.yaml.handler import Handler
from shrek.yaml.parameters import ParameterBlock, buildParameterBlock
from shrek.yaml.codeblock import CodeBlock, buildCodeBlock
from shrek.yaml.outputds import OutputDS, buildOutputList 
from shrek.yaml.inputds import InputDS, buildInputList
from shrek.yaml.secondaryds import SecondaryDS, buildSecondaryList
from shrek.yaml.jobdefinition import JobDefinition
from shrek.yaml.workflow import WorkflowGraph

from shrek.scripts.buildJobScript import buildJobDefinition
from shrek.scripts.buildWorkflowGraph import buildWorkflowGraph

def numberOfPredecessors( node, graph ):
    count=0
    for p in graph.predecessors(node):
        count = count + 1
    return count

def numberOfSuccessors( node, graph ):
    count=0
    for p in graph.successors(node):
        count = count + 1
    return count

def buildListOfWorkflowInputJobs( G ):
    # Build list of input jobs
    injobs = []
    for n in G:
        np = numberOfPredecessors(n,G)
        print(str(n)+" "+str(np))
        if np==0:
            name = str(n)
            injobs.append( str(n) )
    return injobs

def buildListOfWorkflowOutputJobs( G ):
    # Build list of input jobs
    outjobs = []
    for n in G:
        np = numberOfSuccessors(n,G)
        if np==0:
            name = str(n)
            outjobs.append( str(n) )
    return outjobs

def cwl_header():
    h = """
cwlVersion: v1.0
class: Workflow
    """
    return h
def cwl_requirements( multipleInput=True):
    r = ""
    if multipleInput:
        r += """
MultipleInputFeatureRequirement: {}
        """
    return r

def cwl_inputs( wfgraph ):
    inputs = ""

    # Find jobs with no predecessors... these may have inputs registered to them.
    G = wfgraph.graph

    injobs = buildListOfWorkflowInputJobs(G)

    for jobname in injobs:
        job = wfgraph.jobsmap[jobname]
        for inp in job.inputs:
            inputs += "\n# %s : input=%s"%(jobname,inp.name)
        if 0==len(job.inputs):
            inputs += "\n# %s : is a generator"%jobname

    inputs += "\ninputs:"
    count = 0 # count total number of input jobs
    for jobname in injobs:
        job = wfgraph.jobsmap[jobname]
        for inp in job.inputs:
            count = count + 1
            inputs += "\n  %s_input: "%jobname
            inputs += "\n    type: string"
            inputs += "\n    default: %s"%inp.name
    inputs += "\n"
    
    if 0==count:
        inputs = "\ninputs: {}"
            
    return inputs

def cwl_outputs( wfgraph ):
    outputs = ""
    G = wfgraph.graph
    outjobs = buildListOfWorkflowOutputJobs(G)
    for jobname in outjobs:
        job = wfgraph.jobsmap[jobname]
        for out in job.outputs:
            outputs += "\n# %s : output=%s"%(jobname,out.name)    

    outputs += "\noutputs:"
    outputs += "\n  outDS:"
    outputs += "\n    type: string"
    count=0
    for jobname in outjobs:
        job = wfgraph.jobsmap[jobname]
        for out in job.outputs:
            count = count + 1
            outputs += "\n    outputSource: %s"%out.name
    outputs += "\n"

    if 0==count:
        outputs = "\noutputs: {}"

    return outputs

def cwl_steps( wfgraph ):
    steps=""
    G = wfgraph.graph

    steps += "\nstep:"
    count = 0
    for node in nx.topological_sort(G):
        name = str(node)
        job = wfgraph.jobsmap[ name ]
        steps += "\n  %s:"% name
        steps += "\n    run: prun"

        # Input blocks
        secondary = []
        types     = []
        inDS = False
        for (i,IN) in enumerate(job.inputs):
            if i==0:
                steps += "\n      opt_inDS: %s"%IN.name
                inDS = True
            else:
                secondary.append(IN.name)
                secondary.append(None) # TODO 
        if len(secondary):
            steps += "\n      opt_inSecondaryDSs: %s"%str(secondary)
            # TODO: types

        # Exec block
        steps += "\n      opt_exec:"
        steps += "\n        default: %s.sh" % ( job.name )
        for (i,IN) in enumerate(job.inputs):
            if i==0: steps += " %IN"
            else   : steps += " %%IN%i"%(i+1)
            
        

        steps += "\n"
        


    
    return steps


    

def buildCommonWorkflow( yamllist, tag_ ):

    wfg = buildWorkflowGraph( yamllist, tag_ )
    wfg.buildEdges()
    wfg.buildDiGraph()

    print(wfg.edges)

    output = ""
    output += cwl_header()
    output += cwl_requirements()
    output += cwl_inputs( wfg )
    output += cwl_outputs( wfg )
    output += cwl_steps( wfg )

    print("...")
    print(output)
    
def main():

    parser = argparse.ArgumentParser(description='Build job script from yaml')
    parser.add_argument('yaml', metavar='YAML', type=str, nargs="+",
                                        help='input filename')
    parser.add_argument('--tag', type=str, help='production tag' )
    args = parser.parse_args()

    #  print(args.yaml)


    buildCommonWorkflow( args.yaml, args.tag )


if __name__ == '__main__':
    main()    



