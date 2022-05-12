#!/usr/bin/env python 

import yaml
import argparse
import pprint
import networkx as nx
import pydot

from pykwalify.core import Core
import glob

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

from math import ceil, log

def ceil_power_of_10(n):
    exp = log(n, 10)
    exp = ceil(exp)
    return 10**exp
            

def validate( yaml_ ):
    # schema definitions
    schema = glob.glob("schema/*.yaml")
    # Ensure job definition files match schema
    c = Core( source_file=yaml_, schema_files=schema)
    c.validate(raise_exception=True)

def validateJobDefinitions( jdfs ):
    for jdf in jdfs:
        validate(jdf)

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
    r = "\nrequirements:"
    count = 0
    if multipleInput:        
        r += "\n  MultipleInputFeatureRequirement: {}"
        count += 1
    if 0==count:
        r=""
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
            inputs += "\n  %s: string"%inp.name
#           inputs += "\n  %s_input: "%jobname
#           inputs += "\n    type: string"
#           inputs += "\n    default: %s"%inp.name
    inputs += "\n"
    
    if 0==count:
        inputs = "\ninputs: []"
            
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


def cwl_opt_args( job ):

    optargs = ""

    # From job parameters...
    params = job.parameters
    hasMaxAttempt = False
    for par in [ "nJobs", "nFilesPerJob", "nGBPerJob", "maxAttempt" ]:
        val = getattr(params,par,None)        
        if val:
            if par=='maxAttempt': hasMaxAttempt = True
            optargs += ' --%s %s '%( par, str(val))

    # Override annoying PanDA default...
    if hasMaxAttempt == False:
        optargs += ' --maxAttempt 1 '

    # From output filelist
    outputs = []
    for output in job.outputs:
        for f in output.filelist:
            (opt,out) = f.split(':')
            out = out.strip()
            outputs.append(out)

    if len(outputs)>0:
        optargs += ' --outputs ' + ','.join(outputs) + ' '
    else:
        print('SHREK[warning]: '+ job.name+' ill-defined no output files specified')
        print('SHREK[warning]: this will likely cause problems, but continuing ...')


    # From input (secondary) data sets
    inputs = []
    count = 0
    for inp in job.inputs:
        DSn = '%{DS' + str(count) + '}'
        count += 1
        INn = '%IN' + str(count)
        if count==1: continue # Skip the principle dataset 
        name = inp.name
        nfpj = inp.nFilesPerJob
        if nfpj == None:
            nfpj = 1
        inputs.append( INn + ':' + str(nfpj) + ':' + DSn )

    if len(inputs)>0:
        optargs = ' --secondaryDSs ' + ','.join(inputs)
            
    return optargs

def cwl_steps( wfgraph, site ):
    steps=""
    G = wfgraph.graph

    steps += "\nsteps:"
    count = 0
    for node in nx.topological_sort(G):
        name = str(node)
        job = wfgraph.jobsmap[ name ]
        steps += "\n  %s:"% name
        steps += "\n    run: prun"
        steps += "\n    in:"
        # Input blocks
        secondary = []
        types     = []
        ntypes    = 0
        inDS = False
        for (i,IN) in enumerate(job.inputs):
            if i==0:
                steps += "\n        opt_inDS: %s"%IN.name
                if IN.match:
                    steps += "\n        opt_inDsType:"
                    steps += "\n          default: %s"%IN.match
                inDS = True
            else:
                secondary.append(IN.name)
                if IN.match:
                    types.append(IN.match)
                    ntypes = ntypes + 1
                else:
                    types.append("*")
        if len(secondary):
            steps += "\n        opt_secondaryDSs: [%s]"% ','.join(secondary)
        if ntypes>0:
            steps += "\n        opt_secondaryDsTypes:"
            steps += "\n          default: [%s]"% ','.join( types )

        # Exec block
        steps += "\n        opt_exec:"
        steps += '\n          default: "%s.sh ' % ( job.name )
        njobs = getattr(job.parameters,'nJobs',None)

        # Jobs uniquely defined starting with job ID=1
        steps += " %RNDM:1" 

        for (i,IN) in enumerate(job.inputs):
            if i==0: steps += " %IN"
            else   : steps += " %%IN%i"%(i+1)
        steps += '"'

        optargs = cwl_opt_args(job)
        if len(optargs.strip()) > 0:
            steps += "\n        opt_args:"
            steps += '\n          default: "%s --site %s --avoidVP --noBuild "' %(optargs,site)

        
        steps += "\n    out: [outDS]" # by convention...
        steps += "\n"
        


    
    return steps


    

def buildCommonWorkflow( yamllist, tag_, site ):

    wfg = buildWorkflowGraph( yamllist, tag_ )
    wfg.buildEdges()
    wfg.buildDiGraph()


    output = ""
    output += cwl_header()
    output += cwl_requirements()
    output += cwl_inputs( wfg )
    output += cwl_outputs( wfg )
    output += cwl_steps( wfg, site )

    yaml = "# dummy yaml file"

    return ( output, yaml )

    
def main():

    parser = argparse.ArgumentParser(description='Build job script from yaml')
    parser.add_argument('yaml', metavar='YAML', type=str, nargs="+",
                                        help='input filename')
    parser.add_argument('--tag',  type=str, help='production tag' )
    parser.add_argument('--dump', action='store_true')
    parser.set_defaults(dump=False)
    
    args = parser.parse_args()

    validateJobDefinitions( args.yaml )

    wf = buildCommonWorkflow( args.yaml, args.tag )

    if args.dump:
        print(wf)


if __name__ == '__main__':
    main()    



