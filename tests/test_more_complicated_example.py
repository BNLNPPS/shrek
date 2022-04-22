from pykwalify.core import Core
import os
import yaml

from shrek.scripts.buildJobScript import buildJobDefinition
from shrek.scripts.buildWorkflowGraph import buildWorkflowGraph
from shrek.scripts.buildCommonWorklow import buildCommonWorkflow

def commonWorkflow():
    yamls = [
        "tests/more-complicated-chain/combine.yaml",
        "tests/more-complicated-chain/generate_some.yaml",
        "tests/more-complicated-chain/make_background_1.yaml",
        "tests/more-complicated-chain/make_background_2.yaml",
        "tests/more-complicated-chain/make_signal.yaml",
        "tests/more-complicated-chain/pre_mix.yaml"
        ]

    return buildCommonWorkflow( yamls, "sP-TEST" )    

#________________________________________________________________________

def test_create_the_workflow():
    wf = commonWorkflow()
    assert(wf != None)

def test_we_should_be_able_to_parse_the_workflow():
    wf = commonWorkflow()
    print(wf)
    yaml.safe_load( str(wf) )
