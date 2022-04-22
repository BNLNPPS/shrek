from pykwalify.core import Core
import os
import yaml
import pprint

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
    dict_ = yaml.safe_load( str(wf) )
    #pprint.pprint(dict_)

def test_workflow_has_expected_fields():
    wf = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )
    fields = [ "inputs",
               "outputs",
               "requirements",
               "steps",
               "class",
               "cwlVersion"
               ]
    for f in fields:
        x = dict_[f]


def test_workflow_inputs():
    wf = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )
    inputs = dict_["inputs"]
    make_signal = inputs['make_signal_input']
    assert( make_signal['type'] == "string" )
    assert( make_signal['default'] == "signal" )    
    
    make_background_1 = inputs["make_background_1_input"]
    assert(make_background_1['type'] == "string")
    assert(make_background_1['default'] == "background")    

    

def test_workflow_outputs():
    wf = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )
    outputs = dict_["outputs"]
    pprint.pprint(outputs)

    
def test_workflow_class():
    wf = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )
    class_ = dict_["class"]
    assert(class_ == 'Workflow')

    


    
