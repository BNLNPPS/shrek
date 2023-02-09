from pykwalify.core import Core
import os
import yaml
import pprint
import pytest

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

    class Args:
        offset = 0
        site = "BNL_OSG_SPHENIX"
        tag  = "sP-MORE-COMPLICATED-CHAIN-TEST"
        scouting = True
    args = Args()        

    return buildCommonWorkflow( yamls, args.tag, args.site, args, {} )    

#________________________________________________________________________

def test_create_the_workflow():
    (wf,ym,_) = commonWorkflow()
    assert(wf != None)
    assert(ym != None)

def test_we_should_be_able_to_parse_the_workflow():
    (wf,ym,_) = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )
    #pprint.pprint(dict_)

def test_workflow_has_expected_fields():
    (wf,ym,_) = commonWorkflow()
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
    (wf,ym,_) = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )
    inputs = dict_["inputs"]
    make_signal = inputs['signal']
    assert(make_signal == "string")
    
    make_background_1 = inputs["background"]
    assert(make_background_1=="string")

def test_workflow_outputs():
    (wf,ym,_) = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )
    outputs = dict_["outputs"]
    
def test_workflow_class():
    (wf,ym,_) = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )
    class_ = dict_["class"]
    assert(class_ == 'Workflow')

def test_workflow_steps():
    (wf,ym,_) = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )
    steps = dict_["steps"]
    for step in ["make_signal","make_background_1","make_background_2","pre_mix","combine","generate_some"]:
        mystep = steps[step]
        assert( mystep["run"]=="prun" )
        x = mystep["in"]
        y = mystep["out"]

def test_workflow_make_signal():
    (wf,ym,_) = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )
    steps = dict_["steps"]
    mystep=steps['make_signal']
    assert( mystep["run"]=="prun" )
    x = mystep["in"]
    for field in ["opt_inDS","opt_exec","opt_args"]:
        xx = x[field]
    y = mystep["out"]

def test_workflow_make_background_1():
    (wf,ym,_) = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )
    steps = dict_["steps"]
    mystep=steps['make_background_1']
    assert( mystep["run"]=="prun" )
    x = mystep["in"]
    for field in ["opt_inDS","opt_exec","opt_args"]:
        xx = x[field]    
    y = mystep["out"]
    
def test_workflow_pre_mix():
    (wf,ym,_) = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )
    steps = dict_["steps"]
    mystep=steps['pre_mix']
    assert( mystep["run"]=="prun" )
    x = mystep["in"]
    for field in ["opt_inDS","opt_inDsType","opt_secondaryDSs","opt_secondaryDsTypes","opt_exec","opt_args"]:
        xx = x[field]        
    y = mystep["out"]
    
def test_workflow_make_background_2():
    (wf,ym,_) = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )
    steps = dict_["steps"]
    mystep=steps['make_background_2']
    assert( mystep["run"]=="prun" )
    x = mystep["in"]
    for field in ["opt_inDS","opt_secondaryDSs","opt_secondaryDsTypes","opt_exec","opt_args"]:
        xx = x[field]            
    y = mystep["out"]        

def test_workflow_generate_some():
    (wf,ym,_) = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )
    steps = dict_["steps"]
    mystep=steps['generate_some']
    assert( mystep["run"]=="prun" )
    x = mystep["in"]
    for field in ["opt_exec","opt_args"]:
        xx = x[field]        
    y = mystep["out"]    
def test_workflow_combine():
    (wf,ym,_) = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )
    steps = dict_["steps"]
    mystep=steps['combine']
    assert( mystep["run"]=="prun" )
    x = mystep["in"]
    for field in ["opt_inDS","opt_inDsType","opt_secondaryDSs","opt_secondaryDsTypes","opt_exec","opt_args"]:
        xx = x[field]            
    y = mystep["out"]        
   

@pytest.mark.panda
def test_panda_should_validate_the_workflow():
    (cwl,yml) = commonWorkflow()
    name='panda_should_validate_this.cwl'
    with open( name, 'w' ) as f:
        f.write(cwl)
    with open( 'dummy.yaml', 'w' ) as f:
        f.write(yml)

    testds = 'user.%s.thisisatestoftheemergencybroadcastsystemthisisonlyatest'%( os.getlogin() )

    ret = os.system( "pchain --cwl %s --yaml %s --check --outDS %s"%(name,'dummy.yaml',testds) )
    assert( 0 == ret )
    
    

    


    
