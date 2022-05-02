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
        "tests/simple-chain/top.yaml",
        "tests/simple-chain/bottom.yaml"
        ]

    return buildCommonWorkflow( yamls, "sP-TEST", "BNL_OSG_SPHENIX" )    

#________________________________________________________________________

def test_create_the_workflow():
    (wf,ym) = commonWorkflow()
    assert(wf != None) # CWL
    assert(ym != None) # yaml

def test_we_should_be_able_to_parse_the_workflow():
    (wf,yml) = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )  # workflow

def test_workflow_class():
    (wf,yml) = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )
    class_ = dict_["class"]
    assert(class_ == 'Workflow')

def test_workflow_steps():
    (wf,yml) = commonWorkflow()
    dict_ = yaml.safe_load( str(wf) )
    steps = dict_["steps"]
    for step in ["top","bottom"]:
        mystep = steps[step]
        assert( mystep["run"]=="prun" )
        x = mystep["in"]
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

    ret = os.system( "pchain --cwl %s --yaml %s --check --outDS %s --vo wlcg --prodSourceLabel test --workingGroup sphenix"%(name,'dummy.yaml',testds) )
    assert( 0 == ret )
    
    

    


    
