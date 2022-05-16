from pykwalify.core import Core
import os
import yaml
import pprint
import pytest
import glob

from shrek.scripts.buildJobScript import buildJobScript
from shrek.scripts.buildJobScript import buildJobDefinition
from shrek.scripts.buildWorkflowGraph import buildWorkflowGraph
from shrek.scripts.buildCommonWorklow import buildCommonWorkflow

jobDefinitions = glob.glob( "tests/pythia8-charm-simulation/*.yaml" )


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
        tag  = "sP-PYTHIA8-TEST"
    args = Args()        

    return buildCommonWorkflow( yamls, args.tag, args.site, args )

#________________________________________________________________________

def validateAgainstSchema( yaml_ ):
    schema = ["schema/Parameters.yaml",
              "schema/InputDataSet.yaml",
              "schema/OutputDataSet.yaml",
              "schema/UserCommands.yaml",
              "schema/ResourceMap.yaml",
              "schema/JobDefinition.yaml"]

    assert(os.path.exists( yaml_ ) )
    for s in schema:
        assert(os.path.exists(s))

    c = Core( source_file=yaml_, schema_files=schema)
    c.validate(raise_exception=True)        

    with open(yaml_,"r") as stream:
        definition = yaml.safe_load(stream)

#________________________________________________________________________
def buildTheJobRuntimeScript( yaml_, tag = 'sP22aa-TEST' ):
    script = buildJobScript( yaml_, tag )
    assert(script)

#________________________________________________________________________
@pytest.mark.parametrize( 'yaml_', jobDefinitions )
def test_validate_against_schema( yaml_ ):
    validateAgainstSchema( yaml_ )

@pytest.mark.parametrize( 'yaml_', jobDefinitions )
def test_build_the_runtime_script( yaml_ ):
    buildTheJobRuntimeScript( yaml_ )

#________________________________________________________________________
def test_create_the_workflow():
    commonWorkflow()

