from pykwalify.core import Core
import os
import yaml
import glob

from shrek.yaml.handler import Handler

#
# The test data files should all pass the schema
#


# pykwalify -d tests/data/EnvironmentBlock.yaml -s schema/Parameters.yaml
def test_environment_block():

    yaml_   = "tests/data/Parameters.yaml"
    schema = ["schema/Parameters.yaml","tests/schema/Parameters.yaml"]

    assert(os.path.exists( yaml_ ) )
    for s in schema:
        assert(os.path.exists(s))

    
    c = Core( source_file=yaml_, schema_files=schema)
    c.validate(raise_exception=True)


# pykwalify -d tests/data/InputDataSet.yaml -s schema/InputDataSet.yaml
def test_inputdataset_block():

    yaml_   = "tests/data/InputDataSet.yaml"
    schema = ["schema/InputDataSet.yaml","tests/schema/InputDataSet.yaml"]

    assert(os.path.exists( yaml_ ) )
    for s in schema:
        assert(os.path.exists(s))
    
    c = Core( source_file=yaml_, schema_files=schema)
    c.validate(raise_exception=True)


# pykwalify -d tests/data/OutputDataSet.yaml -s schema/OutputDataSet.yaml
def test_outputdataset_block():

    yaml_   = "tests/data/OutputDataSet.yaml"
    schema = ["schema/OutputDataSet.yaml","tests/schema/OutputDataSet.yaml"]

    assert(os.path.exists( yaml_ ) )
    for s in schema:
        assert(os.path.exists(s))
    
    c = Core( source_file=yaml_, schema_files=schema)
    c.validate(raise_exception=True)


# pykwalify -d tests/data/UserCommands.yaml -s schema/UserCommands.yaml
def test_usercommands_block():

    yaml_   = "tests/data/UserCommands.yaml"
    schema = ["schema/UserCommands.yaml","tests/schema/UserCommands.yaml"]

    assert(os.path.exists( yaml_ ) )
    for s in schema:
        assert(os.path.exists(s))
    
    c = Core( source_file=yaml_, schema_files=schema)
    c.validate(raise_exception=True)


# pykwalify -d tests/data/JobDefinition.yaml -s schema/JobDefinition.yaml
def test_jobdefinition_block():

    yaml_   = "tests/data/JobDefinition.yaml"
    schema = ["schema/Parameters.yaml",
              "schema/InputDataSet.yaml",
              "schema/OutputDataSet.yaml",
              "schema/UserCommands.yaml",
              "schema/JobDefinition.yaml"
              ]

    assert(os.path.exists( yaml_ ) )
    for s in schema:
        assert(os.path.exists(s))
    
    c = Core( source_file=yaml_, schema_files=schema)
    c.validate(raise_exception=True)


def test_yaml_handler():
    yaml_   = "tests/data/JobDefinition.yaml"

    handler = Handler()
    with open( yaml_, "r" ) as stream:

        definition = yaml.safe_load(stream)
        handler.traverse( definition )

def test_more_complicated_example_definitions_match_schema():
    schema = ["schema/Parameters.yaml",
              "schema/InputDataSet.yaml",
              "schema/OutputDataSet.yaml",
              "schema/UserCommands.yaml",
              "schema/JobDefinition.yaml"
              ]

    for yaml_ in glob.glob( 'tests/more-comlicated-example/*.yaml' ):
        c = Core( source_file=yaml_, schema_files=schema)
        c.validate(raise_exception=True)


