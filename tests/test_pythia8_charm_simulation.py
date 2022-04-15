from pykwalify.core import Core
import os

#________________________________________________________________________

def test_run_pass1_yaml_must_be_valid():
    yaml = "tests/pythia8-charm-simulation/runPass1.yaml"
    schema = ["schema/Parameters.yaml",
              "schema/InputDataSet.yaml",
              "schema/OutputDataSet.yaml",
              "schema/UserCommands.yaml",
              "schema/JobDefinition.yaml"]

    assert(os.path.exists( yaml ) )
    for s in schema:
        assert(os.path.exists(s))
    
    c = Core( source_file=yaml, schema_files=schema)
    c.validate(raise_exception=True)    

#________________________________________________________________________

def test_run_pass2_yaml_must_be_valid():
    yaml = "tests/pythia8-charm-simulation/runPass2.yaml"
    schema = ["schema/Parameters.yaml",
              "schema/InputDataSet.yaml",
              "schema/OutputDataSet.yaml",
              "schema/UserCommands.yaml",
              "schema/JobDefinition.yaml"]

    assert(os.path.exists( yaml ) )
    for s in schema:
        assert(os.path.exists(s))
    
    c = Core( source_file=yaml, schema_files=schema)
    c.validate(raise_exception=True)    
