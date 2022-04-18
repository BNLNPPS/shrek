#!/usr/bin/env python 

import yaml
import argparse
import pprint

from shrek.yaml.handler import Handler

handler = Handler()

#_______________________________________________________________________________________
class CodeBlock:
    def __init__(self):
        self.block = None

myinit = CodeBlock()
mylocalinit = CodeBlock()
myscript = CodeBlock()
myfinal = CodeBlock()
mylocalfinal = CodeBlock()

def initialize(key, block ):
    myinit.block = block

def initializelocal(key, block ):
    mylocalinit.block = block


def jobcommands(key,block):
    myscript.block = block

def finalize(key,block):
    myfinal.block=block

def finalizelocal(key,block):
    mylocalfinal.block = block


handler.addToken( 'Initialize',  initialize )
handler.addToken( 'LocalInit', initializelocal )
handler.addToken( 'JobCommands', jobcommands )
handler.addToken( 'Finalize', finalize )
handler.addToken( 'LocalFinalize', finalizelocal )

#_______________________________________________________________________________________
class ParameterBlock:
    def __init__(self):
        self.params = ""
myparameters = ParameterBlock()

def parameters( key, params ):
    output = \
"""
# Job parameters block
"""
    for k, v in params.items():
        output = output + "export %s=%s\n"%(k,str(v))

    myparameters.params = output
    return output

handler.addToken( 'Parameters', parameters )
#_______________________________________________________________________________________
class OutputDS:
    def __init__(self):
        self.name = None
        self.comment = None
        self.merge = False
        self.filelist = []        
myoutlist = []
def outputs(key, dslist ):
    for ds in dslist:
        o = OutputDS()
        for (k,v) in ds.items():
            if k=="name": o.name = v
            if k=="comment": o.comment = v
            if k=="merge": o.merge = v
            if k=="filelist": o.filelist = v
        myoutlist.append(o)
handler.addToken( 'OutputDataSets', outputs )
#_______________________________________________________________________________________
class InputDS:
    def __init__(self):
        self.name = None
        self.comment = None
        self.altname = None
        self.nFilesPerJob = None
        self.match = None
        self.nSkip = None
        self.nFiles = None
        self.local = None
        self.localFiles = None
    def str(self):
        return "%s n=s"%( self.name, str(self.nFiles) )
myinputs = []
def inputs(key, dslist ):
    for ds in dslist:
        i = InputDS()
        for (k,v) in ds.items():
            if k=='name': i.name = v
            if k=='comment': i.comment = v
            if k=='altname': i.altname = v
            if k=='nFilesPerJob': i.nFilesPerJob = v
            if k=='match' : i.match = v
            if k=='nSkip': i.nSkip = v
            if k=='nFiles': i.nFiles = str(v) # number or "ALL"
            if k=='local': i.local = v
            if k=='localFiles': i.localFiles = v
        myinputs.append(i)
handler.addToken( 'InputDataSets', inputs )

class SecondaryDS:
    def __init__(self):
        self.name = None
        self.comment = None
        self.altname = None
        self.nFilesPerJob = None
        self.match = None
        self.nSkip = None
        self.nFiles = None
        self.local = None
        self.localFiles = None
mysecondaries = []
def secondaries(key, dslist ):
    for ds in dslist:
        i = SecondaryDS()
        for (k,v) in ds.items():
            if k=='name': i.name = v
            if k=='comment': i.comment = v
            if k=='altname': i.altname = v
            if k=='nFilesPerJob': i.nFilesPerJob = v
            if k=='match' : i.match = v
            if k=='nSkip': i.nSkip = v
            if k=='nFiles': i.nFiles = str(v) # number or "ALL"
            if k=='local': i.local = v
            if k=='localFiles': i.localFiles = v
        mysecondaries.append(i)

handler.addToken( 'SecondaryDataSets', secondaries)        
#_______________________________________________________________________________________


#_______________________________________________________________________________________        



parser = argparse.ArgumentParser(description='Build job script from yaml')
parser.add_argument('yaml', metavar='YAML', type=str, 
                                        help='input filename')

parser.add_argument('--tag', type=str, help='production tag' )

args = parser.parse_args()
    
with open(args.yaml, "r") as stream:

    definition = yaml.safe_load(stream)   
    handler.traverse( definition )


#
# Convention is that the PanDA will pass certain parameters
# through the command line
#
print( \
"""

# Script command line parameters

export shrek_job_index=$1
if [ "$2" ]; then
   export shrek_input_files=( `echo $2 | tr "," " "` )
   export shrek_number_input_files=${#shrek_input_files[@]}
fi
if [ "$3" ]; then
   export shrek_secondary_files=( `echo $3 | tr "," " "` )
   export shrek_number_secondary_files=${#shrek_input_files[@]}
fi

"""
)

#
# Build and output environment block
#
print(myparameters.params)

#
# Data set names
#
for (i,ds) in enumerate(myinputs):
    print("# Input DS %s [%i/%i]"%(ds.name,i+1,len(myinputs)))
    print("export inputDS%i_name=%s"%(i+1,ds.name) )

print("")    

for (i,ds) in enumerate(mysecondaries):
    print("# Secondary DS %s [%i/%i]"%(ds.name,i+1,len(myinputs)))
    print("export secondaryDS%i_name=%s"%(i+1,ds.name) )    

#
# Worker node initialization
#
if myinit.block:
    print( myinit.block )

#
# Worker node execution script
#
if myscript.block:
    print( myscript.block )

#
# Worker node finalization
#
if myfinal.block:
    print( myfinal.block )
