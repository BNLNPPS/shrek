import networks
from shrek.yaml.config import Config
from collections import defaultdict

class Workflow:
    def __init__(self):
        self.jobs     = []                # list of jobs (nodes)
        self.jobs_in  = defaultdict(list) # list of jobs keyed by input       
        self.jobs_out = defaultdict(list) # list of jobs keyed by output
        self.inputs   = {}                # keys contain list of all inputs       
        self.outputs  = {}                # keys contain list of all outputs
        self.edges    = []                # list of edges

    def addJob(self,job):
        self.jobs.append(job)

        for inp in job.inputs:
            self.jobs_in[inp.name].append(job)
            self.inputs[inp.name]=1

        for out in job.outputs:
            self.jobs_out[out.name].append(job)
            self.outputs[out.name]=1
        

        
