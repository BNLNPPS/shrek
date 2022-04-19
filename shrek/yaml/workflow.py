import networks
from shrek.yaml.config import Config
from collections import defaultdict

# Assumes that jobs are static once added to the workflow graph

class WorkflowGraph:

    def __init__(self):
        self.jobs     = []                # list of jobs (nodes)
        self.jobsmap  = {}                # jobs indexed by name 
        self.jobs_in  = defaultdict(list) # list of jobs keyed by input       
        self.jobs_out = defaultdict(list) # list of jobs keyed by output
        self.inputs   = {}                # keys contain list of all inputs       
        self.outputs  = {}                # keys contain list of all outputs
        self.edges    = []                # list of edges (cached)

    def addJob(self,job):

        self.edges = [] # invalidate edge cache
        
        self.jobs.append(job)
        self.jobsmap[ job.name ] = job

        for inp in job.inputs:
            self.jobs_in[inp.name].append(job)
            self.inputs[inp.name]=1
            
        for out in job.outputs:
            self.jobs_out[out.name].append(job)
            self.outputs[out.name]=1

    def buildEdges(self):
        """
        Builds edges based on the current set of jobs and returns them
        """

        if len(self.edges) > 0:
            return self.edges
       
        inputs_list  = self.inputs.keys()
        outputs_list = self.outputs.keys()

        for ojob in self.jobs: # loop over all jobs

            # Loop over all outputs on this job
            for out in ojob.outputs:

                # Get the list of jobs which use this output as an input
                for ijob in self.jobs_in[out]:

                    self.edges.append( (ojob.name, ijob.name) )


                   
        
            
        

        
