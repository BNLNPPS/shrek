import networkx as nx
from shrek.yaml.config import Config
from collections import defaultdict
import pprint

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
        self.dsedges  = []
        self.graph    = None

    def addJob(self,job):

        self.edges = [] # invalidate edge cache
        self.dsedges = []
        self.graph = None # invalidate graph cache
        
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
        Builds edges based on the current set of jobs and returns them.
        Nodes are the *names* of jobs.

        """

        if  len(self.edges) > 0:
            return self.edges
       
        inputs_list  = self.inputs.keys()
        outputs_list = self.outputs.keys()

        for ojob in self.jobs: # loop over all jobs

            # Loop over all outputs on this job
            for out in ojob.outputs:

                print(out.name)

                # Get the list of jobs which use this output as an input
                for ijob in self.jobs_in[out.name]:

                    self.edges.append( (ojob.name, ijob.name) )
                    self.dsedges.append( out.name )

        return self.edges

    def buildDiGraph(self):

        if self.graph:
            return self.graph

        # Construct the directed graph from the list of edges, defined from output --> input
        edges = self.buildEdges()
        dsedges = self.dsedges      # buidEdges should return a tuple here...

        self.graph = nx.DiGraph()
        for edge, dset in zip(edges, dsedges):
            self.graph.add_edge( edge[0], edge[1], dataset=dset )

        #
        # Append some job descriptions to the nodes
        #
        #for i in enumerate(self.graph.nodes):
        #    job = self.jobsmap[ str( self.graph.nodes[i] ) ]
        #    self.graph.nodes[i]["comment"] = job.parameters.comment
        #    self.graph.nodes[i]["build"]   = job.parameters.build
        
        #for node in self.graph.nodes:
            #job = self.jobsmap[ str(node) ]
            #node["comment"] = job.parameters.comment
            #node["build"]   = job.parameters.build
            
        return self.graph

        

        
