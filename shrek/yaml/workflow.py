import networks
from shrek.yaml.config import Config

class Workflow:
    def __init__(self):
        jobs  = [] # list of jobs (nodes)
        outin = [] # pairs of jobs connected output --> input (edges)

        
