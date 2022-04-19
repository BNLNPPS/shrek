class JobDefinition:
    def __init__(self,filename,definition):
        
        self.name       = None
        self.numInputs  = 0
        self.numSecondaries = 0
        self.numOutputs = 0
        self.filename   = filename # name of the job definition file
        self.definition = definition

        self.parameters = None
        self.inputs = []
        self.secondaries = []
        self.outputs = []
        self.localinit = None
        self.init = None
        self.commands = None # executable commands
        self.finish = None
        self.localfinish = None


