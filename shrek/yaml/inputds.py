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
