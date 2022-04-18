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

def buildSecondaryList( key, dslist ):
    myinputs = []
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
        myinputs.append(i)
    return myinputs
