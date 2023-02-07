class InputDS:
    def __init__(self):
        self.name = None
        self.datasets = None
        self.comment = None
        self.altname = None
        self.nFilesPerJob = None
        self.nEventsPerFile = None
        self.match = None
        self.nSkip = None
        self.nFiles = None
        self.local = None
        self.localFiles = None
        self.reusable = None

def buildInputList( key, dslist ):
    myinputs = []
    for ds in dslist:
        i = InputDS()        
        for (k,v) in ds.items():
            if k=='name': i.name = v
            if k=='datasets': i.datasets = v
            if k=='comment': i.comment = v
            if k=='altname': i.altname = v
            if k=='nFilesPerJob': i.nFilesPerJob = v
            if k=='nEventsPerFile': i.nEventsPerFile = v                
            if k=='match' : i.match = v
            if k=='nSkip': i.nSkip = v
            if k=='nFiles': i.nFiles = str(v) # number or "ALL"
            if k=='local': i.local = v
            if k=='localFiles': i.localFiles = v
            if k=='reusable':
                print('reusable = ' + str(v) )
                if v=='False': pass
                if str(v)=='True' or str(v)=='true': i.reusable=True
        myinputs.append(i)
        # Check requirements
        if i.name == None:
            print("Dataset with unspecified name")
            assert(0)
        if i.match == None:
            print("Dataset %s mush specify matching files"%i.name)            
    return myinputs
