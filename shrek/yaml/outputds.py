class OutputDS:
    def __init__(self):
        self.name = None
        self.comment = None
        self.merge = False
        self.filelist = []

def buildOutputList(key, dslist):
    myoutlist = []
    for ds in dslist:
        o = OutputDS()
        for (k,v) in ds.items():
            if k=="name": o.name = v
            if k=="comment": o.comment = v
            if k=="merge": o.merge = v
            if k=="filelist": o.filelist = v
        myoutlist.append(o)
    return myoutlist
