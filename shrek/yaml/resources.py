class Resource:
    def __init__(self):
        self.url = None
        self.action = None
        self.type = None

    def addFile(self,filename):
        self.url = filename
        self.type = "file"

    def addDirectory(self,filename):
        self.url = filename
        self.type = "directory"

    def addRepository(self,ra):        
        self.url    = ra['url']
        self.action = ra['action']
        self.type = "repo"

def buildResourceList( key, rslist ):
    myresources = []
    for rs in rslist:
        for k,v in rs.items():
            r = Resource()
            if k=='file':
                r.addFile( v )
            if k=='directory':
                r.addDirectory( v )
            if k=='repository':
                r.addRepository( v )
            if r.type:
                myresources.append(r)

    return myresources       
            
    
