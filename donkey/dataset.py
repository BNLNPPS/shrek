import datetime
import pickledb

class dataset:
    def __init__(self):
        time0 = "%s" % datetime.datetime.fromtimestamp( 0 ) 
        self.name       = ""                                          # name of the dataset
        self.runnumber  = 0                                           # corresponding run
        self.event      = "none"                                      # last activemq event
        self.created    = time0                                       # time event happened
        self.closed     = time0                                       # time dataset was closed
        self.dispatched = time0                                       # time job dispatched
        self.account    = "none"
        self.scope      = "none"
        self.bytes      = "nan"
        self.length     = "nan"

    def adopt(self,dict_):
        """
        Adopt values from dictionary
        """
        self.__dict__.update(dict_)

class dataset_collection:
    def __init__(self,pickle_,name_=None,title_=None):
        self.pickle = pickle_
        self.db     = pickledb.load( self.pickle + '.db', True )
        if name_:
            self.addlist(name_,title_)               

    def addlist(self,name_,title_):
        self.db.lcreate(name_)
        self.db.set(name_+".meta", { 'title':title_ } )
        

    def add( self, lname, ds ):
        """
        Add a dataset to the specified list in the collection
        """
        if self.exists( lname, ds ):
            print("Only one copy of dataset allowed at a time")
        else:
            self.db.ladd( lname, ds.__dict__ )

    def pop( self, lname ):
        """
        Pop a datase from the specified list in the collection
        """
        result = self.db.lpop( lname, 0 )
        ds = dataset()
        ds.adopt( result )
        return ds

    def exists( self, name, ds ):
        """
        Check if the dataset exists on the specified list and return true if it does
        """
        result = self.db.lexists(name,ds.__dict__)
        return result

    def rem( self, name, ds ):
        if type(ds) is dict:
            self.db.lremvalue( name, ds )
        else:
            self.db.lremvalue( name, ds.__dict__ )

    def update( self, name, d ):
        """
        Update the specified dataset in the collection
        """
        for x in self.db.lgetall(name):
            if x['name']==d.name:
                self.rem( name, x )
                self.add( name, d)
        


        
        
 
