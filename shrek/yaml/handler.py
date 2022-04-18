import sys
import pprint

class Handler:

    def __init__(self,options=None):

        self.syntax  = {}
        self.result_  = {}
        
        #              Tag      Class which handles tag
        # ===================================================
        self.addToken( 'Parameters'  ,      None, None )
        self.addToken( 'InputDataSets',     None, [] )
        self.addToken( 'SecondaryDataSets', None, [] )
        self.addToken( 'OutputDataSets',    None, [] )
        self.addToken( 'Initialize',        None, None )
        self.addToken( 'LocalInit',         None, None )
        self.addToken( 'Finalize',          None, None )
        self.addToken( 'LocalFinalize',     None, None )
        self.addToken( 'JobCommands',       None, None )

        self.actor = None

    # Register action to a given structure
    def addToken(self,token,handler,default_):
        self.syntax[ token ] = handler
        self.result_[ token ] = default_

    def traverse(self,dictionary):
        for key, value in dictionary.items():
            action = self.syntax[key] # Key Error is serious...
            if action:
                self.result_[key] = action( key, value )

    def result(self,key):
        myresult = self.result_[key]            
        return myresult


            

