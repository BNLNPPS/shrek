import sys

class Handler:

    def __init__(self,options=None):

        self.syntax  = {}        
        #              Tag      Class which handles tag
        # ===================================================
        self.addToken( 'Parameters'  ,      None )
        self.addToken( 'InputDataSets',     None )
        self.addToken( 'SecondaryDataSets', None )
        self.addToken( 'OutputDataSets',    None )
        self.addToken( 'Initialize',        None )
        self.addToken( 'LocalInit',         None )
        self.addToken( 'Finalize',          None )
        self.addToken( 'LocalFinalize',     None )
        self.addToken( 'JobCommands',       None )

        self.actor = None

    # Register action to a given structure
    def addToken(self,token,handler):
        self.syntax[ token ] = handler;

    def traverse(self,dictionary):
        for key, value in dictionary.items():
            action = self.syntax[key]
            if action:
                action( key, value )


            

