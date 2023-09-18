from shrek.scripts.simpleLogger import DEBUG, INFO, WARN, ERROR, CRITICAL

import tabulate
import pprint

from donkey.dataset import dataset_collection as collection
from donkey.dataset import dataset

import datetime
import uuid

import re
import sh
import os

def captureActorOutput( out ):
    INFO('| '+ out)

def captureActorError( out ):
    INFO('| ' + out)    

class Rule:
    def __init__(self,name_):
        self.name     = name_   # name of the rule
        self.scopes   = []      # scope(s) to monitor
        self.event    = ""      # event state 
        self.regex    = re.compile('$^')  # regex(es) to be applied to the dataset (default never matches)
        self.actor    = ""      # list of actors to be applied to the dataset

    def matches_event(self,ds):
        event_check = False
        if self.event == ds.event:
            event_check = True
        return event_check

    def matches_scope(self,ds):
        return ds.scope in self.scopes      

    def matches_name(self,ds):
        result = self.regex.match( ds.name )
        return result

    def matches(self,ds):
        name = ds.name
        event_check = self.matches_event(ds)
        scope_check = self.matches_scope(ds)
        name_check  = self.matches_name(ds)
        result = None
        if event_check and scope_check:
            result = name_check
        return result



class Dispatch:
    def __init__(self,dbfile):
        self.rules    = []
        self.dbfile   = dbfile
        self.pending  = []
        self.dispatch = []
        self.dropped  = []

    def addRule( r ):
        self.rules.append(r)

    def run(self):
        # read in the messages file
        coll = collection(self.dbfile)

        # Clear 
        self.pending  = []
        self.dispatch = []
        self.dropped  = []

        while coll.length('pending') > 0:

            # Pop all pending datasets
            ds = coll.pop( 'pending' )

            # dispatch work to any and all matching actors
            ismatched = False
            for r in self.rules:

                ok_scope = r.matches_scope(ds)
                ok_name  = r.matches_name(ds)

                if r.matches(ds) is not None:
                    ismatched = True
                    ds.dispatched = str( datetime.datetime.utcnow() )
                    action = "%s %s %s %s" %( r.actor, ds.name, ds.runnumber, ds.created )
                    self.dispatch.append( action )

                # Matched one or more rules, goes to dispatched
                if ismatched:
                    coll.add( 'dispatched', ds )

                # Not in scope or not a name matching our rules... drop the dataset
                elif not ok_scope or not ok_name:
                    coll.add( 'dropped', ds )

                # Otherwise return to the pending queue
                else:
                    self.pending.append( ds )

        # Return pending datasets to pending collection
        for ds in self.pending:
            coll.add( 'pending', ds )

        # Done with using the DB file, so now we can dispatch the work
        for d in self.dispatch:
            args = d.split()
            #pprint.pprint(args[1:])
            act = sh.Command( args[0] )
            act( args[1:], _out=captureActorOutput, _err=captureActorError, _env=os.environ.copy() )
        

                

            

        

        





      
if __name__=='__main__':

    # setup rules for dispatching work
    r = Rule("submit run to panda")
    r.event = "closed"
    r.scope = [ "user.jwebb2", "group.sphenix" ]
    r.match = re.compile( r"(\w)+CALOR-(\d+)" )

    dispatch = Dispatch("donkey-listener")

    dispatch.rules  = [ r ]
    dispatch.actors = [ "donkey/dispatch.sh" ]
    
    dispatch.run()
