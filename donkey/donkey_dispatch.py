from shrek.scripts.simpleLogger import DEBUG, INFO, WARN, ERROR, CRITICAL

import tabulate
import pprint

from donkey.dataset import dataset_collection as collection
from donkey.dataset import dataset

import datetime
import uuid

import re
import sh


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
        self.actors   = ""      # list of actors to be applied to the dataset

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

    def execute(self,ds):
        if self.matches(ds):
            for a in self.actors:
                actor = sh.Command(a)
                actor( ds, _out=captureActorOutput, _err=captureActorError, _env=os.environ.copy() )



class Dispatch:
    def __init__(self,dbfile):
        self.rules  = []
        self.dbfile = dbfile

    def run(self):
        coll = collection( self.dbfile )
        pend = coll.getall("pending")
        pprint.pprint(pend)




      
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
