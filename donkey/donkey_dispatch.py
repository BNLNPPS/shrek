#!/usr/bin/env python

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
import sys

import argparse

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

    def addRule( self, r ):
        self.rules.append(r)

    def run(self):

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
        

                
def parse_args( args ):
    parser = argparse.ArgumentParser(description='Examines messages DB for workflows to be dispatched according to rules specified on the command line')
    parser.add_argument( '--dbfile', dest='dbfile', help="Specifies the DB file to read")
    parser.add_argument( '--rule',   default=[], dest='rules',  action='append',  help='Name of the next rule' )
    parser.add_argument( '--actor',  default=[], dest='actors', action='append',  help='Actor script' )
    parser.add_argument( '--regex',  default=[], dest='regexs', action='append',  help='Regular expression to match' )
    parser.add_argument( '--scope',  default=[], dest='scopes', action='append',  help='Scope(s) to match' )
    parser.add_argument( '--event',  default=[], dest='events', action='append',  help='Event to match' )
    parser.add_argument( '--run',    default=False,dest='run',  action='store_true', help='Run the dispatcher' )
    return parser.parse_args( args )
    
# e.g.
#   donkey dispatch --dbfile messages.db --rule raw-events --actor actors/dispatch.sh --regex 'r"(\w)+EVENTS-(\d+)"' --scope group.sphenix --event close

dispatch = None
      
def run(argsin):
    global dispatch

    args = parse_args( argsin )
    dispatch = Dispatch( args.dbfile )
    d=dispatch

    head_ = [ "rule", "scopes", "event", "regex", "actor" ]
    rules_ = list( zip( args.rules,
                  args.scopes,
                  args.events,
                  args.regexs,
                  args.actors ))

    # Add dispatch rules
    for name_,scope_,event_,regex_,actor_ in rules_:
        r = Rule( name_ )
        r.scopes = scope_.split(',')
        r.event  = event_
        r.regex  = re.compile( r"%s"%regex_ )                
        r.actor  = actor_
        d.addRule(r)

    if len(rules_):
        print( tabulate.tabulate(rules_, headers=head_) )
    if args.run:
        d.run() # run the dispatcher


if __name__=='__main__':
    run(sys.argv[1:])

