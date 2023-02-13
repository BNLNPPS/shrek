#!/usr/bin/env python

import yaml
import argparse
import shutil
import datetime
import sh
import time
import stomp
import uuid
import os
import json
import cmd

from shrek.scripts.simpleLogger import DEBUG, INFO, WARN, ERROR, CRITICAL

from shrek.scripts.ShrekConfiguration import readSiteConfig

def readConfig( filename = None ):    
    defaults = readSiteConfig()    
    return defaults['Donkey']

def readWatchFile( filename ):
    result = {}
    if filename:
        with open(filename,'r') as stream:
            try:
                result = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

    print(result)
    return result

def makeBackupFile( filename ):
    newfile=filename+";1"


def createAndCacheSubscriptionId( idhx=None ):
    """
    Creates and returns a new, unique ID for this session.  It will be used as the
    subscription ID for activeMQ messaging.  It will be cached to a file and used
    for subsequent sessions.
    """

    result = ""
    oldid  = None
    if idhx:
        result = idhx
    else:
        result = uuid.uuid1().hex

    if not os.path.exists( ".donkey/" ):
        WARN("Creating .donkey directory")
        sh.mkdir( ".donkey" )

    if os.path.exists( ".donkey/subscription-id" ):
        WARN("Overwriting existing subscription id")
        # --> implies we will unsubscribe the existing connection
        with open( ".donkey/subscription-id", 'r' ) as o:
            oldid = o.readline()

    with open( ".donkey/subscription-id", "w") as idfile:
        idfile.write( result )

    return (result, oldid)

def readCachedSubscriptionId( idfile=".donkey/subscription-id" ):
    """
    Reads the subscription ID from the cached file.
    """

    if not os.path.exists( ".donkey/subscription-id" ):
        CRITICAL("Could not load .donkey/subscription-id" )
        raise FileNotFoundError

    result = ""
    with open( idfile, 'r' ) as idf:
        result = idf.readline()

    return (result.strip(), None)

def defaultSubscriptionId( configdir ):
    """
    Initialize default subscription ID for this session.  It is
    always either a new subscriotion ID or a cached subscription
    ID.
    """ 
    sid = None
    if os.path.exists( configdir + "/subscription-id" ):
        sid = readCachedSubscriptionId()
    else:
        sid = createAndCacheSubscriptionId()

    return sid
      
INFO( "-----------------------------------------------------------------------------------------")
INFO( "Hello Donkey" )
INFO( "" )
INFO( "This’ll be fun. We’ll stay up late, swapping manly stories, and in the morning...")
INFO( "I’m making waffles!")
INFO( "-----------------------------------------------------------------------------------------")

def connectAndSubscribe( args, defaults, connection ):
    user     = args.user
    password = args.password
    queue    = defaults['queue']
    ack_     = 'auto'

    connection.connect( user, password, wait=True )
    connection.subscribe( destination=queue, id=args.subscription, ack=ack_ )

class Message:
    def __init__(self, frame ):
        #self.headers = json.loads( str(frame.headers) )
        self.body    = json.loads( str(frame.body) )

class DispatchListener( stomp.ConnectionListener ):
    def __init__(self,connection):
        self.connection = connection
        self.messages = []
    def on_error( self, frame ):
        ERROR('recieved %s' % frame.body)
    def on_message( self, frame ):
        INFO('recieved %s' % str(frame.headers) )
        INFO('         %s' % str(frame.body) )
        self.messages.append( Message(frame) )
        INFO('%i pending'%len(self.messages))
        for m in self.messages:
            INFO( m.body['payload']['name'] )
    def on_disconnected(self):
        WARN("disconnected, attempting to reconnect...")
        connectAndSubscribe( self.connection )

class DonkeyShell( cmd.Cmd ):
    intro  = "Welcome to Donkey shell."
    prompt = "donkey> "
    file_  = None

    def do_nada(self, arg):
        pass

    def do_exit(self, arg):
        """
        Exits command loop and falls through to disconnect.
        """
        print("This is the end of donkey.  Goodbye.")
        return True
        

    
        
def readWatchFile( filename ):
    result = {}
    if filename:
        with open(filename,'r') as stream:
            try:
                result = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

    print(result)
    return result

def captureActorOutput( out ):
    INFO('| ' + out)

def captureActorError( out ):
    INFO('| ' + out)    

def parse_args( defaults ):
    """
    Parse the command line arguments.  Defaults will be set from the shrek site configuraion file.
    """

    parser = argparse.ArgumentParser(description='Monitors activeMQ for new datasets and dispatches work')

    parser.add_argument('--dispatch',    dest='dispatch', action='store_true',  help="Dispatch datasets to actors")
    parser.add_argument('--no-dispatch', dest='dispatch', action='store_false', help="No dispatch, just print out command")
    parser.set_defaults(dispatch=False)

    parser.add_argument('--actor',     dest='actors', action='append',  help="Attach an actor, which will be run on each matching dataset, with the ds passed as the first argument and a running count as the second." )
    parser.set_defaults(actors=[])

    """
    Default subscription ID will be stored in the config directory as 'subscription-id'.  It is created if it does not exist.
    """
    parser.add_argument('--subscription-id', dest='subscription', help="Set the subscription ID for this session.  (n.b. must be unique.)")
    parser.set_defaults( subscription=None )
    parser.add_argument('--new-subscription', dest='newsubscription', action='store_true', help='New subscription')
    parser.set_defaults( newsubscription=False )    

    parser.add_argument('--user',     dest='user', type=str, help='ActiveMQ username')
    parser.set_defaults( user=None )
    parser.add_argument('--password', dest='password', type=str, help='ActiveMQ password')
    parser.set_defaults( password=None )

    parser.add_argument( '--watch-file', dest='watchfile', type=str, help="Definition file")
    parser.set_defaults( watchfile=None )
    parser.add_argument( '--match', type=str, help='Datasets match this string (only "*" allowed as wildcards)' )
    parser.set_defaults( match=defaults['match'] )
    parser.add_argument( '--conditions', type=str, help='Filter conditions applied to dataset metadata' )
    parser.set_defaults( conditions=None )    


    return parser.parse_known_args()

def dropCurrentSubscription( current ):
    WARN("dropCurrentSubscription not yet implemented")
    pass

def main():

    defaults = readConfig()
    args, extras = parse_args( defaults )

    connection = stomp.Connection( [( defaults['host'], defaults['port'] )] )    
    connection.set_listener( 'dispatch', DispatchListener( connection ) )
    
    curr_subid   = None
    past_subid   = None

    if args.subscription !=None and not args.newsubscription:
        """
        User defined subscription for this session only
        """
        # first convert
        args.subscription = uuid.UUID( args.subscription ).hex
        curr_subid        = args.subscription
        WARN("Subscription ID set to %s for this session"%args.subscription)

    elif args.subscription !=None and args.newsubscription:
        """
        New, user defined subscription to persist across sessions is disallowed.
        """
        CRITICAL("Subscription ID cannot be set to user value %s."%args.subscription)
        assert(0)

    elif args.subscription==None and not args.newsubscription:
        """
        Reconnect to existing subscription (or create new one if none exists)
        """
        (args.subscription, _junk) = defaultSubscriptionId( os.path.expanduser( os.path.expandvars( defaults['config'] ) ) )
        curr_subid = args.subscription
        past_subid = args.subscription
        INFO("Reconnect to existing subscription %s" % curr_subid)        

    elif args.subscription==None and args.newsubscription:
        """
        Drop existing subscription and create new one
        """        
        (curr_subid, past_subid) = createAndCacheSubscriptionId()
        args.subscription = curr_subid
        WARN("Dropped existing subscription %s"%past_subid)
        connection.unsubscribe( past_subid )
        INFO("Connect to new subscription %s"%curr_subid)



    connectAndSubscribe(args, defaults, connection)
    #time.sleep(300)

    DonkeyShell().cmdloop()

    
    connection.disconnect()
        

if __name__ == '__main__':
    main()


