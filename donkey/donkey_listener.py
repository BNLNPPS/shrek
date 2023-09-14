from shrek.scripts.simpleLogger import DEBUG, INFO, WARN, ERROR, CRITICAL
from shrek.scripts.ShrekConfiguration import readSiteConfig

import tabulate
import pprint
import stomp

from donkey.dataset import dataset_collection as collection
from donkey.dataset import dataset

import datetime
import uuid

import os
import sh

import time
import json

import signal



class Listener( stomp.ConnectionListener ):
    def __init__(self,connection_,dbfilename,events_=[]):
        self.connection = connection_
        self.messages   = collection( dbfilename )
        self.events     = events_
        # Add lists for each event class (if the list doesn't already
        # exist in the db file).

        self.skip_accounts = ['iddsv1','unknown']
                  
        for event in events_:
            if not self.messages.db.exists(event):
                self.messages.addlist( event, 'collections' )

    def on_error( self, frame ):
        ERROR('recieved %s' % frame.body)

    def on_message( self, frame ):
        utcnow = str(datetime.datetime.utcnow())

        print( str(frame.headers) )            
        print( str(frame.body) )

        body = json.loads( str(frame.body) )

        event   = body["event_type"]        
        payload = body["payload"]

        # Only handle known events
        if True:

            account_ = "unknown"
            try:
                account_ = payload['account']
            except KeyError:
                account_ = "unknown"

            if account_ not in self.skip_accounts:

                # Create a new dataset to persist the message
                ds = dataset()
                ds.name      = payload['name'] # name of the dataset
                ds.runnumber = 0               # TODO: should be encoded in the dataset name
                ds.event     = event           # type of event
                if event=='create_dts':
                    ds.created = utcnow
                elif event=='close':
                    ds.closed  = utcnow
                elif event=='open':
                    ds.reopened = utcnow
                try:
                    ds.account = payload['account']
                except KeyError:
                    ds.account = "unknown"
                try:
                    ds.scope   = payload['scope']
                except KeyError:
                    ds.scope   = "unknown"

                # Add the dataset to the appropriate list in the collection
                self.messages.add( 'pending', ds )

        
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
       
def readConfig( filename = None ):    
    defaults = readSiteConfig()    
    return defaults['Donkey']

def run( sleeps, dbfilename ):
    DEBUG("donkey.listener is starting")
    defaults = readConfig()
    pprint.pprint(defaults)

    DEBUG("Get connection")
    connection = stomp.Connection( [( defaults['host'], defaults['port'] )] )

    DEBUG("Disabling ctrl-c...")
    sig_restore = signal.signal(signal.SIGINT, signal.SIG_IGN)

    DEBUG("Start listener")
    listener = Listener( connection, dbfilename, ['pending','processed'] )
    connection.set_listener( 'donkey_ears', listener )

    #def connectAndSubscribe( args, defaults, connection ):

    DEBUG("Connect and subscribe")
    user     = "donkey"
    password = "donkey"
    queue    = defaults['queue']
    ack_     = 'auto'
    sub, _   = defaultSubscriptionId( os.path.expanduser( os.path.expandvars( defaults['config'] ) ) )

    connection.connect( user, password, wait=True )
    connection.subscribe( destination=queue, id=sub, ack=ack_ )    

    for i in sleeps:
        DEBUG("Main thread sleeping for %i min"%i)
        time.sleep(60)

        
if __name__=='__main__':
    #run( [5,4,3,2,1], 'donkey-listener' )
    run( [1], 'donkey-listener' )
