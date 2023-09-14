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

from rucio.client import Client
client = Client()

def handle_create_dts( meta, messages, skip ):
    """
    A new dataset has been created in rucio
    """
    utcnow = str(datetime.datetime.utcnow())
    account = meta.get( 'account', 'unknown' )
    
    if account not in skip:
        ds = dataset()
        ds.name      = meta.get('name','unknown')
        ds.runnumber = int( meta.get('run_number','0') )
        ds.event     = 'created'
        ds.created   = utcnow
        ds.account   = account
        ds.scope     = meta.get('scope',  'unknown')
        messages.add( 'pending', ds )

def handle_close( meta, messages, skip ):
    """
    A dataset has been closed in rucio
    """
    utcnow = str(datetime.datetime.utcnow())
    account = meta.get( 'account', 'unknown' )

    if account not in skip:
        name = meta['name']
        ds = messages.find('pending',name)
        ds.event     = 'closed'
        ds.closed    = utcnow
        messages.update( 'pending', ds )


def handle_open( meta, messages, skip ):
    """
    A dataset has been opened in rucio
    """
    utcnow = str(datetime.datetime.utcnow())
    account = meta.get( 'account', 'unknown' )

    if account not in skip:

        name = meta['name']
        ds = messages.find('processed',name)
        messages.rem( 'processed', ds )       # remove from processed collection
        ds.event     = 'opened'
        ds.opened    = utcnow
        messages.add( 'pending', ds )         # add back to pending
        
        

    

message_actors = {
    'create_dts' : handle_create_dts,
    'open'       : handle_open,
    'close'      : handle_close,    
    }


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

        body    = json.loads( str(frame.body) )
        event   = body["event_type"]
        payload = body["payload"]
        name    = payload["name"]
        scope   = payload["scope"]
        meta    = client.get_metadata( scope, name )


        actor = message_actors.get( event, None )

        if actor:
            actor( meta, self.messages, self.skip_accounts )

        else:
            print("Unknown actor for event %s"%event )

        
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
