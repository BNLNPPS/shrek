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
import re
import queue
import pandas as pd
import threading
import editor

def captureActorOutput( out ):
    INFO('| ' + out)

def captureActorError( out ):
    INFO('| ' + out)

# Watch file column descriptions
watch_file_columns = ["actors","prescale","scope","match","count", "enable"]
watch_file_template = """
actors:
  - [user script]
prescale: 1
scope:  [rucio scope]
match:  "[regular expression]"
count:  0
enable: "no"
"""


dispatch = pd.DataFrame( columns=watch_file_columns )
listener = None  # DispatchListener
dmanager = None  # DispatchManager

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
INFO("... I mean, white sparkly teeth! I know you probably hear this all the time from your food,")
INFO(" but you must bleach or something 'cause that's one dazzling smile you got there! And do I")
INFO(" detect a hint of minty freshness?")
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
    
#___________________________________________________________________________________
class DispatchListener( stomp.ConnectionListener ):
    """
    Listener class.  Called by the stomp library... from a separate thread.
    Recieved messages will be cached w/in a queue (thread safe python FIFO
    stack).

    NOTE:  This should be a singleton class
    """
    def __init__(self,connection):
        self.connection = connection
        self.messages     = pd.DataFrame() # any write OR read op should be w/in a locked context
        self.lock_        = threading.Lock()

    def show( self ):
        with self.lock_:
            if self.messages.empty:
                WARN("No messages recieved yet")            
            else:
                print(self.messages.to_markdown())

    def on_error( self, frame ):
        ERROR('recieved %s' % frame.body)

    def on_message( self, frame ):
        utcnow = datetime.datetime.utcnow()
        with self.lock_:            
            payload = json.loads( str(frame.body) )["payload"]
            payload['state']='pending'
            payload['recieved']=utcnow
            if self.messages.empty:
                self.messages = pd.DataFrame( payload, columns=payload.keys(), index=[0] )
            else:
                temp = pd.DataFrame( payload, columns=payload.keys(), index=[0] )
                self.messages = pd.concat( [self.messages,temp], ignore_index = True )

    def on_disconnected(self):
        WARN("disconnected, attempting to reconnect...")
        connectAndSubscribe( self.connection )


#___________________________________________________________________________________
class DispatchManager:
    def __init__(self,name):
        self.name = name
        self.lock_ = threading.Lock()
        self.enabled = True
        self.verbose = False
        self.delay   = 30 # seconds

    def stop(self):
        """
        Sets the enabled flag to False, causing run to break out of its loop
        """
        with self.lock_:
            self.enabled = False

    def run(self):
        """
        Loop until command loop sets the enabled flag to False
        """
        global listener
        with self.lock_:
            self.enabled = True
        while self.enabled:            
            self.dispatch()
            if self.verbose:
                listener.show()
            time.sleep( self.delay )

    def set_delay(self,delay_):
        if delay_ > 0:
            with self.lock_:
                self.delay = delay_
            

    def dispatch(self):
        global listener
        global dispatch
        
        active = []
        with self.lock_:
            for index,row in dispatch.iterrows():
                if row['enable'] != "yes":
                    continue # skip disabled actors

                #
                # Ensure that the specified actor is available.  If not, issue warning
                # and mark as disabled.
                #
                actor = None
                try:
                    actor = sh.Command( row['actors'] )
                except sh.CommandNotFound:
                    WARN("Could not locate actor %s, disabling"%row['actors'])
                    dispatch.loc[ int(index), ["enable"] ] = ["no"]
                    continue

                regex    = row['match'] 
                scope    = row['scope']
                prescale = row['prescale']

                active.append( {'actor':actor, 'scope':scope, 'regex':regex, 'index':int(index), 'prescale':prescale } )

        # Lock the listener
        with listener.lock_:
            if listener.messages.empty:
                WARN("No messages found")

            else:
                # Loop over all messages which are in the pending state
                for index,row in listener.messages[ listener.messages['state']=='pending' ].iterrows():

                    for dc in active:
                        
                        if dc['scope'] != row['scope']: continue
                        if re.search( dc['regex'], row['name'] ) == None: continue

                        # Increment the count on the dispatcher and check prescale
                        jndex    = dc['index']
                        prescale = dc['prescale']
                        
                        #with self.lock_:
                        #    count = dispatch.loc[ jndex, ["count"] ]
                        #    dispatch.loc[ jndex, ["count"] ] = [count+1]
                        #    if count % prescale > 0:
                        #        INFO("Skip for prescale")
                        #        continue  

                        # Call the matching actor
                        dc['actor']( " %s"%row['name'], index, _out=captureActorOutput, _err=captureActorError, _env=os.environ.copy() )

                        # Mark the dataset as dispatched
                        listener.messages.loc[ int(index), ["state"] ] = ["dispatched"]

        
#___________________________________________________________________________________
dmthread = None
class DonkeyShell( cmd.Cmd ):
    intro  = "Welcome to Donkey shell."
    prompt = "donkey> "
    file_  = None

    def emptyline(self):
        """
        No action performed on an empty line.
        """
        pass

    

    def do_set(self, arg):
        """
        set condition [filename]
            Opens a new watchfile in your $EDITOR and loads into the dispatch manager
            
        """
        global listener
        global dmanager
        global dispatch

        args = arg.split()

        if args[0]=='condition':
            
            filename = "/tmp/watchfile-"+str(uuid.uuid4())
            print(len(args))
            if len(args)>1:
                filename=args[1]

            with open(filename,'w') as f:
                f.write(watch_file_template)

            # Edit the watch file
            editor.edit( filename )

            # And load it
            self.onecmd( "load watchfile %s"%filename )

        else:

            ERROR("SET: Argument %s not recognized"%args[0])

            



    def do_load(self, arg):
        global listener
        global dmanager
        global dispatch
        
        args=arg.split()

        if args[0]=='watchfile':
            for a in args[1:]:
                INFO("Load watch file %s"%a)
                rwf = readWatchFile(a)
                if rwf.get('count',None) == None:
                    rwf['count']=0 # initialize zero count
                if rwf.get('enable',None) == None:
                    rwf['enable']="yes"  # yes/no                
                dfwf = pd.DataFrame( rwf, columns=watch_file_columns )
                # NOTE: We really need to lock down the share data model!!!  i.e. DispatchManager
                # object should manager all reads and writes to the dispatch dataframe (and choose
                # a better name for this).
                with dmanager.lock_:                  
                    dispatch = pd.concat( [dispatch, dfwf], ignore_index=True )

        else:
            ERROR("LOAD: Argument %s not recognized"%arg)            
            

    def do_dispatch(self,arg):
        """
        dispatch [once|run|start|stop]

        once:  run dispatcher one time
        run:   run dispatcher in thread
        start: aka run
        stop:  shutdown dispatch manager and wait for thread to terminate
        """
        global dmanager
        global dmthread

        # Single call to dispatch
        if arg=="once":
            WARN("Executing dispatch one time")
            dmanager.dispatch()

        elif arg=="run" or arg=="start":
            if dmthread == None:
                WARN("Starting dispatch manager on thread")            
                dmthread = threading.Thread( target=dmanager.run )
                dmthread.start()
            else:
                ERROR("Thread object alread exists... manager should be running")

        elif arg=="stop":
            if dmthread != None:
                dmanager.stop() # signal dispatch manager should stop
                WARN("Waiting up to 30s for DM thread to stop")
                dmthread.join() # block until thread has stopped as we
                dmthread = None
            else:
                ERROR("No dispatch manager thread found")

        else:
            ERROR("DISPATCH: Argument %s not recognized"%arg)


    def do_show(self, arg):
        """
        Display actions when messages match user criteria
        > show dispatch
        Display all messages recieved during session
        > show messages
        """
        global dispatch
        global listener
        # NOTE These should be context locked...
        if arg=='dispatch':
            print(dispatch.to_markdown())
        if arg=='messages':
            listener.show()

    def do_enable(self, arg):
        global dispatch
        #dispatch.at[arg, "enable"]="yes"
        dispatch.loc[ int(arg), ["enable"] ] = ["yes"]
    def do_disable(self, arg):
        global dispatch
        #dispatch = dispatch.at[arg, "enable"]="no"
        dispatch.loc[ int(arg), ["enable"] ] = ["no"]

    def do_exit(self,arg):
        """
        Exits command loop and falls through to disconnect.
        """
        global dmanager

        # Stop the dispatch manager
        dmanager.stop()
        
        
        print("This is the end of donkey.  Goodbye.")

        
        
        return True
    def do_EOF(self,arg):
        return self.do_exit(arg)
        
def readWatchFile( filename ):
    result = {}
    if filename:
        with open(filename,'r') as stream:
            try:
                result = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

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

    parser.add_argument( '--watch-file', dest='watchfile', default=[], nargs='+', type=str, help="Definition file")

    return parser.parse_known_args()

def dropCurrentSubscription( current ):
    WARN("dropCurrentSubscription not yet implemented")
    pass

def main():

    global dispatch
    global listener
    global dmanager

    defaults = readConfig()
    args, extras = parse_args( defaults )

    connection = stomp.Connection( [( defaults['host'], defaults['port'] )] )

    # Listener montiors the activemq channel for new datasets
    listener = DispatchListener( connection )
    # Listener operates in a seperate thread
    connection.set_listener( 'dispatch', listener )

    # Setup the dispatch manager... May be executed in own thread or by the
    # cmd loop.
    dmanager = DispatchManager('manager')
    
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



    #
    # Load watch file into a dataframe... results in one entry per actor, not one entry
    # per watch file.
    #
    
    #
    for wf in args.watchfile:
        rwf = readWatchFile(wf)
        rwf['count']=0 # initialize zero count
        rwf['enable']="yes"  # yes/no
        dfwf = pd.DataFrame( rwf, columns=watch_file_columns )
        dispatch = pd.concat( [dispatch, dfwf], ignore_index=True )

    connectAndSubscribe(args, defaults, connection)

    DonkeyShell().cmdloop()

    
    connection.disconnect()
        

if __name__ == '__main__':
    main()


