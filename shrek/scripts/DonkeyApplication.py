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
import readline

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
connection = None

verbose  = 0

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

    def showfilt( self, keyval ):

        if '==' in keyval:
            (key,val)=keyval.split('==')
            print( self.messages[ self.messages[key] == val ].to_markdown() )
        elif '!=' in keyval:
            (key,val)=keyval.split('!=')
            print( self.messages[ self.messages[key] != val ].to_markdown() )             

        
    def show( self, n=0 ):
        with self.lock_:
            if self.messages.empty:
                WARN("No messages recieved yet")            
            else:
                if n==0:
                    print(self.messages.to_markdown())
                elif n > 0:
                    print(self.messages.tail( n).to_markdown())
                else:
                    print(self.messages.head(-n).to_markdown())                    

    def on_error( self, frame ):
        ERROR('recieved %s' % frame.body)

    def on_message( self, frame ):
        utcnow = datetime.datetime.utcnow()
        with self.lock_:            
            payload = json.loads( str(frame.body) )["payload"]
            payload['state']='pending'
            payload['recieved']=utcnow

            # Check if this is a child dataset
            childscope = payload.get( 'childscope', 'nan')
            childname  = payload.get( 'childname',  'nan' )
            childtype  = payload.get( 'childtype',  'nan' )
            if childscope != 'nan' or childname != 'nan' or childtype != 'nan':
                payload['state'] = 'workflow product'

            if payload['account']=='iddsv1':
                payload['state'] = 'workflow product'           
            
            if self.messages.empty:
                self.messages = pd.DataFrame( payload, columns=payload.keys(), index=[0] )
            else:
                temp = pd.DataFrame( payload, columns=payload.keys(), index=[0] )
                self.messages = pd.concat( [self.messages,temp], ignore_index = True )

    #def on_disconnected(self):
    #    WARN("disconnected, attempting to reconnect...")
    #    connectAndSubscribe( self.connection )


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
        global verbose
        with self.lock_:
            self.enabled = True
        while self.enabled:            
            self.dispatch()
            if verbose>0:
                listener.show()
            time.sleep( self.delay )

    def set_delay(self,delay_):
        if delay_ > 0:
            with self.lock_:
                self.delay = delay_

    def save(self,filename=".donkey/messages.csv"):        
        global listener
        with listener.lock_:
            INFO("Save to message persistency file %s"%filename)                
            listener.messages.to_csv( filename, mode='w', index=False, header=True )

    def restore(self,filename=".donkey/messages.csv"):
        global listener
        if os.path.exists(filename):
            WARN("Messages restored from %s"%filename)            
            with listener.lock_:        
                readin = pd.read_csv( filename )
                listener.messages = readin
            

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

                        myactor =  dc['actor']

                        try:
                            myactor( " %s"%row['name'], index, _out=captureActorOutput, _err=captureActorError, _env=os.environ.copy() )

                            # Only mark to state dispatched if actor succeeded
                            listener.messages.loc[ int(index), ["state"] ] = ["dispatched"]
                            
                        except sh.ErrorReturnCode:
                            WARN("Actor returned error code")


                            



        
#___________________________________________________________________________________
dmthread = None
class DonkeyShell( cmd.Cmd ):
    intro  = "Welcome to Donkey shell."
    prompt = "donkey> "
    file_  = None

    def __init__(self,args,histfile=".donkey/history",histfile_size=10000):
        cmd.Cmd.__init__(self)
        self.args = args
        self.hist = histfile
        self.histsz = histfile_size

    def postloop(self):
        # Save history file
        readline.set_history_length(self.histsz)
        readline.write_history_file(self.hist)


    def preloop(self):
        # Load history file
        if os.path.exists(self.hist):
            readline.read_history_file(self.hist)

        # Process batch files on command line
        go = True

        # Verify that all provided batch files exist
        if self.args.batchfile != [None]:
            for bf in self.args.batchfile:
                if not os.path.exists(bf):
                    WARN("Batch file %s not found.")
                    go = False
        
        # Execute any pending batch files
        if go==True and self.args.batchfile != [None]:
            for bf in self.args.batchfile:
                with open(bf,'r') as bf_:
                    for line in bf_:
                        self.cmdqueue.append(line)
        elif self.args.batchfile != [None]: 
            CRITICAL("One or more batch files could not be found.  Exiting.")
            self.cmdqueue.append("exit")
    

    def emptyline(self):
        """
        No action performed on an empty line.
        """
        pass

    def do_shell(self,arg):
        """
        > shell command

        Execute specified command in the system shell.
        """
        os.system(arg)

    def do_sleep(self,n_):
        """
        > sleep n

        Puts the command line to sleep for N seconds.  Both the listener
        and dispatch manager continue to run in the background.
        """
        time.sleep( int(n_) )
        

    def do_info(self,arg):
        """
        > info message
        
        Log message at INFO level.
        """
        INFO(arg)
    def do_warn(self,arg):
        """
        > warn message
        
        Log message at WARN level.
        """        
        WARN(arg)
    def do_error(self,arg):
        """
        > error message
        
        Log message at ERROR level.
        """        
        ERROR(arg)
    def do_critical(self,arg):
        """
        > info message
        
        Log message at CRITICAL level.
        """        
        CRITICAL(arg)

    def do_exec(self,arg):
        """
        > exec batchfile [batchfile ...]

        Execute the commands contained in the specified batch file(s).
        
        """
        args = arg.split()

        go = True
        for bf in args:
            if not os.path.exists(bf):
                WARN("Batch file %s not found.")
                go = False

        if go == True:
            for bf in args:
                INFO("Execute %s"%bf)
                with open(bf,'r') as file_:
                    for line in file_:
                        self.onecmd( line )
        else:
            WARN("One or more batch files missing.  None executed.")
            

    def do_save(self,arg):
        """
        > save [filename]

        Saves messages to csv file.  If no filename is provided, saves to the checkpoint
        file.
        """
        global dmanager
        if arg == "":
            dmanager.save()
        else:
            dmanager.save(arg)


    def do_edit(self, arg):
        """
        > edit filename
        """
        if os.path.exists(arg):
            editor.edit(arg)
        else:
            ERROR("Could not find %s"%str(arg))


    def do_set(self, arg):
        """
        > set condition [filename]
              Opens a new watchfile in your $EDITOR and loads into the dispatch manager

        > set delay value
              Sets the delay between iterations checking the message queue for dispatching
              work to the actors.

        > set verbose level
              Sets global verbosity level

        > set state row value
              For the specified row (or comma-separated list of rows), set the message state
              to the specified value [pending or ignore]
              
            
        """
        global listener
        global dmanager
        global dispatch
        global verbose

        args = arg.split()

        if args[0]=='verbose':
            verbose = args[1]

        elif args[0]=='condition':
            
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

        elif args[0]=='delay':

            if int(args[1])<30:
                WARN("SET: Setting less that 30s between dispatch iterations doesn't make much sense.")
            dmanager.set_delay( int(args[1]) )

        elif args[0]=='state':
            if len(args)==3:
                rows = args[1].split(',')
                for r in rows:
                    row=int(r)
                    val=args[2]
                    if val in ["pending","ignored"]:
                        with listener.lock_:                    
                            listener.messages.at[ row, "state" ] = val
                    else:
                        WARN("Cannot set state to %s"%val)

        else:

            ERROR("SET: Argument %s not recognized"%args[0])

    def do_load(self, arg):
        """
        > load conditions filename

        Loads a conditions file

        > load messages filename

        Loads message file
        
        """
        global listener
        global dmanager
        global dispatch
        
        args=arg.split()

        if args[0]=='conditions':
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

        elif args[0]=='messages':
            
            dmanager.restore(args[1])

        else:
            ERROR("LOAD: Argument %s not recognized"%arg)            


    def do_addcon(self,cond):
        """
        This is primarily intended for testing purposes.  Use with great care.

        > addcon actor,prescale,scope,regex,count,enable

        e.g.
        addcon helloWorld,1,user.jwebb2,u*,0,yes
        """
        global listener
        global dmanager
        cond_ = cond.split(",")
        with dmanager.lock_:
            dispatch.loc[ len(dispatch.index) ] = cond_

    def do_rmcon(self,index):
        """
        This is an expert level command... use at your own risk.  Removes
        a condition at index.

        > rmcon index
        """
        global listener
        global dmanager
        with dmanager.lock_:
            dispatch = dispatch.drop( int(index) )
            
    def do_addmsg(self,msg):
        """
        This is primarily intended for testing purposes.  Use with great care.
        
        > addmsg account scope name expired_at state recieved childscope childtype childname

        e.g.
        addmsg sphnxpro,user.jwebb2,user.jwebb2.test-dataset,nan,pending,2023-02-23 19:35:34.049738,nan,nan,nan
        """
        global listener        
        msg_ = msg.split(",")
        with listener.lock_:
            listener.messages.loc[ len(listener.messages.index) ] = msg_

    def do_rmmsg(self,index):
        """
        This is an expert level command... use at your own risk.  Removes
        a message at index.

        > rmmsg index
        """
        global listener
        with listener.lock_:
            listener.messages = listener.messages.drop( int(index) )
        

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

        Display all messages recieved during session.  If n is provided,
        show only the n most recent lines.  First n if negative value given.
        > show messages [n]

        Show messages filtered on the column.  "==" and "!=" are supported.
        There cannot be spaces between the key, operator and value.
        > show messages account==sphnxpro
        > show messages scope!=user.jwebb2        
        """
        global dispatch
        global listener

        args=arg.split()
        
        # NOTE These should be context locked...
        if args[0]=='dispatch':
            print(dispatch.to_markdown())

        if args[0]=='messages':
            if len(args)==1:
                listener.show(0) # shows all messages
            elif len(args)>1 and args[1].lstrip('-').isdigit():
                listener.show( int(args[1]) )
            elif len(args)>1:
                listener.showfilt( args[1] )

    def do_history(self,arg):        
        for i in range(readline.get_current_history_length()):
            print("%i %s"%(i,readline.get_history_item(i + 1)))


    def do_enable(self, arg):
        global dispatch
        dispatch.loc[ int(arg), ["enable"] ] = ["yes"]
    def do_disable(self, arg):
        global dispatch
        dispatch.loc[ int(arg), ["enable"] ] = ["no"]

    def do_exit(self,arg):
        """
        Exits command loop and falls through to disconnect.
        """
        global dmanager
        global connection

        connection.disconnect()
        dmanager.save()
        dmanager.stop()
        INFO("This is the end of donkey shell.  Goodbye.")
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

    parser.add_argument( 'batchfile', nargs='?', type=str, help="Batch file", action="append")

    return parser.parse_known_args()

def dropCurrentSubscription( current ):
    WARN("dropCurrentSubscription not yet implemented")
    pass

def main():

    global dispatch
    global listener
    global dmanager
    global connection

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

    # Restore messages
    dmanager.restore()
    
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
    

    for wf in args.watchfile:
        if not os.path.exists(wf):
            WARN("%s does not exist, ignored"%wf)
        else:
            rwf = readWatchFile(wf)
            rwf['count']=0 # initialize zero count
            rwf['enable']="yes"  # yes/no
            dfwf = pd.DataFrame( rwf, columns=watch_file_columns )
            dispatch = pd.concat( [dispatch, dfwf], ignore_index=True )


    connectAndSubscribe(args, defaults, connection)

    # Instantiate and run the donkey shell
    DonkeyShell(args).cmdloop()    

    

    
if __name__ == '__main__':
    main()


