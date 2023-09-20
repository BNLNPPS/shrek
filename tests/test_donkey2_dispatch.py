import pytest
import datetime
import time
import pprint

time0  = "%s" % datetime.datetime.fromtimestamp( 0 )

def test_donkey2_dispatch_default_rules():
    import re
    
    from donkey.donkey_dispatch import Rule
    from donkey.donkey_dispatch import Dispatch
    from donkey.dataset import dataset

    r = Rule("an empty rule should never match")

    ds = dataset()
    ds.name  = "sP23x_THIS_SHOULD_MATCH_EVENTS-123456789"
    ds.scope = "group.sphenix"
    ds.event = "closed"

    assert r.matches_name( ds ) == None, "An empty rule cannot match the dataset name"
    assert r.matches_event( ds ) == False, "An empty rule cannot match an event"
    assert r.matches_scope( ds ) == False, "An empty rule cannot match a scope"
    assert r.matches( ds ) == None, "An empty rule won't match the dataset"

def test_donkey2_dispatch_matching_rules():
    import re    
    from donkey.donkey_dispatch import Rule
    from donkey.donkey_dispatch import Dispatch
    from donkey.dataset import dataset

    r = Rule("submit run to panda")
    r.event = "closed"
    r.scopes = [ "user.jwebb2", "group.sphenix" ]

    # matches any number of words followed by EVENTS and a 9-digit run number
    r.regex  = re.compile( r"(\w)+EVENTS-(\d{9})" )
    r.actor = "tests/hello.sh" 
    
    ds = dataset()
    ds.name  = "sP23x_THIS_SHOULD_MATCH_EVENTS-123456789"
    ds.scope = "group.sphenix"
    ds.event = "closed"

    # TODO improve error messages...

    assert r.matches_scope( ds ), "dataset should match the rule scope"
    assert r.matches_event( ds ), "dataset should match the rule event"
    assert r.matches_name( ds ), "dataset should match the rule's regex"
    assert r.matches( ds ), "dataset should match the rule..."

    ds.scope = "user.jwebb2"
    assert r.matches( ds ), "dataset should match any scope in the rule's list"

    ds.scope = "none"
    assert r.matches( ds ) == None, "dataset should not match a scope not in the list"
    ds.scope = "group.sphenix"

    ds.event = "open"
    assert r.matches( ds ) == None, "dataset should not match if the datase is opened"
    ds.event = "closed"

    ds.name = "blah12345"
    assert r.matches( ds ) == None, "short run number doesn't work..."
    

def test_donkey_dispatch_class():
    import re
    import uuid

    from donkey.donkey_dispatch import Rule
    from donkey.donkey_dispatch import Dispatch

    from donkey.dataset import dataset_collection as collection
    from donkey.dataset import dataset

    #
    # construct db file
    #
    
    fname = 'test-dispatch-%s'%str(uuid.uuid4())
    coll = collection( fname )

    uu = uuid.uuid4()        
    coll.addlist( 'pending', 'pending datasets' )
    coll.addlist( 'dispatched', 'dispatched datasets' )
    coll.addlist( 'dropped', 'dropped datasets' )

    # runs to be processed
    runs = [ 987654321,
             987654322,
             987654323,
             987654324,
             987654325,
             ]

    for r in runs:
        ds = dataset()            
        ds.name = "sP23x_EVENTS-%09i"%r
        ds.scope = 'group.sphenix'
        ds.runnumber = r
        ds.event = 'closed'
        ds.created = str(datetime.datetime.utcnow())
        coll.add( 'pending', ds )

    # and runs not yet ready
    runs = [ 987654326,
             987654327,
             987654328,
             987654329,
             987654330,
             ]

    for r in runs:
        ds = dataset()            
        ds.name = "sP23x_EVENTS-%09i"%r
        ds.scope = 'group.sphenix'        
        ds.runnumber = r
        ds.event = 'created'
        ds.created = str(datetime.datetime.utcnow())
        coll.add( 'pending', ds )

    # and datasets that will not be procssed
    for r in runs:
        ds = dataset()            
        ds.name = "sP23x_NOT_MATCHING-%09i"%r
        ds.scope = 'group.sphenix'        
        ds.runnumber = r
        ds.event = 'closed'
        ds.created = str(datetime.datetime.utcnow())
        coll.add( 'pending', ds )        
            

    #
    # Create dispatch object and setup a matching rule for the datasets
    #
        
    d = Dispatch( fname )
    r = Rule("Match raw events")
    r.event = "closed"
    r.scopes = [ "user.jwebb2", "group.sphenix" ]
    r.regex  = re.compile( r"(\w)+EVENTS-(\d{9})" )
    r.actor = "tests/hello.sh"
    d.rules = [r]

    back_to_pending = []

    while coll.length( 'pending' ) > 0:

        # pop the pending stack
        ds = coll.pop( 'pending' )

        if r.matches(ds) is not None:
            # matching rule ds goes to dispatch
            ds.dispatched = str( datetime.datetime.utcnow() )
            coll.add('dispatched',ds)

        elif not r.matches_scope(ds) or not r.matches_name(ds):
            # not in scope or name is not good goes to dropped
            coll.add('dropped',ds)

        else:           
            back_to_pending.append(ds)

    for d in back_to_pending:
        coll.add('pending',d)


    assert coll.length('pending')==5, "Five datasets are not ready to be processed (event=created)"
    assert coll.length('dispatched')==5, "Five datasets should have been dispatched"
    assert coll.length('dropped')==5, "Five datasets should have been dropped"

                


           



def test_donkey_dispatch_class2():
    import re
    import uuid

    from donkey.donkey_dispatch import Rule
    from donkey.donkey_dispatch import Dispatch

    from donkey.dataset import dataset_collection as collection
    from donkey.dataset import dataset

    #
    # construct db file
    #
    
    fname = 'test-dispatch-%s'%str(uuid.uuid4())
    coll = collection( fname )

    uu = uuid.uuid4()        
    coll.addlist( 'pending', 'pending datasets' )
    coll.addlist( 'dispatched', 'dispatched datasets' )
    coll.addlist( 'dropped', 'dropped datasets' )

    # runs to be processed
    runs = [ 987654321,
             987654322,
             987654323,
             987654324,
             987654325,
             ]

    for r in runs:
        ds = dataset()            
        ds.name = "sP23x_EVENTS-%09i"%r
        ds.scope = 'group.sphenix'
        ds.runnumber = r
        ds.event = 'closed'
        ds.created = str(datetime.datetime.utcnow())
        coll.add( 'pending', ds )

    # and runs not yet ready
    runs = [ 987654326,
             987654327,
             987654328,
             987654329,
             987654330,
             ]

    for r in runs:
        ds = dataset()            
        ds.name = "sP23x_EVENTS-%09i"%r
        ds.scope = 'group.sphenix'        
        ds.runnumber = r
        ds.event = 'created'
        ds.created = str(datetime.datetime.utcnow())
        coll.add( 'pending', ds )

    # and datasets that will not be procssed
    for r in runs:
        ds = dataset()            
        ds.name = "sP23x_NOT_MATCHING-%09i"%r
        ds.scope = 'group.sphenix'        
        ds.runnumber = r
        ds.event = 'closed'
        ds.created = str(datetime.datetime.utcnow())
        coll.add( 'pending', ds )        
            

    #
    # Create dispatch object and setup a matching rule for the datasets
    #
        
    d = Dispatch( fname )
    r1 = Rule("Match raw events")
    r1.event = "closed"
    r1.scopes = [ "user.jwebb2", "group.sphenix" ]
    r1.regex  = re.compile( r"(\w)+EVENTS-(\d{9})" )
    r1.actor  = "tests/hello.sh"

    r2 = Rule("Match raw events")
    r2.event = "closed"
    r2.scopes = [ "user.jwebb2", "group.sphenix" ]
    r2.regex  = re.compile( r"(\w)+EVENTS-(\d{9})" )
    r2.actor  = "tests/launch.sh"
    
    d.rules = [r1, r2]


    # Run the dispatcher
    d.run()

    # refresh the DB file
    coll = collection( fname )    

    assert coll.length('pending')==5,    "Five datasets are not ready to be processed (event=created)"
    assert coll.length('dispatched')==5, "Five datasets should have been dispatched"
    assert coll.length('dropped')==5,    "Five datasets should have been dropped"

                          
def test_donkey_dispatch_rules_cli():
    import re
    import uuid

    from donkey.donkey_dispatch import Rule
    from donkey.donkey_dispatch import Dispatch
    from donkey.donkey_dispatch import run

    from donkey.dataset import dataset_collection as collection
    from donkey.dataset import dataset

    #
    # construct db file
    #
    
    fname = 'test-dispatch-%s'%str(uuid.uuid4())

    coll = collection( fname )
    coll.addlist( 'pending', 'pending datasets' )
    coll.addlist( 'dispatched', 'dispatched datasets' )
    coll.addlist( 'dropped', 'dropped datasets' )

    # runs to be processed
    pending_runs = [ 987654321, ]

    for r in pending_runs:
        ds = dataset()            
        ds.name = "sP23x_RAW_EVENTS-%09i"%r
        ds.scope = 'group.sphenix'
        ds.runnumber = r
        ds.event = 'closed'
        ds.created = str(datetime.datetime.utcnow())
        coll.add( 'pending', ds )

    assert coll.length( 'pending' ) == len( pending_runs ), "Make sure we have all pending runs before running"        

    # Setup the dispatch object but do not run (no --run)
    run([
        '--dbfile', '%s'%fname,
        '--rule',   'raw-events',
        '--event',  'closed',
        '--scope',  'group.sphenix',
        '--actor',  'tests/hello.sh',
        '--regex',  r'(\w)+EVENTS-(\d{9})',
        '--rule',   'raw-events',
        '--event',  'closed',
        '--scope',  'user.jwebb2',
        '--actor',  'tests/hello.sh',
        '--regex',  r'(\w)+EVENTS-(\d{9})',        
        ])

    # Import the dispatch object
    from donkey.donkey_dispatch import dispatch

    assert len(dispatch.rules)==2, "There should be two rules defined on the dispatch object"

    # Create a regex
    regex = re.compile( r"(\w)+EVENTS-(\d{9})" )

    assert( regex.match( "sP23x_RAW_EVENTS-%09i"%987654321 ) ), "Regex must match..."

    while coll.length( 'pending' ) > 0:
        ds = coll.pop( 'pending' )
        r = dispatch.rules[0]
        
        assert( r.matches_scope(ds) ), "The rule must match the scope"
        assert( r.matches_event(ds) ), "The rule must match the event"
        assert( r.matches_name(ds)  ), "The rule must match the name/regex"
        assert( r.matches(ds) is not None ), "The rule must match"

def test_donkey_dispatch_cli():
    import re
    import uuid

    from donkey.donkey_dispatch import Rule
    from donkey.donkey_dispatch import Dispatch
    from donkey.donkey_dispatch import run

    from donkey.dataset import dataset_collection as collection
    from donkey.dataset import dataset

    #
    # construct db file
    #
    
    fname = 'test-dispatch-%s'%str(uuid.uuid4())

    coll = collection( fname )
    coll.addlist( 'pending', 'pending datasets' )
    coll.addlist( 'dispatched', 'dispatched datasets' )
    coll.addlist( 'dropped', 'dropped datasets' )

    # runs to be processed
    pending_runs = [ 987654321,
                     987654322,
                     987654323,
                     987654324,
                     987654325,
                     ]

    for r in pending_runs:
        ds = dataset()            
        ds.name = "sP23x_RAW_EVENTS-%09i"%r
        ds.scope = 'group.sphenix'
        ds.runnumber = r
        ds.event = 'closed'
        ds.created = str(datetime.datetime.utcnow())
        coll.add( 'pending', ds )

    assert coll.length( 'pending' ) == len( pending_runs ), "Make sure we have all pending runs before running"        

    # Setup and run dispatch
    run([
        '--dbfile', '%s'%fname,
        '--rule',   'raw-events',
        '--event',  'closed',
        '--scope',  'group.sphenix',
        '--actor',  'tests/hello.sh',
        '--regex',  r'(\w)+EVENTS-(\d{9})',
        '--rule',   'raw-events',
        '--event',  'closed',
        '--scope',  'user.jwebb2',
        '--actor',  'tests/hello.sh',
        '--regex',  r'(\w)+EVENTS-(\d{9})',
        '--run'
        ])

    coll = collection( fname )
    
    assert coll.length('pending')    == 0,                 "Dispatch should have consumed   all pending runs"
    assert coll.length('dispatched') == len(pending_runs), "Dispatch should have dispatched all pending runs"



        


        
                
                


           

    


    
    
    



    
