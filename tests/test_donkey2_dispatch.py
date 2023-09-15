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
    

def test_donkey2_dispatch_rules():
    import re
    
    from donkey.donkey_dispatch import Rule
    from donkey.donkey_dispatch import Dispatch
    from donkey.dataset import dataset

    r = Rule("submit run to panda")
    r.event = "closed"
    r.scopes = [ "user.jwebb2", "group.sphenix" ]

    # matches any number of words followed by EVENTS and a 9-digit run number
    r.regex  = re.compile( r"(\w)+EVENTS-(\d{9})" )
    r.actors = "hello.sh" 
    
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


    #regex=re.compile( r"(\w)+_EVENTS-(\d{9})" )
    #result = regex.match(ds.name)

    #assert result
    
    #assert r.matches_name( ds ), "dataset should match the rule name"

#    ds.event = "opened"
#    result = r.matches( ds )    
#    assert result[0] is None, "rule should not match an open dataset"

    
    




    
