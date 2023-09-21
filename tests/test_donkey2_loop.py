import pytest
import datetime
import time
import pprint

time0  = "%s" % datetime.datetime.fromtimestamp( 0 )
utcnow = "%s" % datetime.datetime.utcnow()

def test_donkey2_loop():
    """
    """
    from rucio.client import Client
    client = Client()

    import uuid

    from donkey.donkey_listener import run as run_listener
    from donkey.donkey_dispatch import run as run_dispatch

    from donkey.dataset import dataset_collection as collection

    uu = str( uuid.uuid4() ).replace('-','_')    
    dbfile = 'test-donkey-loop-%s'%uu
    rn=1883

    scope  = 'user.jwebb2'
    dsname = 'test_donkey2_%s_EVENTS-%09i'%(uu,rn)

    client.add_dataset( scope, dsname )
    client.set_metadata( scope, dsname, 'run_number', str(rn) )
    assert client.get_did( scope, dsname ), "The dataset must be created in rucio"
    
    client.set_metadata( scope, dsname, 'lifetime',   str(1800) )   # dataset should expire after the test is done
    
    # Run the listener for 3 min and pull the messages into the file
    run_listener( [3,2,1], dbfile, ['pending','dispatched','dropped'] )

    coll = collection(dbfile)
    assert coll.find('pending',dsname), 'dataset %s must exist in pending collection after created in rucio'%dsname

    # Setup and run dispatch
    run_dispatch([
        '--dbfile', '%s'%dbfile,
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

    #coll = collection(dbfile)
    #assert coll.find('pending',dsname), 'dataset %s must still exist in pending collection after dispatcher looks for closed datasets'%dsname



    
    
    
